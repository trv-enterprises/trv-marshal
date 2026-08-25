package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/config"
	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/engine"
)

func main() {
	configPath := flag.String("config", "rules.yaml", "path to rules config file")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}
	slog.Info("config loaded", "rules", len(cfg.Rules), "broker", cfg.MQTT.Broker)

	// Connect to MQTT broker
	hook := &onConnect{}
	client, err := connectMQTT(cfg.MQTT, hook)
	if err != nil {
		slog.Error("failed to connect to MQTT broker", "error", err)
		os.Exit(1)
	}
	defer client.Disconnect(5000)
	slog.Info("connected to MQTT broker")

	// Start engine
	eng := engine.New(cfg, client, *configPath)

	// Arm the reconnect hook, then subscribe once for the connection that is
	// already up.
	//
	// Ordering matters: connectMQTT blocks until connected, so OnConnect has
	// already fired by now -- and it fired with a nil callback, subscribing
	// nothing. This call covers that first connection; OnConnect covers every
	// one after it. Exactly one of the two subscribes per connect, which is
	// what keeps a single handler registered per topic.
	// Recovery builds a REPLACEMENT client rather than reusing this one: a
	// wedged paho client can refuse Connect() indefinitely, and no exported
	// method reliably drives it back to a state that accepts one. A fresh
	// client always starts at `disconnected`, so its Connect() is legal.
	eng.SetClientFactory(func() mqtt.Client {
		c, err := connectMQTT(cfg.MQTT, hook)
		if err != nil {
			slog.Error("building replacement MQTT client failed", "error", err)
			return nil
		}
		return c
	})

	hook.set(eng.SubscribeAll)
	if err := eng.SubscribeAll(); err != nil {
		slog.Error("initial subscribe failed", "error", err)
		os.Exit(1)
	}

	if err := eng.Start(); err != nil {
		slog.Error("failed to start engine", "error", err)
		os.Exit(1)
	}

	// Signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	slog.Info("alert engine running", "alert_topic", cfg.AlertTopic)

	for sig := range sigCh {
		switch sig {
		case syscall.SIGHUP:
			slog.Info("received SIGHUP, reloading config")
			if err := eng.Reload(); err != nil {
				slog.Error("config reload failed", "error", err)
			}
		case syscall.SIGINT, syscall.SIGTERM:
			slog.Info("shutting down", "signal", sig.String())
			eng.Stop()
			return
		}
	}
}

// onConnect holds the resubscribe callback. The MQTT client must exist before
// the engine can be constructed, but the OnConnect handler needs to call back
// into the engine -- so the handler is registered up front and the callback is
// filled in once the engine exists. Guarded because paho invokes OnConnect
// from its own goroutine.
type onConnect struct {
	mu      sync.Mutex
	fn      func() error
	running bool
}

func (o *onConnect) set(fn func() error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.fn = fn
}

func (o *onConnect) get() func() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.fn
}

// enter claims the right to run the resubscribe pass, returning false if
// another goroutine is already inside one. leave releases the claim.
func (o *onConnect) enter() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.running {
		return false
	}
	o.running = true
	return true
}

func (o *onConnect) leave() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.running = false
}

// uniqueClientID suffixes the configured client ID with the hostname and PID.
//
// MQTT allows exactly one live connection per client ID: a second connect
// using the same ID makes the broker evict the first, which the evicted side
// sees as a bare EOF. With a fixed ID any overlap -- a restart whose old
// session the broker has not yet reaped, a redeploy that briefly runs two
// containers, or a reconnect racing its own predecessor -- turns into two
// sessions repeatedly kicking each other off. That is the 2026-08-23 stall:
// four EOF disconnects in six seconds, after which the surviving client held
// a socket the broker had already discarded and received nothing for five
// hours while still reporting healthy.
//
// The suffix makes collisions impossible, so a reconnect can never evict
// itself and an overlapping deploy degrades to two independent clients rather
// than a takeover loop.
func uniqueClientID(base string) string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("%s-%s-%d", base, host, os.Getpid())
}

func connectMQTT(mqttCfg config.MQTTConfig, hook *onConnect) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(mqttCfg.Broker).
		SetClientID(uniqueClientID(mqttCfg.ClientID)).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		// NOTE: SetResumeSubs is deliberately NOT set. Under CleanSession
		// (which this client uses) paho calls persist.Reset() on the initial
		// connect, so the reconnect path replays an empty store and resumes
		// nothing -- see paho client.go:286-291. Resubscribing is handled
		// explicitly in the OnConnect handler below instead.
		// Detect a half-open connection rather than waiting indefinitely on a
		// socket the broker has already forgotten.
		SetKeepAlive(30 * time.Second).
		SetPingTimeout(10 * time.Second).
		SetMaxReconnectInterval(60 * time.Second).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			slog.Error("MQTT connection lost", "error", err)
		}).
		SetReconnectingHandler(func(_ mqtt.Client, _ *mqtt.ClientOptions) {
			slog.Warn("MQTT reconnecting")
		}).
		SetOnConnectHandler(func(_ mqtt.Client) {
			slog.Info("MQTT connected/reconnected")
			// Re-subscribe on EVERY connect. With CleanSession the broker
			// drops all subscriptions on disconnect and paho does not restore
			// them, so without this the client reconnects and goes silent.
			//
			// enter() collapses overlapping invocations: paho can fire
			// OnConnect from more than one goroutine (the initial Connect and
			// the auto-reconnect path both do), and each concurrent pass used
			// to register a second handler for every topic -- visible in the
			// logs as each topic being "subscribed" twice. Duplicate handlers
			// mean every inbound message is processed twice, which for an
			// edge-triggered action rule means the command is published twice.
			if !hook.enter() {
				slog.Warn("resubscribe already in progress, skipping duplicate OnConnect")
				return
			}
			defer hook.leave()

			if fn := hook.get(); fn != nil {
				if err := fn(); err != nil {
					slog.Error("resubscribe after reconnect failed", "error", err)
					return
				}
				slog.Info("resubscribed after reconnect")
			}
		})

	client := mqtt.NewClient(opts)
	token := client.Connect()

	if !token.WaitTimeout(30 * time.Second) {
		return nil, fmt.Errorf("MQTT connect timeout after 30s")
	}
	if token.Error() != nil {
		return nil, fmt.Errorf("MQTT connect: %w", token.Error())
	}

	return client, nil
}
