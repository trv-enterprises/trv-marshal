package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-homelab/sensor-alert-engine/internal/config"
	"github.com/trv-homelab/sensor-alert-engine/internal/engine"
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
	client, err := connectMQTT(cfg.MQTT)
	if err != nil {
		slog.Error("failed to connect to MQTT broker", "error", err)
		os.Exit(1)
	}
	defer client.Disconnect(5000)
	slog.Info("connected to MQTT broker")

	// Start engine
	eng := engine.New(cfg, client, *configPath)
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

func connectMQTT(mqttCfg config.MQTTConfig) (mqtt.Client, error) {
	opts := mqtt.NewClientOptions().
		AddBroker(mqttCfg.Broker).
		SetClientID(mqttCfg.ClientID).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			slog.Error("MQTT connection lost", "error", err)
		}).
		SetOnConnectHandler(func(_ mqtt.Client) {
			slog.Info("MQTT connected/reconnected")
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
