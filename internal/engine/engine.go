package engine

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/actuator"
	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/alerter"
	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/config"
	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/evaluator"
	"github.com/trv-enterprises/trv-homelab/edge/sensor-alert-engine/internal/state"
)

const (
	sweepInterval = 30 * time.Second

	// heartbeatInterval is how often the engine reports liveness. Long enough
	// to stay out of the way in normal operation, short enough that a stall is
	// obvious within a few minutes rather than hours.
	heartbeatInterval = 5 * time.Minute

	// stallTimeout is how long the engine tolerates a nominally-connected
	// client delivering nothing before it forces a reconnect.
	//
	// This is a liveness backstop, not a traffic expectation: the trigger is
	// silence on a connection paho believes is up, which is the signature of a
	// half-open socket the broker has already discarded. Zigbee devices here
	// report at least on a periodic heartbeat, so 20 minutes of total silence
	// across every subscribed topic is a fault rather than a quiet house.
	stallTimeout = 20 * time.Minute

	// Recovery timings. Every one of these is a bound on a paho call that can
	// otherwise block or silently no-op; none may be zero.
	//
	// disconnectQuiesceMS is how long Disconnect() waits for in-flight work.
	// Short on purpose: this path runs because the connection is already
	// believed dead, so there is nothing worth draining.
	disconnectQuiesceMS = 250

	// disconnectWait bounds how long we wait for the status to reach
	// `disconnected` after Disconnect() returns. Disconnect does its work in a
	// goroutine, so this gap is normal rather than exceptional.
	disconnectWait = 10 * time.Second
	disconnectPoll = 50 * time.Millisecond

	// connectWait bounds the reconnect itself. Shorter than heartbeatInterval
	// so a failed attempt is reported and retried on the next tick rather than
	// overlapping the following one.
	connectWait = 30 * time.Second
)

// healthPath is where each heartbeat records its verdict for the
// container healthcheck to read. Under /tmp because it is genuinely
// ephemeral: it is rewritten every heartbeat and means nothing across a
// restart. Nothing outside the container reads it, so it is not a bind mount.
//
// A var rather than a const solely so tests can redirect it to a temp dir.
var healthPath = "/tmp/alert-engine-health"

// Engine is the core alert processing engine.
type Engine struct {
	cfg        *config.Config
	client     mqtt.Client
	tracker    *state.Tracker
	actuators  *actuator.Tracker
	alerter    *alerter.Alerter
	configPath string
	stopSweep  chan struct{}

	// Liveness counters. A stalled engine looks identical to an idle one in
	// the logs -- the process is up, the container is healthy, and nothing is
	// published because nothing is happening. These make the difference
	// visible: the heartbeat reports how many messages actually arrived, so a
	// run of "messages=0" while devices are known to be reporting is a stall,
	// not quiet.
	mu         sync.Mutex
	msgCount   uint64
	lastMsgAt  time.Time
	cmdCount   uint64
	sweepCount uint64
	startedAt  time.Time
}

// mqttPublisher adapts the paho MQTT client to the alerter.Publisher interface.
type mqttPublisher struct {
	client mqtt.Client
}

func (p *mqttPublisher) Publish(topic string, payload []byte) error {
	token := p.client.Publish(topic, 1, false, payload) // QoS 1
	token.Wait()
	return token.Error()
}

// New creates a new Engine.
func New(cfg *config.Config, client mqtt.Client, configPath string) *Engine {
	tracker := state.NewTracker()
	pub := &mqttPublisher{client: client}
	a := alerter.New(pub, cfg.AlertTopic)

	return &Engine{
		cfg:        cfg,
		client:     client,
		tracker:    tracker,
		actuators:  actuator.NewTracker(),
		alerter:    a,
		configPath: configPath,
		stopSweep:  make(chan struct{}),
		startedAt:  time.Now(),
	}
}

// Start subscribes to MQTT topics and begins the sweep ticker.
// Start begins the sweep and heartbeat loops.
//
// Deliberately does NOT subscribe: the OnConnect handler already calls
// SubscribeAll on every connect, the initial one included. Subscribing here
// too registered a second handler for every topic -- harmless-looking in the
// logs (each topic "subscribed" twice) but it meant every inbound message was
// processed twice, so an edge-triggered action published its command twice.
func (e *Engine) Start() error {
	// Seed the health file before the first heartbeat. Start is only reached
	// once the initial connect and subscribe have both succeeded, so "ok" is
	// accurate here -- and without it the container would read as unhealthy
	// for the whole first heartbeatInterval, which is a normal boot rather
	// than a fault.
	e.writeHealth(true)

	go e.sweepLoop()
	go e.heartbeatLoop()
	return nil
}

// SubscribeAll (re)subscribes to every topic the config requires.
//
// This MUST run on every connect, not just the first. The client uses
// CleanSession, so the broker discards all subscriptions the moment the
// connection drops. paho does not restore them for us: on the initial connect
// it takes the CleanSession branch and calls persist.Reset(), and the
// reconnect path then replays a store that is empty -- so ResumeSubs, despite
// its name, resumes nothing under CleanSession. The result is a client that
// reconnects, reports healthy, and silently receives nothing. That is exactly
// the 2026-08-22 stall: connected for ten hours, zero messages.
//
// Re-subscribing explicitly from OnConnect sidesteps paho's store entirely.
// Subscribing to a topic that is already subscribed is harmless.
func (e *Engine) SubscribeAll() error {
	topics := e.cfg.Topics()
	slog.Info("subscribing to topics", "count", len(topics))

	for _, topic := range topics {
		t := topic // capture for closure
		token := e.client.Subscribe(t, 1, func(_ mqtt.Client, msg mqtt.Message) {
			e.handleMessage(msg.Topic(), msg.Payload())
		})
		if !token.WaitTimeout(10 * time.Second) {
			return fmt.Errorf("subscribing to %q: timed out", t)
		}
		if err := token.Error(); err != nil {
			return fmt.Errorf("subscribing to %q: %w", t, err)
		}
		slog.Info("subscribed", "topic", t)
	}
	return nil
}

// Stop unsubscribes from all topics and stops the sweep ticker.
func (e *Engine) Stop() {
	close(e.stopSweep)

	topics := e.cfg.Topics()
	for _, topic := range topics {
		token := e.client.Unsubscribe(topic)
		token.Wait()
	}
	slog.Info("unsubscribed from all topics")
}

// Reload loads a new config, diffs subscriptions, and preserves state.
func (e *Engine) Reload() error {
	newCfg, err := config.Load(e.configPath)
	if err != nil {
		return fmt.Errorf("reloading config: %w", err)
	}

	oldTopics := toSet(e.cfg.Topics())
	newTopics := toSet(newCfg.Topics())

	// Unsubscribe removed topics
	for topic := range oldTopics {
		if !newTopics[topic] {
			token := e.client.Unsubscribe(topic)
			token.Wait()
			slog.Info("unsubscribed", "topic", topic)
		}
	}

	// Subscribe new topics
	for topic := range newTopics {
		if !oldTopics[topic] {
			t := topic
			token := e.client.Subscribe(t, 1, func(_ mqtt.Client, msg mqtt.Message) {
				e.handleMessage(msg.Topic(), msg.Payload())
			})
			token.Wait()
			if err := token.Error(); err != nil {
				slog.Error("subscribe failed on reload", "topic", t, "error", err)
				continue
			}
			slog.Info("subscribed", "topic", t)
		}
	}

	// Remove state for rules that no longer exist
	newRuleNames := make(map[string]bool)
	for _, r := range newCfg.Rules {
		newRuleNames[r.Name] = true
	}
	for _, name := range e.tracker.RuleNames() {
		if !newRuleNames[name] {
			e.tracker.RemoveRule(name)
			slog.Info("removed state for deleted rule", "rule", name)
		}
	}
	for _, name := range e.actuators.RuleNames() {
		if !newRuleNames[name] {
			e.actuators.RemoveRule(name)
			slog.Info("removed actuation state for deleted rule", "rule", name)
		}
	}

	// Update alerter if alert_topic changed
	if newCfg.AlertTopic != e.cfg.AlertTopic {
		pub := &mqttPublisher{client: e.client}
		e.alerter = alerter.New(pub, newCfg.AlertTopic)
		slog.Info("alert topic changed", "old", e.cfg.AlertTopic, "new", newCfg.AlertTopic)
	}

	e.cfg = newCfg
	slog.Info("config reloaded", "rules", len(newCfg.Rules))
	return nil
}

// handleMessage processes an incoming MQTT message. A topic may be a rule
// trigger, a control topic (override/enable), or both.
func (e *Engine) handleMessage(topic string, payload []byte) {
	now := time.Now()

	e.mu.Lock()
	e.msgCount++
	e.lastMsgAt = now
	e.mu.Unlock()

	e.handleControl(topic, payload, now)

	rules := e.cfg.RulesForTopic(topic)
	if len(rules) == 0 {
		return
	}

	for _, rule := range rules {
		conditionMet, err := evaluator.Evaluate(payload, rule)
		if err != nil {
			slog.Warn("evaluation error",
				"rule", rule.Name,
				"topic", topic,
				"error", err,
			)
			continue
		}

		// Alert path: level-triggered, unchanged semantics.
		if rule.Alert != nil {
			action := e.tracker.Update(rule.Name, conditionMet, rule.Alert.DurationMinutes, rule.Alert.RepeatMinutes, now, conditionMet)
			e.processAction(action, rule, now)
		}

		// Action path: edge-triggered, ownership-arbitrated.
		if rule.Action != nil {
			e.processActuation(rule, conditionMet, now)
		}
	}
}

// handleControl applies override and enable messages for any rule whose
// control topics match. An override message marks manual ownership; an enable
// message parks or resumes automation.
func (e *Engine) handleControl(topic string, payload []byte, now time.Time) {
	overrideRules, enableRules := e.cfg.RulesForControlTopic(topic)

	for _, rule := range overrideRules {
		if !e.actuators.NoteOverride(rule.Name, now) {
			// Echo of our own command; not a manual override.
			continue
		}
		slog.Info("manual override engaged",
			"rule", rule.Name,
			"ttl_minutes", rule.Action.OverrideTTLMinutes,
		)
		e.publishOwnerState(rule, now)
	}

	for _, rule := range enableRules {
		enabled, ok := parseEnable(payload)
		if !ok {
			slog.Warn("unparseable enable payload", "rule", rule.Name, "payload", string(payload))
			continue
		}
		e.actuators.SetEnabled(rule.Name, enabled, now)
		slog.Info("automation enable changed", "rule", rule.Name, "enabled", enabled)
		e.publishOwnerState(rule, now)
	}
}

// processActuation runs the edge-triggered action path for one rule.
func (e *Engine) processActuation(rule config.Rule, conditionMet bool, now time.Time) {
	a := rule.Action
	cmds := e.actuators.Evaluate(
		rule.Name, conditionMet,
		a.Topic, a.Payload,
		a.OffTopicOrDefault(), a.OffPayload,
		a.OffDelaySeconds, a.OverrideTTLMinutes,
		now,
	)
	for _, c := range cmds {
		// If we publish onto our own override topic, pre-register the echo so
		// the inbound copy is not mistaken for a manual command.
		if c.Topic == a.OverrideTopic {
			e.actuators.NoteSelfCommand(rule.Name)
		}
		e.publishCommand(rule.Name, c)
	}
	if len(cmds) > 0 {
		e.publishOwnerState(rule, now)
	}
}

// publishCommand sends a single actuation command.
func (e *Engine) publishCommand(ruleName string, c actuator.Command) {
	token := e.client.Publish(c.Topic, 1, false, c.Payload)
	token.Wait()
	if err := token.Error(); err != nil {
		slog.Error("failed to publish command", "rule", ruleName, "topic", c.Topic, "error", err)
		return
	}
	e.mu.Lock()
	e.cmdCount++
	e.mu.Unlock()
	slog.Info("published command", "rule", ruleName, "topic", c.Topic, "payload", c.Payload)
}

// publishOwnerState publishes the current ownership tier, when the rule
// declares a state topic. Retained so a restarting consumer sees it.
func (e *Engine) publishOwnerState(rule config.Rule, now time.Time) {
	if rule.Action == nil || rule.Action.StateTopic == "" {
		return
	}
	owner := e.actuators.Owner(rule.Name, rule.Action.OverrideTTLMinutes, now)
	token := e.client.Publish(rule.Action.StateTopic, 1, true, owner.String())
	token.Wait()
	if err := token.Error(); err != nil {
		slog.Error("failed to publish owner state", "rule", rule.Name, "error", err)
	}
}

// parseEnable interprets an enable-topic payload as a boolean.
func parseEnable(payload []byte) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(string(payload))) {
	case "true", "on", "1", "enable", "enabled":
		return true, true
	case "false", "off", "0", "disable", "disabled":
		return false, true
	}
	return false, false
}

// heartbeatLoop emits a periodic liveness record.
//
// This exists because a stalled engine is indistinguishable from an idle one
// in the logs: the process stays up, the container reports healthy, and no
// commands are published because -- as far as the engine knows -- nothing has
// happened. On 2026-08-22 the engine went ten hours without reacting to
// motion and the only evidence was the absence of log lines, which is not
// something you can alert on. A heartbeat turns that absence into a signal:
// `messages` counts what actually arrived from the broker, so repeated
// heartbeats reporting zero while devices are known to be publishing is a
// stall, and `last_message_age` says how long it has been going on.
func (e *Engine) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	var lastMsgCount uint64

	for {
		select {
		case <-e.stopSweep:
			return
		case <-ticker.C:
			e.mu.Lock()
			msgs, cmds, sweeps := e.msgCount, e.cmdCount, e.sweepCount
			lastMsgAt, startedAt := e.lastMsgAt, e.startedAt
			e.mu.Unlock()

			sinceLast := msgs - lastMsgCount
			lastMsgCount = msgs

			// Age of the most recent inbound message. Reported as -1 when the
			// engine has never received one, which is distinct from "a long
			// time ago" and usually means subscriptions did not survive a
			// reconnect.
			ageSec := -1.0
			if !lastMsgAt.IsZero() {
				ageSec = time.Since(lastMsgAt).Seconds()
			}

			// IsConnected() reports paho's *intent* to hold a session, not
			// the health of the socket. With auto-reconnect enabled it keeps
			// returning true while the reconnect machinery believes it owns a
			// connection -- so a client evicted by the broker reports healthy
			// indefinitely. Treat prolonged silence as the real signal.
			connected := e.client.IsConnected()
			stalled := connected && !lastMsgAt.IsZero() && time.Since(lastMsgAt) > stallTimeout

			level := slog.LevelInfo
			// No messages in a whole interval is not automatically wrong --
			// a quiet house is quiet -- but combined with a lost connection
			// it is worth surfacing louder.
			if !connected || stalled {
				level = slog.LevelError
			}

			slog.Log(context.Background(), level, "heartbeat",
				"connected", connected,
				"stalled", stalled,
				"messages_total", msgs,
				"messages_since_last_heartbeat", sinceLast,
				"last_message_age_sec", ageSec,
				"commands_total", cmds,
				"sweeps_total", sweeps,
				"uptime_sec", time.Since(startedAt).Seconds(),
			)

			// Publish the same verdict to disk for the container healthcheck.
			//
			// Written on every heartbeat, healthy or not: the file's mtime is
			// what proves the heartbeat loop is still running at all. A
			// process wedged somewhere else entirely would leave a stale
			// "healthy" file, and a check that only read the contents would
			// believe it.
			e.writeHealth(connected && !stalled)

			// Recover whenever the client is not usable, not only when it is
			// stalled.
			//
			// Gating this on `stalled` alone is unrecoverable by
			// construction: `stalled` requires connected == true, so the
			// moment a recovery attempt leaves the client disconnected there
			// is no condition left that can ever fire again. That is exactly
			// how the engine sat offline for fourteen hours on 2026-08-23,
			// logging an ERROR heartbeat every five minutes with nothing
			// acting on it.
			if stalled || !connected {
				slog.Error("MQTT unhealthy, reconnecting",
					"reason", map[bool]string{true: "stalled", false: "disconnected"}[stalled],
					"silent_for_sec", ageSec,
					"threshold_sec", stallTimeout.Seconds(),
				)
				e.reconnect()
			}
		}
	}
}

// reconnect tears down the current MQTT session and establishes a new one.
//
// Used by the stall watchdog to recover a half-open socket. The inbound
// counter is reset first so a reconnect that itself fails silently does not
// immediately re-trigger the watchdog on the next tick -- lastMsgAt is set to
// now, giving the fresh connection a full stallTimeout to prove itself.
func (e *Engine) reconnect() {
	e.mu.Lock()
	e.lastMsgAt = time.Now()
	e.mu.Unlock()

	// Disconnect() is the only teardown paho exposes, and it is a blunt one:
	// it marks the session user-requested, which permanently suppresses
	// auto-reconnect ("disconnect cleans up after a final disconnection (user
	// requested so no auto reconnection)", client.go:498). Everything after
	// this point is therefore our responsibility -- paho will not retry on its
	// own, so this function must not return without either a live connection
	// or a client left in a state a later attempt can recover from.
	e.client.Disconnect(disconnectQuiesceMS)

	// Wait for the teardown to actually land before reconnecting.
	//
	// Disconnect() does its work in a goroutine and returns once the quiesce
	// timer expires, so it routinely returns while the status is still
	// `disconnecting`. Connect() rejects that with "status can only transition
	// to connecting from disconnected" (status.go:104) and returns an already-
	// failed token -- which is precisely how the old code wedged the engine
	// offline: it treated that rejection as terminal and never retried, and
	// the watchdog could not fire again because the client was no longer
	// "connected".
	if !e.waitForDisconnect(disconnectWait) {
		slog.Error("MQTT disconnect did not settle, will retry on next heartbeat",
			"waited_sec", disconnectWait.Seconds())
		return
	}

	token := e.client.Connect()
	if !token.WaitTimeout(connectWait) {
		slog.Error("MQTT reconnect timed out, will retry on next heartbeat",
			"waited_sec", connectWait.Seconds())
		return
	}
	if err := token.Error(); err != nil {
		slog.Error("MQTT reconnect failed, will retry on next heartbeat", "error", err)
		return
	}

	// Connect() does not restore subscriptions under CleanSession. OnConnect
	// normally handles this, but call it explicitly so recovery does not
	// depend on that handler having been wired up.
	if err := e.SubscribeAll(); err != nil {
		// Connected but deaf -- the same silent failure the heartbeat exists
		// to catch. Left as-is deliberately: the next heartbeat sees no
		// inbound messages and drives another recovery.
		slog.Error("resubscribe after recovery failed, will retry on next heartbeat", "error", err)
		return
	}
	slog.Info("MQTT recovered")
}

// writeHealth records the latest heartbeat verdict where the container
// healthcheck can read it.
//
// The file carries "ok" or "unhealthy", and its mtime carries the liveness of
// the heartbeat loop itself. The healthcheck requires both: recent content AND
// a recent write. Content alone would keep reporting the last verdict forever
// if this goroutine died; mtime alone would not notice a client that is up but
// deaf.
//
// Written unconditionally, and a write failure is logged but never fatal --
// losing the health file must not take down a service that is otherwise
// working. A missing file reads as unhealthy at the healthcheck, which is the
// safe direction.
func (e *Engine) writeHealth(ok bool) {
	status := "unhealthy"
	if ok {
		status = "ok"
	}

	// Write-then-rename so the healthcheck never observes a partial write.
	tmp := healthPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(status+"\n"), 0o644); err != nil {
		slog.Warn("could not write health file", "path", tmp, "error", err)
		return
	}
	if err := os.Rename(tmp, healthPath); err != nil {
		slog.Warn("could not update health file", "path", healthPath, "error", err)
	}
}

// waitForDisconnect blocks until the client has fully torn down, returning
// false if it has not settled within timeout.
//
// IsConnectionOpen() is the right probe here: it reports the `connected`
// status specifically, where IsConnected() also reports true while paho is
// merely *intending* to hold a session (connecting, reconnecting, or
// disconnecting-with-retry). Waiting on IsConnected() would return the moment
// the status left `connected`, which is the race this exists to close.
func (e *Engine) waitForDisconnect(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !e.client.IsConnectionOpen() && !e.client.IsConnected() {
			return true
		}
		time.Sleep(disconnectPoll)
	}
	return !e.client.IsConnectionOpen() && !e.client.IsConnected()
}

// sweepLoop periodically checks all active rule states for threshold crossings.
func (e *Engine) sweepLoop() {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-e.stopSweep:
			return
		case <-ticker.C:
			e.sweep()
		}
	}
}

// sweep checks all rules with active conditions.
func (e *Engine) sweep() {
	e.mu.Lock()
	e.sweepCount++
	e.mu.Unlock()

	rules := make(map[string]struct{ DurationMin, RepeatMin int })
	for _, r := range e.cfg.Rules {
		if r.Alert == nil {
			continue
		}
		rules[r.Name] = struct{ DurationMin, RepeatMin int }{r.Alert.DurationMinutes, r.Alert.RepeatMinutes}
	}

	now := time.Now()
	actions := e.tracker.CheckThresholds(rules, now)

	// Build a lookup for rules by name to get full rule details
	rulesByName := make(map[string]config.Rule)
	for _, r := range e.cfg.Rules {
		rulesByName[r.Name] = r
	}

	for ruleName, action := range actions {
		rule, ok := rulesByName[ruleName]
		if !ok {
			continue
		}
		e.processAction(action, rule, now)
	}

	e.sweepActuations(rulesByName, now)
}

// sweepActuations fires any deferred off-commands whose delay has elapsed.
func (e *Engine) sweepActuations(rulesByName map[string]config.Rule, now time.Time) {
	specs := make(map[string]actuator.SweepSpec)
	for name, r := range rulesByName {
		if r.Action == nil {
			continue
		}
		specs[name] = actuator.SweepSpec{
			OffTopic:   r.Action.OffTopicOrDefault(),
			OffPayload: r.Action.OffPayload,
			TTLMinutes: r.Action.OverrideTTLMinutes,
		}
	}

	for _, due := range e.actuators.Sweep(specs, now) {
		if r, ok := rulesByName[due.Rule]; ok && r.Action != nil && due.Command.Topic == r.Action.OverrideTopic {
			e.actuators.NoteSelfCommand(due.Rule)
		}
		e.publishCommand(due.Rule, due.Command)
		if r, ok := rulesByName[due.Rule]; ok {
			e.publishOwnerState(r, now)
		}
	}
}

// processAction fires an alert based on the state machine action.
func (e *Engine) processAction(action state.Action, rule config.Rule, now time.Time) {
	if action == state.ActionNone || rule.Alert == nil {
		return
	}

	device := alerter.DeviceFromTopic(rule.Topic)

	var alertType string
	switch action {
	case state.ActionAlert:
		alertType = "new"
	case state.ActionRepeat:
		alertType = "repeat"
	case state.ActionResolve:
		alertType = "resolved"
	default:
		return
	}

	// Build template variables
	vars := map[string]string{
		"device": device,
		"name":   rule.Name,
		"field":  rule.Condition.Field,
		"value":  fmt.Sprintf("%v", rule.Condition.Value),
	}

	// Calculate duration from state
	if s, ok := e.tracker.GetState(rule.Name); ok && !s.ConditionSince.IsZero() {
		vars["duration"] = alerter.FormatDuration(now.Sub(s.ConditionSince))
	} else {
		vars["duration"] = "0 seconds"
	}

	message := alerter.RenderMessage(rule.Alert.Message, vars)

	if err := e.alerter.SendAlert(alertType, rule.Alert.Severity, rule.Name, message, device); err != nil {
		slog.Error("failed to publish alert",
			"rule", rule.Name,
			"type", alertType,
			"error", err,
		)
	}
}

func toSet(items []string) map[string]bool {
	s := make(map[string]bool, len(items))
	for _, item := range items {
		s[item] = true
	}
	return s
}
