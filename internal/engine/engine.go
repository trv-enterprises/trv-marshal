package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-enterprises/trv-marshal/internal/actuator"
	"github.com/trv-enterprises/trv-marshal/internal/alerter"
	"github.com/trv-enterprises/trv-marshal/internal/config"
	"github.com/trv-enterprises/trv-marshal/internal/evaluator"
	"github.com/trv-enterprises/trv-marshal/internal/state"
)

const (
	sweepInterval = 30 * time.Second

	// heartbeatInterval is how often the engine reports liveness. Long enough
	// to stay out of the way in normal operation, short enough that a stall is
	// obvious within a few minutes rather than hours.
	heartbeatInterval = 5 * time.Minute

	// watchdogInterval is how often the engine publishes its loopback probe,
	// re-evaluates the health verdict, and checks whether recovery is needed.
	//
	// The verdict used to be written only by the heartbeat. That made a single
	// unhealthy verdict stand for a full heartbeatInterval -- longer than the
	// container healthcheck's three retries -- so one bad reading restarted
	// the container and self-recovery never got the chance to clear it.
	// Evaluating every watchdogInterval lets a successful recovery read "ok"
	// within one loopback round trip.
	watchdogInterval = 30 * time.Second

	// stallTimeout is how long the engine tolerates a nominally-connected
	// client delivering nothing -- not even its own loopback probe -- before
	// it forces a reconnect.
	//
	// Silence means silence on EVERY subscription including the loopback, not
	// silence from the devices. Until 2026-10-03 it was measured on device
	// traffic alone, on the assumption that some Zigbee device always reports
	// within five minutes. That is false for this house: Zigbee2MQTT's own log
	// shows gaps of four to twelve minutes are routine. A quiet house read as
	// a half-open socket, and autoheal restarted the container 25-39 times a
	// day, discarding hold timers and overrides each time. The loopback
	// arrives every watchdogInterval whatever the house is doing, so ten
	// missed in a row is a fault.
	stallTimeout = 5 * time.Minute

	// recoveryBackoff is the minimum spacing between recovery attempts. The
	// watchdog ticks far more often than a reconnect can prove itself, and
	// every attempt builds a new client, so attempts are spaced rather than
	// repeated on each tick.
	recoveryBackoff = 5 * time.Minute

	// inboxDepth bounds the internal message queue between paho's router and
	// the engine's serial dispatcher. Sized for bursts (Z2M can publish
	// several duplicates per device per second), not sustained backlog: if
	// the dispatcher cannot keep up at this depth something is wedged, and
	// dropping with an error beats blocking paho's router.
	inboxDepth = 256

	// Recovery timings. Every one of these is a bound on a paho call that can
	// otherwise block or silently no-op; none may be zero.
	//
	// disconnectQuiesceMS is how long Disconnect() waits for in-flight work.
	// Short on purpose: this path runs because the connection is already
	// believed dead, so there is nothing worth draining.
	disconnectQuiesceMS = 250

	// connectWait bounds the reconnect itself. Shorter than heartbeatInterval
	// so a failed attempt is reported and retried on the next tick rather than
	// overlapping the following one.
	connectWait = 30 * time.Second

	// publishWait bounds every QoS 1 publish. These run on the dispatcher
	// goroutine, where an unbounded Wait on a dying client freezes all
	// message processing until the watchdog fires.
	publishWait = 10 * time.Second

	// unhealthyTimeout is when silence starts being reported as UNHEALTHY to
	// the container healthcheck. Deliberately shorter than stallTimeout.
	//
	// The two thresholds answer different questions. stallTimeout (5m) gates
	// RECOVERY and is the longer of the two to avoid thrashing a connection
	// over a few lost probes. unhealthyTimeout gates the health VERDICT and
	// must sour first: on 2026-08-25 the engine was deaf for twenty minutes
	// while the container reported healthy the whole time, because the
	// verdict was keyed on `stalled`. Reporting unhealthy sooner lets
	// recovery attempt the repair first, and the healthcheck's own retries
	// add further delay before anything restarts.
	//
	// Like stallTimeout, this is silence on the loopback as well as on the
	// devices: six missed probes, never a quiet house.
	unhealthyTimeout = 3 * time.Minute
)

// healthPath is where each watchdog tick records its verdict for the
// container healthcheck to read. Under /tmp because it is genuinely
// ephemeral: it is rewritten every tick and means nothing across a
// restart. Nothing outside the container reads it, so it is not a bind mount.
//
// A var rather than a const solely so tests can redirect it to a temp dir.
var healthPath = "/tmp/marshal-health"

// Engine is the core alert processing engine.
type Engine struct {
	cfg        *config.Config
	client     mqtt.Client
	tracker    *state.Tracker
	actuators  *actuator.Tracker
	alerter    *alerter.Alerter
	configPath string
	stopSweep  chan struct{}

	// inbox decouples paho's delivery goroutine from message processing.
	//
	// The client runs with order matters (paho's default), so handlers are
	// invoked synchronously on the router goroutine and MUST NOT block. The
	// engine's processing path blocks by design — publishCommand waits for
	// the QoS 1 PUBACK, which arrives through the same inbound machinery the
	// handler is holding up. Under load that is a progressive starvation:
	// on 2026-08-27 per-heartbeat message counts decayed 65 → 19 → 4 → 0
	// twice in one evening while paho reported connected the whole time.
	// The subscribe callback therefore only enqueues; a single dispatcher
	// goroutine drains the queue, preserving per-topic message order (one
	// worker) without ever blocking the router.
	inbox chan inboundMsg

	// newClient builds a REPLACEMENT MQTT client during recovery.
	//
	// Recovery cannot reuse the existing client. paho's connection status is a
	// state machine with transitions that reject Connect() outright ("status
	// can only transition to connecting from disconnected"), and the exported
	// API gives no reliable way to drive a wedged client back to a state that
	// accepts one -- IsConnected()/IsConnectionOpen() both read false while the
	// status is still `disconnecting`, and a second Disconnect() returns early
	// on an already-disconnected client, leaving exactly the state that
	// rejects Connect. Verified against paho v1.5.0 with a live broker.
	//
	// Building a new client sidesteps the state machine entirely: a fresh
	// client starts at `disconnected`, so its Connect() is always legal.
	// nil disables client replacement (tests that inject a fake client).
	newClient func() mqtt.Client

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

	// loopbackTopic is where this process publishes a probe to itself every
	// watchdogInterval. A probe that comes back proves the whole inbound
	// chain -- broker, subscription, paho's router, the inbox, the dispatcher
	// -- independently of whether any device has anything to say.
	loopbackTopic string

	// lastInboundAt is when anything last arrived, device message or loopback.
	// It is the liveness signal for both the health verdict and recovery, and
	// is only ever set by a real arrival (or to startedAt, so a fresh process
	// gets a full threshold to hear its first probe). lastMsgAt above stays
	// device-only so the heartbeat still shows how quiet the house is.
	lastInboundAt  time.Time
	lastLoopbackAt time.Time

	// lastRecoveryAt spaces recovery attempts. Kept separate from
	// lastInboundAt on purpose: faking the liveness signal to buy a retry
	// delay would make a wedged engine report healthy after every attempt.
	lastRecoveryAt time.Time
}

// mqttPublisher adapts the paho MQTT client to the alerter.Publisher interface.
type mqttPublisher struct {
	client mqtt.Client
}

func (p *mqttPublisher) Publish(topic string, payload []byte) error {
	token := p.client.Publish(topic, 1, false, payload) // QoS 1
	if !token.WaitTimeout(publishWait) {
		return fmt.Errorf("publish to %q timed out", topic)
	}
	return token.Error()
}

// New creates a new Engine.
func New(cfg *config.Config, client mqtt.Client, configPath string) *Engine {
	tracker := state.NewTracker()
	pub := &mqttPublisher{client: client}
	a := alerter.New(pub, cfg.AlertTopic)
	now := time.Now()

	return &Engine{
		cfg:           cfg,
		client:        client,
		tracker:       tracker,
		actuators:     actuator.NewTracker(),
		alerter:       a,
		configPath:    configPath,
		stopSweep:     make(chan struct{}),
		inbox:         make(chan inboundMsg, inboxDepth),
		startedAt:     now,
		loopbackTopic: newLoopbackTopic(),
		lastInboundAt: now,
	}
}

// newLoopbackTopic names the probe topic for this process. Host and pid, the
// same suffix main gives the client ID, so two overlapping instances -- a
// redeploy that briefly runs both -- cannot keep each other looking alive.
func newLoopbackTopic() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return fmt.Sprintf("marshal/loopback/%s-%d", host, os.Getpid())
}

// subscriptions is every topic the engine must be subscribed to: what the
// config requires, plus the loopback probe.
func (e *Engine) subscriptions() []string {
	return append(e.cfg.Topics(), e.loopbackTopic)
}

// inboundMsg is one MQTT message awaiting dispatch.
type inboundMsg struct {
	topic   string
	payload []byte
}

// enqueue hands a message from paho's router to the dispatcher without
// blocking. A full inbox means the dispatcher is wedged or drowning; the
// message is dropped with an error, because stalling paho's router here is
// how the whole client goes deaf.
func (e *Engine) enqueue(topic string, payload []byte) {
	select {
	case e.inbox <- inboundMsg{topic: topic, payload: payload}:
	default:
		slog.Error("inbox full, dropping message", "topic", topic, "depth", inboxDepth)
	}
}

// dispatchLoop serially processes enqueued messages until Stop.
func (e *Engine) dispatchLoop() {
	for {
		select {
		case <-e.stopSweep:
			return
		case m := <-e.inbox:
			e.handleMessage(m.topic, m.payload)
		}
	}
}

// SetClientFactory supplies the constructor used to build a replacement MQTT
// client during recovery. Without it, recovery can only retry the existing
// client, which a wedged paho state machine may refuse forever.
func (e *Engine) SetClientFactory(f func() mqtt.Client) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.newClient = f
}

// swapClient installs a replacement client and points the publisher at it.
func (e *Engine) swapClient(c mqtt.Client) {
	e.mu.Lock()
	e.client = c
	e.mu.Unlock()
	e.alerter.SetPublisher(&mqttPublisher{client: c})
}

// Start begins the dispatch, sweep, heartbeat and watchdog loops.
//
// Deliberately does NOT subscribe: the OnConnect handler already calls
// SubscribeAll on every connect, the initial one included. Subscribing here
// too registered a second handler for every topic -- harmless-looking in the
// logs (each topic "subscribed" twice) but it meant every inbound message was
// processed twice, so an edge-triggered action published its command twice.
func (e *Engine) Start() error {
	// Seed the health file before the first watchdog tick. Start is only
	// reached once the initial connect and subscribe have both succeeded, so
	// "ok" is accurate here -- and without it the container would read as
	// unhealthy until that first tick, which is a normal boot rather than a
	// fault.
	e.writeHealth(true)

	go e.dispatchLoop()
	go e.sweepLoop()
	go e.heartbeatLoop()
	go e.watchdogLoop()
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
	topics := e.subscriptions()
	slog.Info("subscribing to topics", "count", len(topics))

	for _, topic := range topics {
		t := topic // capture for closure
		// Enqueue only — processing happens on the dispatcher goroutine.
		// This callback runs on paho's router and must never block.
		token := e.client.Subscribe(t, 1, func(_ mqtt.Client, msg mqtt.Message) {
			e.enqueue(msg.Topic(), msg.Payload())
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

	topics := e.subscriptions()
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
				e.enqueue(msg.Topic(), msg.Payload())
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
	// An alert switched inactive stops being tracked. Its state is dropped
	// rather than left to go stale, so switching it back on starts the hold
	// from scratch. No "resolved" is sent for an alert that was firing.
	for _, r := range newCfg.Rules {
		if r.Alert != nil && !r.Alert.IsActive() {
			e.tracker.RemoveRule(r.Name)
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

	// The loopback probe is liveness only. It is deliberately consumed here,
	// after the inbox and on the dispatcher, so its arrival proves the whole
	// chain a device message travels -- and deliberately kept out of
	// msgCount/lastMsgAt, which report what the house is doing.
	if topic == e.loopbackTopic {
		e.mu.Lock()
		e.lastInboundAt = now
		e.lastLoopbackAt = now
		e.mu.Unlock()
		return
	}

	e.mu.Lock()
	e.msgCount++
	e.lastMsgAt = now
	e.lastInboundAt = now
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
		if rule.Alert != nil && rule.Alert.IsActive() {
			action := e.tracker.Update(rule.Name, conditionMet, rule.Alert.DurationMinutes, rule.Alert.RepeatMinutes, now, conditionMet)
			e.processAction(action, rule, now)
		}

		// Action path: edge-triggered, ownership-arbitrated.
		if rule.Action != nil {
			e.processActuation(rule, conditionMet, e.gateOK(rule, conditionMet, payload), now)
		}
	}
}

// handleControl applies override, enable and active messages for any rule
// whose control topics match. An override message marks manual ownership; an
// enable message parks or resumes automation; an active message switches
// whether the action may start new cycles.
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

	for _, rule := range e.cfg.RulesForActiveTopic(topic) {
		// An empty payload is what a subscriber sees when the retained value
		// is cleared: the decision goes back to the configured default.
		if strings.TrimSpace(string(payload)) == "" {
			if e.actuators.ClearActive(rule.Name) {
				slog.Info("action active reset to configured default",
					"rule", rule.Name, "active", rule.Action.IsActive())
			}
			continue
		}
		active, ok := parseActive(payload)
		if !ok {
			slog.Warn("unparseable active payload", "rule", rule.Name, "payload", string(payload))
			continue
		}
		// Logged only on a change: the value is retained, so the broker
		// replays it on every subscribe.
		if e.actuators.SetActive(rule.Name, active) {
			slog.Info("action active changed", "rule", rule.Name, "active", active)
		}
	}
}

// gateOK evaluates a rule's action gate against the triggering payload. True
// when no gate is configured or the condition is not met (the gate only
// matters on a rising edge). A gate that cannot be evaluated — field missing
// from the payload, type mismatch — fails open with a warning, per the
// ActionSpec.Gate contract.
func (e *Engine) gateOK(rule config.Rule, conditionMet bool, payload []byte) bool {
	if rule.Action.Gate == nil || !conditionMet {
		return true
	}
	ok, err := evaluator.EvaluateCondition(payload, *rule.Action.Gate)
	if err != nil {
		slog.Warn("gate evaluation error, failing open",
			"rule", rule.Name,
			"field", rule.Action.Gate.Field,
			"error", err,
		)
		return true
	}
	if !ok {
		slog.Debug("gate not satisfied", "rule", rule.Name, "field", rule.Action.Gate.Field)
	}
	return ok
}

// processActuation runs the edge-triggered action path for one rule.
func (e *Engine) processActuation(rule config.Rule, conditionMet, gateOK bool, now time.Time) {
	a := rule.Action

	// An inactive action is handled exactly like a failed gate, because that
	// is the behaviour wanted: no on-command on the rising edge, the edge
	// still recorded, and the off path untouched so a cycle already running
	// finishes. Ownership is not consulted or changed.
	active := e.actuators.Active(rule.Name, a.IsActive())
	if conditionMet && !active {
		slog.Debug("action inactive, not turning on", "rule", rule.Name)
	}

	cmds := e.actuators.Evaluate(
		rule.Name, conditionMet, gateOK && active,
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
	// Bounded wait: this runs on the dispatcher goroutine, and an unbounded
	// Wait on a dying client would freeze all message processing until the
	// watchdog notices. A timed-out publish is logged and abandoned.
	if !token.WaitTimeout(publishWait) {
		slog.Error("publish timed out", "rule", ruleName, "topic", c.Topic)
		return
	}
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
	if !token.WaitTimeout(publishWait) {
		slog.Error("owner state publish timed out", "rule", rule.Name)
		return
	}
	if err := token.Error(); err != nil {
		slog.Error("failed to publish owner state", "rule", rule.Name, "error", err)
	}
}

// parseActive interprets an active-topic payload. Same two wire forms as
// parseEnable -- a bare word, or a JSON object, here {"active": <v>} -- plus
// the words "active" and "inactive".
func parseActive(payload []byte) (bool, bool) {
	return parseSwitch(payload, "active", func(word string) (bool, bool) {
		switch strings.ToLower(strings.TrimSpace(word)) {
		case "active":
			return true, true
		case "inactive":
			return false, true
		}
		return parseEnableWord(word)
	})
}

// parseEnable interprets an enable-topic payload as a boolean. Two wire
// forms are accepted:
//
//   - a bare string: "on"/"true"/"1"/"enable"/"enabled" (and the off
//     counterparts) — the original convention, what the Homebridge codec
//     and mosquitto_pub speak;
//   - a JSON object {"enable": <v>} where <v> is a boolean or one of the
//     bare-string words — for publishers that can only emit JSON objects
//     (the dashboard's mqtt_publish control).
//
// Anything else — including {} and JSON without an "enable" key — is
// rejected, and the caller logs it rather than guessing.
func parseEnable(payload []byte) (bool, bool) {
	return parseSwitch(payload, "enable", parseEnableWord)
}

// parseSwitch reads a boolean from a control payload: a bare word understood
// by word, or a JSON object whose key holds a boolean or such a word.
func parseSwitch(payload []byte, key string, word func(string) (bool, bool)) (bool, bool) {
	trimmed := strings.TrimSpace(string(payload))
	if strings.HasPrefix(trimmed, "{") {
		var obj map[string]any
		if err := json.Unmarshal([]byte(trimmed), &obj); err != nil {
			return false, false
		}
		switch v := obj[key].(type) {
		case bool:
			return v, true
		case string:
			return word(v)
		}
		return false, false
	}
	return word(trimmed)
}

func parseEnableWord(word string) (bool, bool) {
	switch strings.ToLower(strings.TrimSpace(word)) {
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
// something you can alert on. A heartbeat turns that absence into a signal.
//
// Read the two ages together. `last_message_age_sec` is the house: a large
// value with `loopback_age_sec` small is simply quiet. `loopback_age_sec`
// growing past a couple of watchdog intervals is the engine going deaf, and
// that -- not device silence -- is what `stalled` reports.
//
// The heartbeat only reports. The verdict and recovery live in watchdogLoop,
// which runs often enough to notice a repair before the healthcheck gives up.
func (e *Engine) heartbeatLoop() {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	var lastMsgCount uint64

	for {
		select {
		case <-e.stopSweep:
			return
		case <-ticker.C:
			now := time.Now()

			e.mu.Lock()
			client := e.client
			msgs, cmds, sweeps := e.msgCount, e.cmdCount, e.sweepCount
			lastMsgAt, lastLoopbackAt := e.lastMsgAt, e.lastLoopbackAt
			lastInboundAt, startedAt := e.lastInboundAt, e.startedAt
			e.mu.Unlock()

			sinceLast := msgs - lastMsgCount
			lastMsgCount = msgs

			// IsConnected() reports paho's *intent* to hold a session, not
			// the health of the socket. With auto-reconnect enabled it keeps
			// returning true while the reconnect machinery believes it owns a
			// connection -- so a client evicted by the broker reports healthy
			// indefinitely. Treat prolonged silence as the real signal.
			connected := client.IsConnected()
			stalled := isStalled(connected, now.Sub(lastInboundAt))

			level := slog.LevelInfo
			if !connected || stalled {
				level = slog.LevelError
			}

			slog.Log(context.Background(), level, "heartbeat",
				"connected", connected,
				"stalled", stalled,
				"messages_total", msgs,
				"messages_since_last_heartbeat", sinceLast,
				"last_message_age_sec", ageSec(lastMsgAt, now),
				"loopback_age_sec", ageSec(lastLoopbackAt, now),
				"commands_total", cmds,
				"sweeps_total", sweeps,
				"uptime_sec", now.Sub(startedAt).Seconds(),
			)
		}
	}
}

// ageSec is the age of a timestamp in seconds, or -1 when it has never been
// set. -1 on last_message_age_sec means no device message has ever arrived;
// on loopback_age_sec it means the probe has never come back, which usually
// means subscriptions did not survive a reconnect.
func ageSec(t, now time.Time) float64 {
	if t.IsZero() {
		return -1
	}
	return now.Sub(t).Seconds()
}

// isHealthy is the verdict written for the container healthcheck: paho says
// connected AND something -- at minimum the loopback probe -- has arrived
// within unhealthyTimeout. Deliberately NOT the same test as recovery; see
// unhealthyTimeout.
func isHealthy(connected bool, silence time.Duration) bool {
	return connected && silence <= unhealthyTimeout
}

// isStalled reports a client paho believes is connected that has delivered
// nothing for stallTimeout: the signature of a half-open socket.
func isStalled(connected bool, silence time.Duration) bool {
	return connected && silence > stallTimeout
}

// needsRecovery is true whenever the client is not usable, not only when it
// is stalled.
//
// Gating recovery on `stalled` alone is unrecoverable by construction:
// `stalled` requires connected == true, so the moment a recovery attempt
// leaves the client disconnected there is no condition left that can ever
// fire again. That is exactly how the engine sat offline for fourteen hours
// on 2026-08-23, logging an ERROR heartbeat every five minutes with nothing
// acting on it.
func needsRecovery(connected bool, silence time.Duration) bool {
	return !connected || isStalled(connected, silence)
}

// watchdogLoop proves the connection is alive, records the verdict, and
// repairs the connection when it is not.
func (e *Engine) watchdogLoop() {
	ticker := time.NewTicker(watchdogInterval)
	defer ticker.Stop()

	// First probe straight away, so a healthy boot has heard itself long
	// before the first verdict is due.
	e.publishLoopback()

	for {
		select {
		case <-e.stopSweep:
			return
		case <-ticker.C:
			e.watchdogTick(time.Now())
		}
	}
}

// watchdogTick is one pass of the watchdog: verdict, recovery if due, probe.
func (e *Engine) watchdogTick(now time.Time) {
	e.mu.Lock()
	client := e.client
	silence := now.Sub(e.lastInboundAt)
	sinceRecovery := now.Sub(e.lastRecoveryAt)
	e.mu.Unlock()

	connected := client.IsConnected()

	// Written on every tick, healthy or not: the file's mtime is what proves
	// this loop is still running at all. A process wedged somewhere else
	// entirely would leave a stale "ok", and a check that only read the
	// contents would believe it.
	e.writeHealth(isHealthy(connected, silence))

	if needsRecovery(connected, silence) && sinceRecovery >= recoveryBackoff {
		slog.Error("MQTT unhealthy, reconnecting",
			"reason", map[bool]string{true: "stalled", false: "disconnected"}[connected],
			"silent_for_sec", silence.Seconds(),
			"threshold_sec", stallTimeout.Seconds(),
		)
		e.reconnect()
	}

	// Probe after any recovery, so a repaired connection proves itself on
	// this tick rather than the next.
	e.publishLoopback()
}

// publishLoopback sends the probe that handleMessage consumes.
//
// QoS 0: the point is whether it comes BACK, and an acknowledged publish
// proves nothing about the inbound half -- the half that fails silently. A
// failed publish is only logged; the missing arrival is what the watchdog
// acts on.
func (e *Engine) publishLoopback() {
	e.mu.Lock()
	client := e.client
	e.mu.Unlock()

	token := client.Publish(e.loopbackTopic, 0, false, "1")
	if !token.WaitTimeout(publishWait) {
		slog.Warn("loopback publish timed out", "topic", e.loopbackTopic)
		return
	}
	if err := token.Error(); err != nil {
		slog.Warn("loopback publish failed", "topic", e.loopbackTopic, "error", err)
	}
}

// reconnect tears down the current MQTT session and establishes a new one.
//
// Used by the watchdog to recover a half-open socket. The attempt is stamped
// first so a reconnect that itself fails silently is not repeated on the very
// next tick. lastInboundAt is deliberately left alone: only a real arrival may
// move it, otherwise every attempt would make a deaf engine read healthy.
func (e *Engine) reconnect() {
	e.mu.Lock()
	e.lastRecoveryAt = time.Now()
	factory := e.newClient
	old := e.client
	e.mu.Unlock()

	// Tear the old client down best-effort. Its state afterwards does not
	// matter, because it is being discarded -- which is the point. Reusing it
	// is what failed in production: Disconnect() returns while the status is
	// still `disconnecting`, and Connect() rejects that with "status can only
	// transition to connecting from disconnected". Both exported probes read
	// false during that window, so there is no reliable way to wait it out.
	old.Disconnect(disconnectQuiesceMS)

	if factory == nil {
		// No factory (tests, or a deliberately fixed client): retry the
		// existing one. Better than nothing, but subject to the state-machine
		// rejection described above.
		e.finishReconnect(old)
		return
	}

	// A fresh client starts at `disconnected`, so its Connect() is always a
	// legal transition regardless of how wedged the old one was.
	fresh := factory()
	if fresh == nil {
		// The factory already logged why. Returning here leaves the engine on
		// the old client, which the watchdog will try to recover again once
		// the backoff allows.
		slog.Error("no replacement MQTT client, will retry after backoff")
		return
	}

	token := fresh.Connect()
	if !token.WaitTimeout(connectWait) {
		slog.Error("MQTT reconnect timed out, will retry after backoff",
			"waited_sec", connectWait.Seconds())
		return
	}
	if err := token.Error(); err != nil {
		slog.Error("MQTT reconnect failed, will retry after backoff", "error", err)
		return
	}

	// Only adopt the new client once it is actually connected, so a failed
	// attempt leaves the engine pointing at something known rather than at a
	// half-built replacement.
	e.swapClient(fresh)
	e.finishReconnect(fresh)
}

// finishReconnect resubscribes on a freshly connected client.
//
// Connect() does not restore subscriptions under CleanSession, so without this
// the engine reconnects, reports healthy, and silently receives nothing.
func (e *Engine) finishReconnect(c mqtt.Client) {
	if !c.IsConnected() {
		token := c.Connect()
		if !token.WaitTimeout(connectWait) {
			slog.Error("MQTT reconnect timed out, will retry after backoff")
			return
		}
		if err := token.Error(); err != nil {
			slog.Error("MQTT reconnect failed, will retry after backoff", "error", err)
			return
		}
	}

	if err := e.SubscribeAll(); err != nil {
		// Connected but deaf -- the same silent failure the watchdog exists
		// to catch. Left as-is deliberately: the loopback will not come back,
		// so the watchdog drives another recovery once the backoff allows.
		slog.Error("resubscribe after recovery failed, will retry after backoff", "error", err)
		return
	}
	slog.Info("MQTT recovered")
}

// writeHealth records the latest watchdog verdict where the container
// healthcheck can read it.
//
// The file carries "ok" or "unhealthy", and its mtime carries the liveness of
// the watchdog loop itself. The healthcheck requires both: recent content AND
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
		if r.Alert == nil || !r.Alert.IsActive() {
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
