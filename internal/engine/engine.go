package engine

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-homelab/sensor-alert-engine/internal/actuator"
	"github.com/trv-homelab/sensor-alert-engine/internal/alerter"
	"github.com/trv-homelab/sensor-alert-engine/internal/config"
	"github.com/trv-homelab/sensor-alert-engine/internal/evaluator"
	"github.com/trv-homelab/sensor-alert-engine/internal/state"
)

const (
	sweepInterval = 30 * time.Second

	// heartbeatInterval is how often the engine reports liveness. Long enough
	// to stay out of the way in normal operation, short enough that a stall is
	// obvious within a few minutes rather than hours.
	heartbeatInterval = 5 * time.Minute
)

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
func (e *Engine) Start() error {
	if err := e.SubscribeAll(); err != nil {
		return err
	}

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

			connected := e.client.IsConnected()

			level := slog.LevelInfo
			// No messages in a whole interval is not automatically wrong --
			// a quiet house is quiet -- but combined with a lost connection
			// it is worth surfacing louder.
			if !connected {
				level = slog.LevelError
			}

			slog.Log(context.Background(), level, "heartbeat",
				"connected", connected,
				"messages_total", msgs,
				"messages_since_last_heartbeat", sinceLast,
				"last_message_age_sec", ageSec,
				"commands_total", cmds,
				"sweeps_total", sweeps,
				"uptime_sec", time.Since(startedAt).Seconds(),
			)
		}
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
