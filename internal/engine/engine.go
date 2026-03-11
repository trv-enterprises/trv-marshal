package engine

import (
	"fmt"
	"log/slog"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/trv-homelab/sensor-alert-engine/internal/alerter"
	"github.com/trv-homelab/sensor-alert-engine/internal/config"
	"github.com/trv-homelab/sensor-alert-engine/internal/evaluator"
	"github.com/trv-homelab/sensor-alert-engine/internal/state"
)

const sweepInterval = 30 * time.Second

// Engine is the core alert processing engine.
type Engine struct {
	cfg        *config.Config
	client     mqtt.Client
	tracker    *state.Tracker
	alerter    *alerter.Alerter
	configPath string
	stopSweep  chan struct{}
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
		alerter:    a,
		configPath: configPath,
		stopSweep:  make(chan struct{}),
	}
}

// Start subscribes to MQTT topics and begins the sweep ticker.
func (e *Engine) Start() error {
	topics := e.cfg.Topics()
	slog.Info("subscribing to topics", "count", len(topics))

	for _, topic := range topics {
		t := topic // capture for closure
		token := e.client.Subscribe(t, 1, func(_ mqtt.Client, msg mqtt.Message) {
			e.handleMessage(msg.Topic(), msg.Payload())
		})
		token.Wait()
		if err := token.Error(); err != nil {
			return fmt.Errorf("subscribing to %q: %w", t, err)
		}
		slog.Info("subscribed", "topic", t)
	}

	go e.sweepLoop()
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

// handleMessage processes an incoming MQTT message against all matching rules.
func (e *Engine) handleMessage(topic string, payload []byte) {
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

		now := time.Now()
		action := e.tracker.Update(rule.Name, conditionMet, rule.DurationMinutes, rule.RepeatMinutes, now, conditionMet)
		e.processAction(action, rule, now)
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
	rules := make(map[string]struct{ DurationMin, RepeatMin int })
	for _, r := range e.cfg.Rules {
		rules[r.Name] = struct{ DurationMin, RepeatMin int }{r.DurationMinutes, r.RepeatMinutes}
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
}

// processAction fires an alert based on the state machine action.
func (e *Engine) processAction(action state.Action, rule config.Rule, now time.Time) {
	if action == state.ActionNone {
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

	message := alerter.RenderMessage(rule.Message, vars)

	if err := e.alerter.SendAlert(alertType, rule.Severity, rule.Name, message, device); err != nil {
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
