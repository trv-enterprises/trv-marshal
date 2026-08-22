package config

import (
	"fmt"
	"os"
	"slices"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the automation engine.
type Config struct {
	MQTT       MQTTConfig `yaml:"mqtt"`
	AlertTopic string     `yaml:"alert_topic"`
	Rules      []Rule     `yaml:"rules"`
}

// MQTTConfig holds MQTT broker connection settings.
type MQTTConfig struct {
	Broker   string `yaml:"broker"`
	ClientID string `yaml:"client_id"`
}

// Rule defines a single automation rule. A rule always has a condition, and
// carries an alert block, an action block, or both:
//
//   - alert  — level-triggered. Fires once the condition has held for
//     duration_minutes, repeats every repeat_minutes, and resolves when the
//     condition clears.
//   - action — edge-triggered. Publishes a command the moment the condition
//     becomes true, subject to manual-override arbitration.
//
// The legacy flat alert fields (duration_minutes, repeat_minutes, severity,
// message) are still accepted and are normalized into Alert at load time so
// pre-existing rules.yaml files keep working unchanged.
type Rule struct {
	Name      string    `yaml:"name"`
	Topic     string    `yaml:"topic"`
	Condition Condition `yaml:"condition"`

	Alert  *AlertSpec  `yaml:"alert"`
	Action *ActionSpec `yaml:"action"`

	// Deprecated: legacy flat alert fields, normalized into Alert on load.
	DurationMinutes int    `yaml:"duration_minutes"`
	RepeatMinutes   int    `yaml:"repeat_minutes"`
	Severity        string `yaml:"severity"`
	Message         string `yaml:"message"`
}

// AlertSpec configures the level-triggered alerting path.
type AlertSpec struct {
	DurationMinutes int    `yaml:"duration_minutes"`
	RepeatMinutes   int    `yaml:"repeat_minutes"`
	Severity        string `yaml:"severity"`
	Message         string `yaml:"message"`
}

// ActionSpec configures the edge-triggered actuation path.
//
// On the rising edge of the condition the engine publishes Payload to Topic.
// When the condition clears it waits OffDelaySeconds and then publishes
// OffPayload to OffTopic (defaulting to Topic), so a motion rule can hold a
// light on for a fixed window after the last movement.
type ActionSpec struct {
	Topic   string `yaml:"topic"`
	Payload string `yaml:"payload"`

	OffTopic        string `yaml:"off_topic"`
	OffPayload      string `yaml:"off_payload"`
	OffDelaySeconds int    `yaml:"off_delay_seconds"`

	// Override arbitration. When a manual command is seen on OverrideTopic the
	// rule yields control for OverrideTTLMinutes, after which automation
	// resumes. Publishing "false"/"OFF" to EnableTopic parks the rule
	// indefinitely; "true"/"ON" clears both the park and any active override.
	OverrideTopic     string `yaml:"override_topic"`
	OverrideTTLMinutes int   `yaml:"override_ttl_minutes"`
	EnableTopic       string `yaml:"enable_topic"`
	StateTopic        string `yaml:"state_topic"`
}

// Condition defines the field comparison for a rule.
type Condition struct {
	Field    string `yaml:"field"`
	Operator string `yaml:"operator"`
	Value    any    `yaml:"value"`
}

var validOperators = []string{"eq", "ne", "lt", "gt", "le", "ge"}
var validSeverities = []string{"info", "warning", "critical"}

// Load reads and parses a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config: %w", err)
	}

	cfg.normalize()

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// normalize folds the legacy flat alert fields into an AlertSpec. A rule that
// specifies neither an alert nor an action block is treated as alert-only,
// which is how every rule written before the action path existed behaves.
func (c *Config) normalize() {
	for i := range c.Rules {
		r := &c.Rules[i]
		if r.Alert != nil || r.Action != nil {
			continue
		}
		r.Alert = &AlertSpec{
			DurationMinutes: r.DurationMinutes,
			RepeatMinutes:   r.RepeatMinutes,
			Severity:        r.Severity,
			Message:         r.Message,
		}
	}
}

// Validate checks the config for required fields and valid values.
func (c *Config) Validate() error {
	if c.MQTT.Broker == "" {
		return fmt.Errorf("mqtt.broker is required")
	}
	if c.MQTT.ClientID == "" {
		return fmt.Errorf("mqtt.client_id is required")
	}
	if len(c.Rules) == 0 {
		return fmt.Errorf("at least one rule is required")
	}
	// alert_topic is only required if some rule actually alerts.
	for _, r := range c.Rules {
		if r.Alert != nil && c.AlertTopic == "" {
			return fmt.Errorf("alert_topic is required when any rule has an alert")
		}
	}

	names := make(map[string]bool)
	for i, r := range c.Rules {
		if err := c.Rules[i].validate(i); err != nil {
			return err
		}
		if names[r.Name] {
			return fmt.Errorf("rule[%d]: duplicate name %q", i, r.Name)
		}
		names[r.Name] = true
	}

	return nil
}

func (r *Rule) validate(index int) error {
	if r.Name == "" {
		return fmt.Errorf("rule[%d]: name is required", index)
	}
	if r.Topic == "" {
		return fmt.Errorf("rule[%d] %q: topic is required", index, r.Name)
	}
	if r.Condition.Field == "" {
		return fmt.Errorf("rule[%d] %q: condition.field is required", index, r.Name)
	}
	if !slices.Contains(validOperators, r.Condition.Operator) {
		return fmt.Errorf("rule[%d] %q: invalid operator %q (must be one of %v)", index, r.Name, r.Condition.Operator, validOperators)
	}
	if r.Condition.Value == nil {
		return fmt.Errorf("rule[%d] %q: condition.value is required", index, r.Name)
	}
	if r.Alert == nil && r.Action == nil {
		return fmt.Errorf("rule[%d] %q: rule must define an alert, an action, or both", index, r.Name)
	}
	if r.Alert != nil {
		if err := r.Alert.validate(index, r.Name); err != nil {
			return err
		}
	}
	if r.Action != nil {
		if err := r.Action.validate(index, r.Name); err != nil {
			return err
		}
	}
	return nil
}

func (a *AlertSpec) validate(index int, name string) error {
	if a.DurationMinutes < 0 {
		return fmt.Errorf("rule[%d] %q: duration_minutes must be >= 0", index, name)
	}
	if a.RepeatMinutes < 0 {
		return fmt.Errorf("rule[%d] %q: repeat_minutes must be >= 0", index, name)
	}
	if a.Severity == "" {
		a.Severity = "warning"
	}
	if !slices.Contains(validSeverities, a.Severity) {
		return fmt.Errorf("rule[%d] %q: invalid severity %q (must be one of %v)", index, name, a.Severity, validSeverities)
	}
	if a.Message == "" {
		return fmt.Errorf("rule[%d] %q: message is required", index, name)
	}
	return nil
}

func (a *ActionSpec) validate(index int, name string) error {
	if a.Topic == "" {
		return fmt.Errorf("rule[%d] %q: action.topic is required", index, name)
	}
	if a.Payload == "" {
		return fmt.Errorf("rule[%d] %q: action.payload is required", index, name)
	}
	if a.OffDelaySeconds < 0 {
		return fmt.Errorf("rule[%d] %q: action.off_delay_seconds must be >= 0", index, name)
	}
	if a.OverrideTTLMinutes < 0 {
		return fmt.Errorf("rule[%d] %q: action.override_ttl_minutes must be >= 0", index, name)
	}
	if a.OverrideTopic != "" && a.OverrideTTLMinutes == 0 {
		return fmt.Errorf("rule[%d] %q: action.override_ttl_minutes must be > 0 when override_topic is set", index, name)
	}
	// An off_payload with no off path configured is a silent no-op; catch it.
	if a.OffPayload == "" && (a.OffTopic != "" || a.OffDelaySeconds > 0) {
		return fmt.Errorf("rule[%d] %q: action.off_payload is required when off_topic or off_delay_seconds is set", index, name)
	}
	return nil
}

// OffTopicOrDefault returns the topic used for the off command.
func (a *ActionSpec) OffTopicOrDefault() string {
	if a.OffTopic != "" {
		return a.OffTopic
	}
	return a.Topic
}

// Topics returns the deduplicated set of MQTT topics the engine must
// subscribe to: every rule's trigger topic plus any override/enable
// control topics.
func (c *Config) Topics() []string {
	seen := make(map[string]bool)
	var topics []string
	add := func(t string) {
		if t == "" || seen[t] {
			return
		}
		seen[t] = true
		topics = append(topics, t)
	}
	for _, r := range c.Rules {
		add(r.Topic)
		if r.Action != nil {
			add(r.Action.OverrideTopic)
			add(r.Action.EnableTopic)
		}
	}
	return topics
}

// RulesForTopic returns all rules whose trigger topic matches.
func (c *Config) RulesForTopic(topic string) []Rule {
	var matched []Rule
	for _, r := range c.Rules {
		if r.Topic == topic {
			matched = append(matched, r)
		}
	}
	return matched
}

// RulesForControlTopic returns rules whose override or enable topic matches,
// along with which kind of control topic it was.
func (c *Config) RulesForControlTopic(topic string) (override, enable []Rule) {
	for _, r := range c.Rules {
		if r.Action == nil {
			continue
		}
		if r.Action.OverrideTopic == topic {
			override = append(override, r)
		}
		if r.Action.EnableTopic == topic {
			enable = append(enable, r)
		}
	}
	return override, enable
}
