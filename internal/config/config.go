package config

import (
	"fmt"
	"os"
	"slices"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for the alert engine.
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

// Rule defines a single alert rule.
type Rule struct {
	Name            string    `yaml:"name"`
	Topic           string    `yaml:"topic"`
	Condition       Condition `yaml:"condition"`
	DurationMinutes int       `yaml:"duration_minutes"`
	RepeatMinutes   int       `yaml:"repeat_minutes"`
	Severity        string    `yaml:"severity"`
	Message         string    `yaml:"message"`
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

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	return &cfg, nil
}

// Validate checks the config for required fields and valid values.
func (c *Config) Validate() error {
	if c.MQTT.Broker == "" {
		return fmt.Errorf("mqtt.broker is required")
	}
	if c.MQTT.ClientID == "" {
		return fmt.Errorf("mqtt.client_id is required")
	}
	if c.AlertTopic == "" {
		return fmt.Errorf("alert_topic is required")
	}
	if len(c.Rules) == 0 {
		return fmt.Errorf("at least one rule is required")
	}

	names := make(map[string]bool)
	for i, r := range c.Rules {
		if err := r.validate(i); err != nil {
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
	if r.DurationMinutes < 0 {
		return fmt.Errorf("rule[%d] %q: duration_minutes must be >= 0", index, r.Name)
	}
	if r.Severity == "" {
		r.Severity = "warning"
	}
	if !slices.Contains(validSeverities, r.Severity) {
		return fmt.Errorf("rule[%d] %q: invalid severity %q (must be one of %v)", index, r.Name, r.Severity, validSeverities)
	}
	if r.Message == "" {
		return fmt.Errorf("rule[%d] %q: message is required", index, r.Name)
	}
	return nil
}

// Topics returns the deduplicated set of MQTT topics from all rules.
func (c *Config) Topics() []string {
	seen := make(map[string]bool)
	var topics []string
	for _, r := range c.Rules {
		if !seen[r.Topic] {
			seen[r.Topic] = true
			topics = append(topics, r.Topic)
		}
	}
	return topics
}

// RulesForTopic returns all rules matching a given MQTT topic.
func (c *Config) RulesForTopic(topic string) []Rule {
	var matched []Rule
	for _, r := range c.Rules {
		if r.Topic == topic {
			matched = append(matched, r)
		}
	}
	return matched
}
