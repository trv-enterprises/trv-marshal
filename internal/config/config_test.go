package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad(t *testing.T) {
	yaml := `
mqtt:
  broker: "tcp://localhost:1883"
  client_id: "test-engine"

alert_topic: "sensors/alerts"

rules:
  - name: garage_door_open
    topic: "zigbee2mqtt/garage_door_1"
    condition:
      field: "contact"
      operator: "eq"
      value: false
    duration_minutes: 30
    repeat_minutes: 60
    severity: "warning"
    message: "Garage door has been open for {duration}"
  - name: moisture_detected
    topic: "zigbee2mqtt/moisture_1"
    condition:
      field: "water_leak"
      operator: "eq"
      value: true
    duration_minutes: 0
    severity: "critical"
    message: "Water leak detected at {device}"
`
	path := writeTemp(t, yaml)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.MQTT.Broker != "tcp://localhost:1883" {
		t.Errorf("broker = %q, want tcp://localhost:1883", cfg.MQTT.Broker)
	}
	if len(cfg.Rules) != 2 {
		t.Fatalf("len(rules) = %d, want 2", len(cfg.Rules))
	}
	if cfg.Rules[0].Name != "garage_door_open" {
		t.Errorf("rules[0].name = %q, want garage_door_open", cfg.Rules[0].Name)
	}
	if cfg.Rules[0].DurationMinutes != 30 {
		t.Errorf("rules[0].duration_minutes = %d, want 30", cfg.Rules[0].DurationMinutes)
	}
	// YAML parses false as bool
	if cfg.Rules[0].Condition.Value != false {
		t.Errorf("rules[0].condition.value = %v, want false", cfg.Rules[0].Condition.Value)
	}
}

func TestValidate_MissingBroker(t *testing.T) {
	cfg := &Config{
		MQTT:       MQTTConfig{ClientID: "test"},
		AlertTopic: "alerts",
		Rules:      []Rule{{Name: "r", Topic: "t", Condition: Condition{Field: "f", Operator: "eq", Value: true}, Message: "m"}},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing broker")
	}
}

func TestValidate_DuplicateRuleNames(t *testing.T) {
	cfg := &Config{
		MQTT:       MQTTConfig{Broker: "tcp://localhost:1883", ClientID: "test"},
		AlertTopic: "alerts",
		Rules: []Rule{
			{Name: "dup", Topic: "t1", Condition: Condition{Field: "f", Operator: "eq", Value: true}, Severity: "warning", Message: "m"},
			{Name: "dup", Topic: "t2", Condition: Condition{Field: "f", Operator: "eq", Value: true}, Severity: "warning", Message: "m"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for duplicate rule names")
	}
}

func TestValidate_InvalidOperator(t *testing.T) {
	cfg := &Config{
		MQTT:       MQTTConfig{Broker: "tcp://localhost:1883", ClientID: "test"},
		AlertTopic: "alerts",
		Rules: []Rule{
			{Name: "r", Topic: "t", Condition: Condition{Field: "f", Operator: "contains", Value: true}, Severity: "warning", Message: "m"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid operator")
	}
}

func TestValidate_InvalidSeverity(t *testing.T) {
	cfg := &Config{
		MQTT:       MQTTConfig{Broker: "tcp://localhost:1883", ClientID: "test"},
		AlertTopic: "alerts",
		Rules: []Rule{
			{Name: "r", Topic: "t", Condition: Condition{Field: "f", Operator: "eq", Value: true}, Severity: "emergency", Message: "m"},
		},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid severity")
	}
}

func TestTopics(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Topic: "zigbee2mqtt/door"},
			{Topic: "zigbee2mqtt/moisture"},
			{Topic: "zigbee2mqtt/door"}, // duplicate
		},
	}
	topics := cfg.Topics()
	if len(topics) != 2 {
		t.Errorf("len(topics) = %d, want 2", len(topics))
	}
}

func TestRulesForTopic(t *testing.T) {
	cfg := &Config{
		Rules: []Rule{
			{Name: "r1", Topic: "topic/a"},
			{Name: "r2", Topic: "topic/b"},
			{Name: "r3", Topic: "topic/a"},
		},
	}
	matched := cfg.RulesForTopic("topic/a")
	if len(matched) != 2 {
		t.Errorf("len(matched) = %d, want 2", len(matched))
	}
}

func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
