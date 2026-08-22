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

// --- action path / backward compatibility ---

const mqttHeader = `
mqtt:
  broker: "tcp://mosquitto:1883"
  client_id: "automation-engine"
alert_topic: "sensors/alerts"
rules:
`

func TestLegacyFlatRuleStillLoads(t *testing.T) {
	cfg, err := Load(writeTemp(t, mqttHeader+`
  - name: legacy
    topic: "zigbee2mqtt/Door"
    condition: {field: contact, operator: eq, value: false}
    duration_minutes: 30
    repeat_minutes: 60
    severity: warning
    message: "open for {duration}"
`))
	if err != nil {
		t.Fatalf("legacy rule failed to load: %v", err)
	}
	r := cfg.Rules[0]
	if r.Alert == nil {
		t.Fatal("legacy rule should normalize into an Alert block")
	}
	if r.Alert.DurationMinutes != 30 || r.Alert.RepeatMinutes != 60 {
		t.Fatalf("legacy fields not carried over: %+v", r.Alert)
	}
	if r.Action != nil {
		t.Fatal("legacy rule should have no action")
	}
}

func TestActionOnlyRuleNeedsNoAlertFields(t *testing.T) {
	cfg, err := Load(writeTemp(t, mqttHeader+`
  - name: nightlight
    topic: "zigbee2mqtt/nl"
    condition: {field: occupancy, operator: eq, value: true}
    action:
      topic: "zigbee2mqtt/nl/set"
      payload: '{"state":"ON"}'
      off_payload: '{"state":"OFF"}'
      off_delay_seconds: 120
      override_topic: "zigbee2mqtt/nl/set"
      override_ttl_minutes: 30
`))
	if err != nil {
		t.Fatalf("action rule failed to load: %v", err)
	}
	if cfg.Rules[0].Alert != nil {
		t.Fatal("action-only rule must not synthesize an alert")
	}
	if cfg.Rules[0].Action.OffTopicOrDefault() != "zigbee2mqtt/nl/set" {
		t.Fatal("off topic should default to action topic")
	}
}

func TestRuleWithNeitherAlertNorActionIsRejected(t *testing.T) {
	// A rule with no alert fields and no action normalizes to an Alert with an
	// empty message, which validation must reject.
	_, err := Load(writeTemp(t, mqttHeader+`
  - name: empty
    topic: "t"
    condition: {field: f, operator: eq, value: true}
`))
	if err == nil {
		t.Fatal("expected rejection of rule with no alert and no action")
	}
}

func TestOverrideTopicRequiresTTL(t *testing.T) {
	_, err := Load(writeTemp(t, mqttHeader+`
  - name: bad
    topic: "t"
    condition: {field: f, operator: eq, value: true}
    action:
      topic: "x/set"
      payload: "ON"
      override_topic: "x/set"
`))
	if err == nil {
		t.Fatal("expected error when override_topic set without a TTL")
	}
}

func TestOffDelayRequiresOffPayload(t *testing.T) {
	_, err := Load(writeTemp(t, mqttHeader+`
  - name: bad
    topic: "t"
    condition: {field: f, operator: eq, value: true}
    action:
      topic: "x/set"
      payload: "ON"
      off_delay_seconds: 60
`))
	if err == nil {
		t.Fatal("expected error when off_delay_seconds set without off_payload")
	}
}

func TestTopicsIncludeControlTopics(t *testing.T) {
	cfg, err := Load(writeTemp(t, mqttHeader+`
  - name: nightlight
    topic: "zigbee2mqtt/nl"
    condition: {field: occupancy, operator: eq, value: true}
    action:
      topic: "zigbee2mqtt/nl/set"
      payload: "ON"
      override_topic: "zigbee2mqtt/nl/set"
      override_ttl_minutes: 30
      enable_topic: "automation/nl/enable"
`))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, tp := range cfg.Topics() {
		got[tp] = true
	}
	for _, want := range []string{"zigbee2mqtt/nl", "zigbee2mqtt/nl/set", "automation/nl/enable"} {
		if !got[want] {
			t.Fatalf("Topics() missing %q, got %v", want, cfg.Topics())
		}
	}
}
