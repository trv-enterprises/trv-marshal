package alerter

import (
	"encoding/json"
	"testing"
	"time"
)

type mockPublisher struct {
	lastTopic   string
	lastPayload []byte
	err         error
}

func (m *mockPublisher) Publish(topic string, payload []byte) error {
	m.lastTopic = topic
	m.lastPayload = payload
	return m.err
}

func TestSendAlert(t *testing.T) {
	pub := &mockPublisher{}
	a := New(pub, "sensors/alerts")

	err := a.SendAlert("new", "warning", "garage_door_open", "Garage door open for 30 minutes", "garage_door_1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if pub.lastTopic != "sensors/alerts" {
		t.Errorf("topic = %q, want sensors/alerts", pub.lastTopic)
	}

	var alert Alert
	if err := json.Unmarshal(pub.lastPayload, &alert); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if alert.Type != "new" {
		t.Errorf("type = %q, want new", alert.Type)
	}
	if alert.Source != "alert_engine" {
		t.Errorf("source = %q, want alert_engine", alert.Source)
	}
	if alert.Severity != "warning" {
		t.Errorf("severity = %q, want warning", alert.Severity)
	}
	if alert.Rule != "garage_door_open" {
		t.Errorf("rule = %q, want garage_door_open", alert.Rule)
	}
	if alert.Device != "garage_door_1" {
		t.Errorf("device = %q, want garage_door_1", alert.Device)
	}
	if alert.Timestamp == "" {
		t.Error("timestamp should not be empty")
	}
}

func TestRenderMessage(t *testing.T) {
	tests := []struct {
		template string
		vars     map[string]string
		want     string
	}{
		{
			template: "Garage door has been open for {duration}",
			vars:     map[string]string{"duration": "30 minutes"},
			want:     "Garage door has been open for 30 minutes",
		},
		{
			template: "Water leak detected at {device}",
			vars:     map[string]string{"device": "basement_sensor"},
			want:     "Water leak detected at basement_sensor",
		},
		{
			template: "{name}: {field} is {value}",
			vars:     map[string]string{"name": "temp_alert", "field": "temperature", "value": "95.2"},
			want:     "temp_alert: temperature is 95.2",
		},
		{
			template: "No variables here",
			vars:     map[string]string{},
			want:     "No variables here",
		},
	}

	for _, tt := range tests {
		got := RenderMessage(tt.template, tt.vars)
		if got != tt.want {
			t.Errorf("RenderMessage(%q) = %q, want %q", tt.template, got, tt.want)
		}
	}
}

func TestFormatDuration(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{30 * time.Second, "30 seconds"},
		{1 * time.Minute, "1 minute"},
		{5 * time.Minute, "5 minutes"},
		{30 * time.Minute, "30 minutes"},
		{60 * time.Minute, "1 hour"},
		{90 * time.Minute, "1 hour 30 minutes"},
		{120 * time.Minute, "2 hours"},
		{150 * time.Minute, "2 hours 30 minutes"},
	}

	for _, tt := range tests {
		got := FormatDuration(tt.d)
		if got != tt.want {
			t.Errorf("FormatDuration(%v) = %q, want %q", tt.d, got, tt.want)
		}
	}
}

func TestDeviceFromTopic(t *testing.T) {
	tests := []struct {
		topic string
		want  string
	}{
		{"zigbee2mqtt/garage_door_1", "garage_door_1"},
		{"zigbee2mqtt/moisture_1", "moisture_1"},
		{"caseta/kitchen_lights", "kitchen_lights"},
		{"simple", "simple"},
	}

	for _, tt := range tests {
		got := DeviceFromTopic(tt.topic)
		if got != tt.want {
			t.Errorf("DeviceFromTopic(%q) = %q, want %q", tt.topic, got, tt.want)
		}
	}
}
