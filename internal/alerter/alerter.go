package alerter

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Alert represents an alert event published to MQTT.
type Alert struct {
	Type      string `json:"type"`      // "new", "repeat", or "resolved"
	Source    string `json:"source"`    // always "alert_engine"
	Severity  string `json:"severity"`
	Rule      string `json:"rule"`
	Message   string `json:"message"`
	Device    string `json:"device"`
	Timestamp string `json:"timestamp"`
}

// Publisher is an interface for publishing alert messages.
type Publisher interface {
	Publish(topic string, payload []byte) error
}

// Alerter builds and publishes alert events.
type Alerter struct {
	publisher  Publisher
	alertTopic string
}

// New creates a new Alerter.
func New(publisher Publisher, alertTopic string) *Alerter {
	return &Alerter{
		publisher:  publisher,
		alertTopic: alertTopic,
	}
}

// SendAlert publishes an alert event.
func (a *Alerter) SendAlert(alertType, severity, ruleName, message, device string) error {
	alert := Alert{
		Type:      alertType,
		Source:    "alert_engine",
		Severity:  severity,
		Rule:      ruleName,
		Message:   message,
		Device:    device,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	payload, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("marshaling alert: %w", err)
	}

	slog.Info("publishing alert",
		"type", alertType,
		"rule", ruleName,
		"severity", severity,
		"device", device,
	)

	return a.publisher.Publish(a.alertTopic, payload)
}

// RenderMessage replaces template variables in a message string.
// Supported variables: {duration}, {value}, {field}, {device}, {name}
func RenderMessage(template string, vars map[string]string) string {
	result := template
	for k, v := range vars {
		result = strings.ReplaceAll(result, "{"+k+"}", v)
	}
	return result
}

// FormatDuration returns a human-readable duration string.
func FormatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%d seconds", int(d.Seconds()))
	}
	minutes := int(d.Minutes())
	if minutes == 1 {
		return "1 minute"
	}
	if minutes < 60 {
		return fmt.Sprintf("%d minutes", minutes)
	}
	hours := minutes / 60
	remainMin := minutes % 60
	if remainMin == 0 {
		if hours == 1 {
			return "1 hour"
		}
		return fmt.Sprintf("%d hours", hours)
	}
	if hours == 1 {
		return fmt.Sprintf("1 hour %d minutes", remainMin)
	}
	return fmt.Sprintf("%d hours %d minutes", hours, remainMin)
}

// DeviceFromTopic extracts the device name from an MQTT topic.
// e.g., "zigbee2mqtt/garage_door_1" → "garage_door_1"
func DeviceFromTopic(topic string) string {
	parts := strings.Split(topic, "/")
	if len(parts) == 0 {
		return topic
	}
	return parts[len(parts)-1]
}
