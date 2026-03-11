package evaluator

import (
	"testing"

	"github.com/trv-homelab/sensor-alert-engine/internal/config"
)

func TestEvaluate_BoolEqual(t *testing.T) {
	payload := []byte(`{"contact": false, "battery": 97}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "contact", Operator: "eq", Value: false},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true, got false")
	}
}

func TestEvaluate_BoolNotEqual(t *testing.T) {
	payload := []byte(`{"contact": true}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "contact", Operator: "eq", Value: false},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result {
		t.Error("expected false, got true")
	}
}

func TestEvaluate_NumericLessThan(t *testing.T) {
	payload := []byte(`{"temperature": 2.5}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "temperature", Operator: "lt", Value: 5},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for 2.5 < 5")
	}
}

func TestEvaluate_NumericGreaterThan(t *testing.T) {
	payload := []byte(`{"humidity": 85.2}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "humidity", Operator: "gt", Value: 80},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for 85.2 > 80")
	}
}

func TestEvaluate_NumericEqual(t *testing.T) {
	// JSON numbers are float64, YAML ints become Go int — test coercion
	payload := []byte(`{"battery": 100}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "battery", Operator: "eq", Value: 100},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for 100 == 100")
	}
}

func TestEvaluate_NestedField(t *testing.T) {
	payload := []byte(`{"action": {"state": "on"}}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "action.state", Operator: "eq", Value: "on"},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for nested field")
	}
}

func TestEvaluate_MissingField(t *testing.T) {
	payload := []byte(`{"battery": 97}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "contact", Operator: "eq", Value: false},
	}
	_, err := Evaluate(payload, rule)
	if err == nil {
		t.Error("expected error for missing field")
	}
}

func TestEvaluate_InvalidJSON(t *testing.T) {
	payload := []byte(`not json`)
	rule := config.Rule{
		Condition: config.Condition{Field: "f", Operator: "eq", Value: true},
	}
	_, err := Evaluate(payload, rule)
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestEvaluate_StringEqual(t *testing.T) {
	payload := []byte(`{"state": "ON"}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "state", Operator: "eq", Value: "ON"},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for string equality")
	}
}

func TestEvaluate_StringNotEqual(t *testing.T) {
	payload := []byte(`{"state": "OFF"}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "state", Operator: "ne", Value: "ON"},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for string not-equal")
	}
}

func TestEvaluate_BoolNe(t *testing.T) {
	payload := []byte(`{"water_leak": true}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "water_leak", Operator: "ne", Value: false},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for bool ne")
	}
}

func TestEvaluate_NumericLe(t *testing.T) {
	payload := []byte(`{"battery": 10}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "battery", Operator: "le", Value: 10},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for 10 <= 10")
	}
}

func TestEvaluate_NumericGe(t *testing.T) {
	payload := []byte(`{"battery": 50}`)
	rule := config.Rule{
		Condition: config.Condition{Field: "battery", Operator: "ge", Value: 50},
	}
	result, err := Evaluate(payload, rule)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result {
		t.Error("expected true for 50 >= 50")
	}
}
