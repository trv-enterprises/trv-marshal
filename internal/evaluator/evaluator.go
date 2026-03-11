package evaluator

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/trv-homelab/sensor-alert-engine/internal/config"
)

// Evaluate checks whether a JSON payload satisfies a rule's condition.
// Returns true if the condition is met (alert state).
func Evaluate(payload []byte, rule config.Rule) (bool, error) {
	var data map[string]any
	if err := json.Unmarshal(payload, &data); err != nil {
		return false, fmt.Errorf("parsing JSON: %w", err)
	}

	actual, err := extractField(data, rule.Condition.Field)
	if err != nil {
		return false, err
	}

	return compare(actual, rule.Condition.Operator, rule.Condition.Value)
}

// extractField retrieves a value from a nested map using dot-notation.
// e.g., "before.severity" extracts data["before"]["severity"].
func extractField(data map[string]any, path string) (any, error) {
	parts := strings.Split(path, ".")
	var current any = data

	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("field %q: expected object, got %T", path, current)
		}
		val, exists := m[part]
		if !exists {
			return nil, fmt.Errorf("field %q: key %q not found", path, part)
		}
		current = val
	}

	return current, nil
}

// compare performs operator-based comparison between actual (from JSON) and
// expected (from YAML config) values.
func compare(actual any, operator string, expected any) (bool, error) {
	// Bool comparison — only eq/ne are meaningful
	if ab, ok := actual.(bool); ok {
		eb, ok := toBool(expected)
		if !ok {
			return false, fmt.Errorf("cannot compare bool with %T", expected)
		}
		switch operator {
		case "eq":
			return ab == eb, nil
		case "ne":
			return ab != eb, nil
		default:
			return false, fmt.Errorf("operator %q not supported for bool", operator)
		}
	}

	// Numeric comparison
	af, aOk := toFloat64(actual)
	ef, eOk := toFloat64(expected)
	if aOk && eOk {
		switch operator {
		case "eq":
			return af == ef, nil
		case "ne":
			return af != ef, nil
		case "lt":
			return af < ef, nil
		case "gt":
			return af > ef, nil
		case "le":
			return af <= ef, nil
		case "ge":
			return af >= ef, nil
		default:
			return false, fmt.Errorf("unknown operator %q", operator)
		}
	}

	// String comparison — eq/ne only
	as, aStr := actual.(string)
	es, eStr := expected.(string)
	if aStr && eStr {
		switch operator {
		case "eq":
			return as == es, nil
		case "ne":
			return as != es, nil
		default:
			return false, fmt.Errorf("operator %q not supported for string", operator)
		}
	}

	return false, fmt.Errorf("cannot compare %T with %T using %q", actual, expected, operator)
}

// toFloat64 converts numeric types (int, float64, json.Number) to float64.
func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

// toBool converts a value to bool if possible.
func toBool(v any) (bool, bool) {
	b, ok := v.(bool)
	return b, ok
}
