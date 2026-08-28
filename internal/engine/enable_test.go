package engine

import "testing"

func TestParseEnable(t *testing.T) {
	cases := []struct {
		payload string
		want    bool
		ok      bool
	}{
		// Bare strings — the original convention (Homebridge, mosquitto_pub).
		{"on", true, true},
		{"ON", true, true},
		{"true", true, true},
		{"1", true, true},
		{"enabled", true, true},
		{" off ", false, true},
		{"false", false, true},
		{"0", false, true},
		{"disable", false, true},
		// JSON object form — for JSON-only publishers (dashboard mqtt_publish).
		{`{"enable": true}`, true, true},
		{`{"enable": false}`, false, true},
		{`{"enable": "on"}`, true, true},
		{`{"enable": "OFF"}`, false, true},
		// Rejected: no guessing.
		{"", false, false},
		{"{}", false, false},
		{`{"enable": 5}`, false, false},
		{`{"state": "ON"}`, false, false},
		{`{"enable": "maybe"}`, false, false},
		{"not-a-word", false, false},
		{"{malformed", false, false},
	}
	for _, c := range cases {
		got, ok := parseEnable([]byte(c.payload))
		if got != c.want || ok != c.ok {
			t.Errorf("parseEnable(%q) = (%v,%v), want (%v,%v)", c.payload, got, ok, c.want, c.ok)
		}
	}
}
