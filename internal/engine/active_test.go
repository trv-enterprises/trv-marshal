package engine

import (
	"testing"
	"time"

	"github.com/trv-enterprises/trv-marshal/internal/actuator"
	"github.com/trv-enterprises/trv-marshal/internal/config"
)

const (
	nlState  = "zigbee2mqtt/nl"
	nlSet    = "zigbee2mqtt/nl/set"
	nlEnable = "automation/nl/enable"
	nlActive = "automation/nl/active"
	nlAlerts = "sensors/alerts"

	motion   = `{"occupancy":true}`
	noMotion = `{"occupancy":false}`
	lightOn  = `{"state":"ON"}`
	lightOff = `{"state":"OFF"}`
)

// nightlightEngine builds an engine with one rule shaped like the live
// nightlight rules: notify on the motion itself, and drive the light with a
// deferred off, a manual-override TTL, an enable topic and an active topic.
func nightlightEngine(c *fakeClient, mutate func(*config.Rule)) *Engine {
	rule := config.Rule{
		Name:      "nl",
		Topic:     nlState,
		Condition: config.Condition{Field: "occupancy", Operator: "eq", Value: true},
		Alert:     &config.AlertSpec{Severity: "info", Message: "Motion"},
		Action: &config.ActionSpec{
			Topic:              nlSet,
			Payload:            lightOn,
			OffPayload:         lightOff,
			OffDelaySeconds:    120,
			OverrideTopic:      nlSet,
			OverrideTTLMinutes: 30,
			EnableTopic:        nlEnable,
			StateTopic:         "automation/nl/owner",
			ActiveTopic:        nlActive,
		},
	}
	if mutate != nil {
		mutate(&rule)
	}
	return New(&config.Config{AlertTopic: nlAlerts, Rules: []config.Rule{rule}}, c, "")
}

// sweepAt runs the deferred-off sweep as if it were `at`.
func (e *Engine) sweepAt(at time.Time) {
	byName := make(map[string]config.Rule)
	for _, r := range e.cfg.Rules {
		byName[r.Name] = r
	}
	e.sweepActuations(byName, at)
}

func boolPtr(b bool) *bool { return &b }

// The point of the feature: "tell me about motion here, but leave the light
// alone". The alert path must not notice that the action is inactive.
func TestInactiveActionStillAlertsButDoesNotTurnOn(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, nil)

	e.handleMessage(nlActive, []byte("false"))
	e.handleMessage(nlState, []byte(motion))

	if got := c.sentTo(nlSet); len(got) != 0 {
		t.Fatalf("inactive action published %v to the light, want nothing", got)
	}
	if got := c.sentTo(nlAlerts); len(got) != 1 {
		t.Fatalf("published %d alert(s), want 1 -- inactive must not silence the alert", len(got))
	}
}

// Deactivating is not parking. A light the engine already turned on still gets
// its scheduled off; parking would drop it and leave the light on.
func TestDeactivatingMidCycleLetsTheCycleFinish(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, nil)

	e.handleMessage(nlState, []byte(motion))
	e.handleMessage(nlActive, []byte("false"))
	e.handleMessage(nlState, []byte(noMotion))
	e.sweepAt(time.Now().Add(3 * time.Minute))

	got := c.sentTo(nlSet)
	if len(got) != 2 || got[0] != lightOn || got[1] != lightOff {
		t.Fatalf("light received %v, want [ON OFF] -- the running cycle must finish", got)
	}

	// And the next motion does nothing.
	e.handleMessage(nlState, []byte(motion))
	if got := c.sentTo(nlSet); len(got) != 2 {
		t.Fatalf("inactive action started a new cycle: %v", got)
	}
}

// Motion returning while inactive must not cancel the off that is already
// scheduled -- the same rule the gate follows.
func TestMotionWhileInactiveDoesNotCancelThePendingOff(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, nil)

	e.handleMessage(nlState, []byte(motion))
	e.handleMessage(nlState, []byte(noMotion)) // off scheduled
	e.handleMessage(nlActive, []byte("false"))
	e.handleMessage(nlState, []byte(motion)) // would cancel it if active
	e.sweepAt(time.Now().Add(3 * time.Minute))

	got := c.sentTo(nlSet)
	if len(got) != 2 || got[1] != lightOff {
		t.Fatalf("light received %v, want [ON OFF]", got)
	}
}

// Reactivating keeps a manual override that is still running. This is the
// deliberate difference from the enable topic, where "on" means "give control
// back now".
func TestReactivatingKeepsAManualOverride(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, nil)

	e.handleMessage(nlActive, []byte("false"))
	e.handleMessage(nlSet, []byte(lightOn)) // a person turns the light on
	e.handleMessage(nlActive, []byte("true"))

	if got := e.actuators.Owner("nl", 30, time.Now()); got != actuator.OwnerOverride {
		t.Fatalf("owner after reactivating = %s, want override", got)
	}
	e.handleMessage(nlState, []byte(motion))
	if got := c.sentTo(nlSet); len(got) != 0 {
		t.Fatalf("automation commanded %v during a manual override", got)
	}
}

// The value is retained, so the broker replays it on every subscribe. A replay
// must not disturb anything -- in particular it must not behave like the
// enable topic, where every "on" clears the override.
func TestReplayedActiveValueChangesNothing(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, nil)

	e.handleMessage(nlState, []byte(motion))
	e.handleMessage(nlState, []byte(noMotion)) // off scheduled

	for i := 0; i < 3; i++ {
		e.handleMessage(nlActive, []byte("true"))
	}
	e.sweepAt(time.Now().Add(3 * time.Minute))

	got := c.sentTo(nlSet)
	if len(got) != 2 || got[1] != lightOff {
		t.Fatalf("light received %v, want [ON OFF] -- a replayed value dropped the pending off", got)
	}
}

// The master switch must not be able to reactivate a light that was
// deliberately made inactive.
func TestEnableOnDoesNotReactivate(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, nil)

	e.handleMessage(nlActive, []byte("false"))
	e.handleMessage(nlEnable, []byte("off"))
	e.handleMessage(nlEnable, []byte("on"))
	e.handleMessage(nlState, []byte(motion))

	if got := c.sentTo(nlSet); len(got) != 0 {
		t.Fatalf("enable on reactivated an inactive action: %v", got)
	}
}

// With no retained value the YAML default applies; a retained value overrides
// it; clearing the retained value (an empty payload) hands the decision back.
func TestActiveTopicOverridesTheConfiguredDefaultUntilCleared(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, func(r *config.Rule) { r.Action.Active = boolPtr(false) })

	e.handleMessage(nlState, []byte(motion))
	if got := c.sentTo(nlSet); len(got) != 0 {
		t.Fatalf("active: false in config still commanded %v", got)
	}
	e.handleMessage(nlState, []byte(noMotion))

	e.handleMessage(nlActive, []byte(`{"active": true}`))
	e.handleMessage(nlState, []byte(motion))
	if got := c.sentTo(nlSet); len(got) != 1 || got[0] != lightOn {
		t.Fatalf("retained true did not override the default: %v", got)
	}
	e.handleMessage(nlState, []byte(noMotion))
	e.sweepAt(time.Now().Add(3 * time.Minute))

	e.handleMessage(nlActive, []byte(""))
	e.handleMessage(nlState, []byte(motion))
	if got := c.sentTo(nlSet); len(got) != 2 {
		t.Fatalf("after clearing the retained value the light received %v, want [ON OFF] only", got)
	}
}

// An alert switched off in the config publishes nothing; the action on the
// same rule is unaffected.
func TestInactiveAlertPublishesNothing(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, func(r *config.Rule) { r.Alert.Active = boolPtr(false) })

	e.handleMessage(nlState, []byte(motion))
	e.sweep()

	if got := c.sentTo(nlAlerts); len(got) != 0 {
		t.Fatalf("inactive alert published %v", got)
	}
	if got := c.sentTo(nlSet); len(got) != 1 {
		t.Fatalf("action on the same rule sent %v, want one ON", got)
	}
}

// A payload that cannot be read is ignored, never guessed.
func TestUnreadableActivePayloadIsIgnored(t *testing.T) {
	c := &fakeClient{open: true}
	e := nightlightEngine(c, nil)

	e.handleMessage(nlActive, []byte("false"))
	e.handleMessage(nlActive, []byte("{}"))
	e.handleMessage(nlActive, []byte("maybe"))
	e.handleMessage(nlState, []byte(motion))

	if got := c.sentTo(nlSet); len(got) != 0 {
		t.Fatalf("an unreadable payload reactivated the action: %v", got)
	}
}

func TestParseActive(t *testing.T) {
	cases := []struct {
		payload string
		want    bool
		ok      bool
	}{
		{"true", true, true},
		{"on", true, true},
		{"active", true, true},
		{" Inactive ", false, true},
		{"false", false, true},
		{"0", false, true},
		{`{"active": true}`, true, true},
		{`{"active": "off"}`, false, true},
		{`{"active": "inactive"}`, false, true},
		// The enable key is a different switch; it must not be read as active.
		{`{"enable": false}`, false, false},
		{`{}`, false, false},
		{`{"active": 1}`, false, false},
		{"maybe", false, false},
	}
	for _, tc := range cases {
		got, ok := parseActive([]byte(tc.payload))
		if got != tc.want || ok != tc.ok {
			t.Errorf("parseActive(%q) = (%v, %v), want (%v, %v)", tc.payload, got, ok, tc.want, tc.ok)
		}
	}
}
