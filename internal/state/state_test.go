package state

import (
	"testing"
	"time"
)

func TestUpdate_ImmediateAlert(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// duration_minutes=0 means alert immediately
	action := tracker.Update("moisture", true, 0, 0, now, true)
	if action != ActionAlert {
		t.Errorf("expected ActionAlert, got %v", action)
	}
}

func TestUpdate_DelayedAlert(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// Condition becomes true, but duration hasn't elapsed
	action := tracker.Update("garage_door", true, 30, 0, now, false)
	if action != ActionNone {
		t.Errorf("expected ActionNone before threshold, got %v", action)
	}

	// 29 minutes later — still no alert
	action = tracker.Update("garage_door", true, 30, 0, now.Add(29*time.Minute), false)
	if action != ActionNone {
		t.Errorf("expected ActionNone at 29min, got %v", action)
	}

	// 30 minutes later — alert fires
	action = tracker.Update("garage_door", true, 30, 0, now.Add(30*time.Minute), false)
	if action != ActionAlert {
		t.Errorf("expected ActionAlert at 30min, got %v", action)
	}
}

func TestUpdate_Resolve(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// Alert fires
	tracker.Update("garage_door", true, 0, 0, now, false)

	// Condition resolves
	action := tracker.Update("garage_door", false, 0, 0, now.Add(time.Minute), true)
	if action != ActionResolve {
		t.Errorf("expected ActionResolve, got %v", action)
	}

	// State should be reset
	s, _ := tracker.GetState("garage_door")
	if s.ConditionMet || s.Alerted {
		t.Error("state should be reset after resolve")
	}
}

func TestUpdate_SilentReset(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// Condition true but hasn't reached threshold
	tracker.Update("garage_door", true, 30, 0, now, false)

	// Condition becomes false before threshold — silent reset, no resolve
	action := tracker.Update("garage_door", false, 30, 0, now.Add(5*time.Minute), true)
	if action != ActionNone {
		t.Errorf("expected ActionNone for silent reset, got %v", action)
	}
}

func TestUpdate_Repeat(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// Initial alert
	tracker.Update("garage_door", true, 0, 60, now, false)

	// 30 min later — no repeat yet
	action := tracker.Update("garage_door", true, 0, 60, now.Add(30*time.Minute), false)
	if action != ActionNone {
		t.Errorf("expected ActionNone before repeat interval, got %v", action)
	}

	// 60 min later — repeat fires
	action = tracker.Update("garage_door", true, 0, 60, now.Add(60*time.Minute), false)
	if action != ActionRepeat {
		t.Errorf("expected ActionRepeat at 60min, got %v", action)
	}

	// Another 60 min — another repeat
	action = tracker.Update("garage_door", true, 0, 60, now.Add(120*time.Minute), false)
	if action != ActionRepeat {
		t.Errorf("expected ActionRepeat at 120min, got %v", action)
	}
}

func TestUpdate_NoRepeatWhenZero(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// Alert fires, repeat_minutes=0
	tracker.Update("moisture", true, 0, 0, now, true)

	// Much later — no repeat
	action := tracker.Update("moisture", true, 0, 0, now.Add(24*time.Hour), true)
	if action != ActionNone {
		t.Errorf("expected ActionNone with repeat_minutes=0, got %v", action)
	}
}

func TestCheckThresholds(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// Set up an active condition
	tracker.Update("garage_door", true, 30, 0, now, false)

	rules := map[string]struct{ DurationMin, RepeatMin int }{
		"garage_door": {30, 0},
	}

	// Before threshold — no actions
	actions := tracker.CheckThresholds(rules, now.Add(29*time.Minute))
	if len(actions) != 0 {
		t.Errorf("expected no actions before threshold, got %v", actions)
	}

	// At threshold — alert
	actions = tracker.CheckThresholds(rules, now.Add(30*time.Minute))
	if actions["garage_door"] != ActionAlert {
		t.Errorf("expected ActionAlert at threshold, got %v", actions["garage_door"])
	}
}

func TestCheckThresholds_Repeat(t *testing.T) {
	tracker := NewTracker()
	now := time.Now()

	// Fire initial alert
	tracker.Update("garage_door", true, 0, 60, now, false)

	rules := map[string]struct{ DurationMin, RepeatMin int }{
		"garage_door": {0, 60},
	}

	// At repeat interval
	actions := tracker.CheckThresholds(rules, now.Add(60*time.Minute))
	if actions["garage_door"] != ActionRepeat {
		t.Errorf("expected ActionRepeat, got %v", actions["garage_door"])
	}
}

func TestRemoveRule(t *testing.T) {
	tracker := NewTracker()
	tracker.Update("old_rule", true, 0, 0, time.Now(), true)

	tracker.RemoveRule("old_rule")
	_, exists := tracker.GetState("old_rule")
	if exists {
		t.Error("expected rule to be removed")
	}
}
