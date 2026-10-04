package actuator

import (
	"testing"
	"time"
)

func TestActiveFallsBackToTheDefaultUntilSet(t *testing.T) {
	tr := NewTracker()

	if !tr.Active("r", true) || tr.Active("r", false) {
		t.Fatal("with no runtime value, Active must return the default it is given")
	}
	tr.SetActive("r", false)
	if tr.Active("r", true) {
		t.Fatal("a runtime false must override a default of true")
	}
	tr.ClearActive("r")
	if !tr.Active("r", true) {
		t.Fatal("after ClearActive the default must apply again")
	}
}

// The value is retained and replayed on every subscribe, so setting what is
// already held must report no change.
func TestSetActiveReportsOnlyRealChanges(t *testing.T) {
	tr := NewTracker()

	if !tr.SetActive("r", true) {
		t.Fatal("first value must count as a change")
	}
	if tr.SetActive("r", true) {
		t.Fatal("a replay of the same value must not count as a change")
	}
	if !tr.SetActive("r", false) {
		t.Fatal("a different value must count as a change")
	}
	if !tr.ClearActive("r") || tr.ClearActive("r") {
		t.Fatal("ClearActive must report a change once, then none")
	}
}

// Activation must leave ownership and the off timer alone, in both
// directions: that is what separates it from the enable switch.
func TestSetActiveLeavesOverrideAndPendingOffAlone(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, offDelay)
	eval(tr, false, now.Add(time.Second), offDelay) // off pending
	tr.SetActive("r", false)
	tr.SetActive("r", true)

	due := tr.Sweep(map[string]SweepSpec{"r": {OffTopic: onTopic, OffPayload: offPayl, TTLMinutes: ttlMin}},
		now.Add(time.Duration(offDelay+5)*time.Second))
	if len(due) != 1 {
		t.Fatalf("pending off after toggling active: %d due, want 1", len(due))
	}

	tr.NoteOverride("r", now)
	tr.SetActive("r", false)
	tr.SetActive("r", true)
	if got := tr.Owner("r", ttlMin, now.Add(time.Minute)); got != OwnerOverride {
		t.Fatalf("owner after toggling active = %s, want override", got)
	}
}

// And the reverse: the enable switch must not touch activation.
func TestSetEnabledLeavesActiveAlone(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.SetActive("r", false)
	tr.SetEnabled("r", false, now)
	tr.SetEnabled("r", true, now)

	if tr.Active("r", true) {
		t.Fatal("enable on reactivated an inactive rule")
	}
}
