package actuator

import (
	"testing"
	"time"
)

const (
	onTopic  = "zigbee2mqtt/light/set"
	onPayl   = `{"state":"ON"}`
	offPayl  = `{"state":"OFF"}`
	ttlMin   = 30
	offDelay = 120
)

func eval(t *Tracker, met bool, now time.Time, delay int) []Command {
	return t.Evaluate("r", met, onTopic, onPayl, onTopic, offPayl, delay, ttlMin, now)
}

func TestRisingEdgeCommandsOn(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	cmds := eval(tr, true, now, 0)
	if len(cmds) != 1 || cmds[0].Payload != onPayl {
		t.Fatalf("expected on-command, got %+v", cmds)
	}
}

func TestNoCommandWithoutEdge(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, 0)
	if cmds := eval(tr, true, now.Add(time.Second), 0); cmds != nil {
		t.Fatalf("repeat true should not re-command, got %+v", cmds)
	}
}

func TestFallingEdgeImmediateOff(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, 0)
	cmds := eval(tr, false, now.Add(time.Second), 0)
	if len(cmds) != 1 || cmds[0].Payload != offPayl {
		t.Fatalf("expected off-command, got %+v", cmds)
	}
}

func TestFallingEdgeDefersOffByDelay(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, offDelay)
	if cmds := eval(tr, false, now.Add(time.Second), offDelay); cmds != nil {
		t.Fatalf("off should be deferred, got %+v", cmds)
	}

	specs := map[string]SweepSpec{"r": {OffTopic: onTopic, OffPayload: offPayl, TTLMinutes: ttlMin}}

	if due := tr.Sweep(specs, now.Add(30*time.Second)); len(due) != 0 {
		t.Fatalf("off fired early, got %+v", due)
	}
	due := tr.Sweep(specs, now.Add(time.Duration(offDelay+5)*time.Second))
	if len(due) != 1 || due[0].Command.Payload != offPayl {
		t.Fatalf("expected deferred off, got %+v", due)
	}
	// Must not fire twice.
	if again := tr.Sweep(specs, now.Add(10*time.Minute)); len(again) != 0 {
		t.Fatalf("off fired twice, got %+v", again)
	}
}

func TestMotionDuringOffDelayCancelsIt(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, offDelay)
	eval(tr, false, now.Add(time.Second), offDelay)
	// New motion before the delay elapses.
	cmds := eval(tr, true, now.Add(10*time.Second), offDelay)
	if len(cmds) != 1 || cmds[0].Payload != onPayl {
		t.Fatalf("expected re-trigger on-command, got %+v", cmds)
	}

	specs := map[string]SweepSpec{"r": {OffTopic: onTopic, OffPayload: offPayl, TTLMinutes: ttlMin}}
	if due := tr.Sweep(specs, now.Add(time.Duration(offDelay+5)*time.Second)); len(due) != 0 {
		t.Fatalf("stale off should have been cancelled, got %+v", due)
	}
}

func TestOverrideSuppressesAutomation(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.NoteOverride("r", now)
	if got := tr.Owner("r", ttlMin, now); got != OwnerOverride {
		t.Fatalf("owner = %v, want override", got)
	}
	if cmds := eval(tr, true, now.Add(time.Second), 0); cmds != nil {
		t.Fatalf("automation must stay quiet under override, got %+v", cmds)
	}
}

func TestOverrideExpiresAfterTTL(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.NoteOverride("r", now)
	later := now.Add(time.Duration(ttlMin)*time.Minute + time.Second)
	if got := tr.Owner("r", ttlMin, later); got != OwnerAutomation {
		t.Fatalf("owner = %v, want automation after TTL", got)
	}
	// Condition was never seen as true, so this is a rising edge.
	if cmds := eval(tr, true, later, 0); len(cmds) != 1 {
		t.Fatalf("automation should resume after TTL, got %+v", cmds)
	}
}

func TestEnableSwitchClearsOverrideImmediately(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.NoteOverride("r", now)
	tr.SetEnabled("r", true, 0, now.Add(time.Minute))
	if got := tr.Owner("r", ttlMin, now.Add(time.Minute)); got != OwnerAutomation {
		t.Fatalf("owner = %v, want automation after enable", got)
	}
}

func TestParkedBeatsOverride(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.NoteOverride("r", now)
	tr.SetEnabled("r", false, 0, now)
	if got := tr.Owner("r", ttlMin, now); got != OwnerParked {
		t.Fatalf("owner = %v, want parked", got)
	}
	if cmds := eval(tr, true, now.Add(time.Second), 0); cmds != nil {
		t.Fatalf("parked automation must be silent, got %+v", cmds)
	}
}

func TestOverrideDuringOffDelayCancelsPendingOff(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, offDelay)
	eval(tr, false, now.Add(time.Second), offDelay)
	tr.NoteOverride("r", now.Add(2*time.Second))

	specs := map[string]SweepSpec{"r": {OffTopic: onTopic, OffPayload: offPayl, TTLMinutes: ttlMin}}
	if due := tr.Sweep(specs, now.Add(time.Duration(offDelay+5)*time.Second)); len(due) != 0 {
		t.Fatalf("manual override must cancel pending off, got %+v", due)
	}
}

func TestSweepSkipsWhenOwnershipChanged(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, offDelay)
	eval(tr, false, now.Add(time.Second), offDelay)
	tr.SetEnabled("r", false, 0, now.Add(2*time.Second)) // park it

	specs := map[string]SweepSpec{"r": {OffTopic: onTopic, OffPayload: offPayl, TTLMinutes: ttlMin}}
	if due := tr.Sweep(specs, now.Add(time.Duration(offDelay+5)*time.Second)); len(due) != 0 {
		t.Fatalf("parked rule must not fire deferred off, got %+v", due)
	}
}

func TestNoOffWithoutPriorOn(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	// First observation is false — no on-command was ever issued.
	if cmds := eval(tr, false, now, 0); cmds != nil {
		t.Fatalf("must not send off without prior on, got %+v", cmds)
	}
}

func TestRemoveRuleDropsState(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.NoteOverride("r", now)
	tr.RemoveRule("r")
	if got := tr.Owner("r", ttlMin, now); got != OwnerAutomation {
		t.Fatalf("owner = %v, want automation after removal", got)
	}
}

// The condition can fall while a rule is overridden, leaving no edge to act on
// once control returns. Resuming must reconcile with the current condition
// instead of waiting for the next motion cycle -- otherwise the light stays on
// indefinitely, which is what "toggled Auto back on and nothing happened"
// looked like in the field on 2026-08-23.
func TestResumeAfterOverrideSchedulesOffWhenConditionAlreadyFell(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	// Motion turns the light on.
	if cmds := eval(tr, true, now, offDelay); len(cmds) != 1 {
		t.Fatalf("expected on-command, got %+v", cmds)
	}

	// A manual command takes control.
	if !tr.NoteOverride("r", now.Add(time.Second)) {
		t.Fatal("expected manual command to engage the override")
	}

	// Occupancy falls while overridden: tracked, but no command and no timer.
	if cmds := eval(tr, false, now.Add(2*time.Second), offDelay); cmds != nil {
		t.Fatalf("expected no command while overridden, got %+v", cmds)
	}

	// Hand control back.
	resume := now.Add(3 * time.Second)
	tr.SetEnabled("r", true, offDelay, resume)

	if got := tr.Owner("r", ttlMin, resume); got != OwnerAutomation {
		t.Fatalf("expected automation to own the rule, got %v", got)
	}

	specs := map[string]SweepSpec{"r": {OffTopic: onTopic, OffPayload: offPayl, TTLMinutes: ttlMin}}

	// Not yet due.
	if due := tr.Sweep(specs, resume.Add(time.Duration(offDelay-5)*time.Second)); len(due) != 0 {
		t.Fatalf("off fired early: %+v", due)
	}

	// Due after the configured delay, measured from the resume.
	due := tr.Sweep(specs, resume.Add(time.Duration(offDelay+5)*time.Second))
	if len(due) != 1 || due[0].Command.Payload != offPayl {
		t.Fatalf("expected the deferred off to fire after resume, got %+v", due)
	}
}

// Resuming a rule whose condition is still true must NOT schedule an off --
// the light should stay on while motion continues.
func TestResumeWithConditionStillTrueDoesNotScheduleOff(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	eval(tr, true, now, offDelay)
	tr.NoteOverride("r", now.Add(time.Second))
	tr.SetEnabled("r", true, offDelay, now.Add(2*time.Second))

	specs := map[string]SweepSpec{"r": {OffTopic: onTopic, OffPayload: offPayl, TTLMinutes: ttlMin}}
	if due := tr.Sweep(specs, now.Add(time.Duration(offDelay+30)*time.Second)); len(due) != 0 {
		t.Fatalf("expected no off while the condition holds, got %+v", due)
	}
}
