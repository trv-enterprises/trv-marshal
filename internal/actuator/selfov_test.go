package actuator

import (
	"testing"
	"time"
)

// When the rule's command topic doubles as its override topic, the engine
// hears its own publishes. Those echoes must not be mistaken for manual
// commands -- otherwise the engine overrides itself on its first action and
// never actuates again.
func TestSelfCommandEchoIsNotAnOverride(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	cmds := tr.Evaluate("r", true, "x/set", "ON", "x/set", "OFF", 0, 30, now)
	if len(cmds) != 1 {
		t.Fatalf("expected on-command, got %+v", cmds)
	}

	// Engine registers the echo, then publishes; the copy arrives back.
	tr.NoteSelfCommand("r")
	if manual := tr.NoteOverride("r", now.Add(time.Millisecond)); manual {
		t.Fatal("own echo reported as a manual override")
	}
	if got := tr.Owner("r", 30, now.Add(time.Second)); got != OwnerAutomation {
		t.Fatalf("engine overrode itself: owner = %v, want automation", got)
	}
}

// A genuine manual command on the same topic must still take ownership.
func TestRealOverrideOnSharedTopicStillWins(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.Evaluate("r", true, "x/set", "ON", "x/set", "OFF", 0, 30, now)
	tr.NoteSelfCommand("r")
	tr.NoteOverride("r", now.Add(time.Millisecond)) // our echo, consumed

	// Now a human presses the light in HomeKit.
	if manual := tr.NoteOverride("r", now.Add(time.Second)); !manual {
		t.Fatal("human command should register as a manual override")
	}
	if got := tr.Owner("r", 30, now.Add(2*time.Second)); got != OwnerOverride {
		t.Fatalf("owner = %v, want override", got)
	}
}

// Echo credits must not leak across a park/resume.
func TestEnableClearsPendingEchoes(t *testing.T) {
	tr := NewTracker()
	now := time.Now()

	tr.NoteSelfCommand("r")
	tr.SetEnabled("r", true, now)

	if manual := tr.NoteOverride("r", now.Add(time.Second)); !manual {
		t.Fatal("stale echo credit swallowed a real override after resume")
	}
}
