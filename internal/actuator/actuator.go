// Package actuator implements the edge-triggered actuation path: it decides
// who currently owns a device and publishes commands accordingly.
//
// Ownership is a strict precedence stack, highest first:
//
//  1. parked      — automation explicitly disabled via the enable topic.
//  2. override    — a manual command was seen recently; expires after a TTL.
//  3. automation  — this engine's rules drive the device.
//  4. device-local — whatever the device does on its own when we stay quiet.
//
// The engine never contends with tier 4; it is what remains when the engine
// issues no commands at all. Keeping the device's own motion rule dormant is a
// deployment concern, not something this package enforces.
package actuator

import (
	"sync"
	"time"
)

// Owner identifies which tier currently controls a rule's target device.
type Owner int

const (
	OwnerAutomation Owner = iota
	OwnerOverride
	OwnerParked
)

func (o Owner) String() string {
	switch o {
	case OwnerOverride:
		return "override"
	case OwnerParked:
		return "parked"
	default:
		return "automation"
	}
}

// Command is a single MQTT publish the engine should perform.
type Command struct {
	Topic   string
	Payload string
}

// ruleState is the per-rule actuation state.
type ruleState struct {
	// conditionMet is the last evaluated condition, used for edge detection.
	conditionMet bool
	// commandedOn records whether we have issued the on-command and not yet
	// issued the matching off-command.
	commandedOn bool
	// offDueAt is when the pending off-command should fire; zero when none.
	offDueAt time.Time

	parked     bool
	overrideAt time.Time

	// selfEcho counts commands this engine published on a topic that is also
	// the rule's override topic. Each such publish comes back on our own
	// subscription and must be consumed rather than mistaken for a manual
	// command. Without this the engine overrides itself on its first action
	// and never actuates again.
	selfEcho int
}

// Tracker owns actuation state for all action-bearing rules.
// Safe for concurrent use.
type Tracker struct {
	mu     sync.Mutex
	states map[string]*ruleState
}

// NewTracker creates a Tracker.
func NewTracker() *Tracker {
	return &Tracker{states: make(map[string]*ruleState)}
}

func (t *Tracker) getOrCreate(rule string) *ruleState {
	s, ok := t.states[rule]
	if !ok {
		s = &ruleState{}
		t.states[rule] = s
	}
	return s
}

// ownerLocked resolves the precedence stack. Caller must hold the mutex.
func (s *ruleState) ownerLocked(ttl time.Duration, now time.Time) Owner {
	if s.parked {
		return OwnerParked
	}
	if ttl > 0 && !s.overrideAt.IsZero() && now.Sub(s.overrideAt) < ttl {
		return OwnerOverride
	}
	return OwnerAutomation
}

// Owner reports which tier currently controls the rule.
func (t *Tracker) Owner(rule string, ttlMinutes int, now time.Time) Owner {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.getOrCreate(rule).ownerLocked(time.Duration(ttlMinutes)*time.Minute, now)
}

// Evaluate processes a condition result for a rule and returns the commands to
// publish. Commands are emitted only on condition edges, and only while
// automation owns the device.
//
// Rising edge  → the on-command, and any pending off is cancelled.
// Falling edge → the off-command, deferred by offDelay (fired later by Sweep
// when the delay is non-zero).
func (t *Tracker) Evaluate(rule string, conditionMet bool, onTopic, onPayload, offTopic, offPayload string, offDelaySeconds, ttlMinutes int, now time.Time) []Command {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.getOrCreate(rule)
	was := s.conditionMet
	s.conditionMet = conditionMet

	// Only act on edges.
	if was == conditionMet {
		return nil
	}

	// While overridden or parked, track the condition but issue no commands.
	// The pending off-timer is cleared so automation does not fire a stale
	// off-command the moment control returns.
	if s.ownerLocked(time.Duration(ttlMinutes)*time.Minute, now) != OwnerAutomation {
		s.offDueAt = time.Time{}
		return nil
	}

	if conditionMet {
		s.offDueAt = time.Time{}
		s.commandedOn = true
		return []Command{{Topic: onTopic, Payload: onPayload}}
	}

	// Falling edge. Nothing to turn off if we never turned it on.
	if !s.commandedOn || offPayload == "" {
		return nil
	}
	if offDelaySeconds > 0 {
		s.offDueAt = now.Add(time.Duration(offDelaySeconds) * time.Second)
		return nil
	}
	s.commandedOn = false
	return []Command{{Topic: offTopic, Payload: offPayload}}
}

// PendingOff is a deferred off-command that has come due.
type PendingOff struct {
	Rule    string
	Command Command
}

// Sweep fires any off-commands whose delay has elapsed. specs supplies the
// off topic/payload and override TTL per rule, so the sweep can re-check
// ownership before firing.
type SweepSpec struct {
	OffTopic   string
	OffPayload string
	TTLMinutes int
}

// Sweep returns the off-commands that are now due.
func (t *Tracker) Sweep(specs map[string]SweepSpec, now time.Time) []PendingOff {
	t.mu.Lock()
	defer t.mu.Unlock()

	var due []PendingOff
	for name, s := range t.states {
		if s.offDueAt.IsZero() || now.Before(s.offDueAt) {
			continue
		}
		spec, ok := specs[name]
		if !ok {
			// Rule vanished on reload; drop the pending timer.
			s.offDueAt = time.Time{}
			continue
		}
		// Ownership may have changed while the timer ran.
		if s.ownerLocked(time.Duration(spec.TTLMinutes)*time.Minute, now) != OwnerAutomation {
			s.offDueAt = time.Time{}
			continue
		}
		s.offDueAt = time.Time{}
		s.commandedOn = false
		due = append(due, PendingOff{
			Rule:    name,
			Command: Command{Topic: spec.OffTopic, Payload: spec.OffPayload},
		})
	}
	return due
}

// NoteOverride records a manual command, starting the override TTL.
//
// When the rule's override topic is also its command topic, the engine hears
// its own publishes. Those are consumed here as echoes and ignored; only a
// message we did not send counts as a manual override.
func (t *Tracker) NoteOverride(rule string, now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.getOrCreate(rule)
	if s.selfEcho > 0 {
		s.selfEcho--
		return false
	}
	s.overrideAt = now
	// A manual command supersedes any scheduled automation off-command.
	s.offDueAt = time.Time{}
	s.commandedOn = false
	return true
}

// NoteSelfCommand records that the engine is about to publish a command which
// will echo back on the rule's own override topic. Call once per publish, and
// only when the command topic and override topic coincide.
func (t *Tracker) NoteSelfCommand(rule string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.getOrCreate(rule).selfEcho++
}

// SetEnabled parks or resumes automation for a rule. Resuming also clears any
// active override, so the enable switch is a single "give control back now".
//
// Resuming schedules nothing. The primary use is cutting an override short --
// "I turned the light off on my way out, now let motion have it again" -- and
// the tracker does not know whether the device is on: it sees the rule's
// condition field (occupancy), never the light's state. Any off scheduled here
// would be a guess, and the wrong guess turns the lights off on someone who
// just walked in. Resuming therefore restores control and waits for the next
// motion edge, which is both correct and the safe default.
//
// The pending off is cleared for the same reason: a timer armed before the
// override no longer describes anything real once a human has intervened.
func (t *Tracker) SetEnabled(rule string, enabled bool, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.getOrCreate(rule)
	s.parked = !enabled
	s.selfEcho = 0
	s.offDueAt = time.Time{}
	if enabled {
		s.overrideAt = time.Time{}
		// The device's state is unknown after a manual command, so do not
		// claim an on that a later falling edge would try to undo.
		s.commandedOn = false
	}
}

// RemoveRule drops state for a rule that no longer exists in config.
func (t *Tracker) RemoveRule(rule string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.states, rule)
}

// RuleNames returns all tracked rule names.
func (t *Tracker) RuleNames() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	names := make([]string, 0, len(t.states))
	for n := range t.states {
		names = append(names, n)
	}
	return names
}
