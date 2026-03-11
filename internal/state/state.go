package state

import (
	"sync"
	"time"
)

// Action represents what the engine should do in response to a state change.
type Action int

const (
	ActionNone    Action = iota // No alert action needed
	ActionAlert                // Fire new alert
	ActionRepeat               // Fire repeat alert
	ActionResolve              // Fire resolve notification
)

// RuleState tracks the alert state for a single rule.
type RuleState struct {
	ConditionMet   bool
	ConditionSince time.Time
	Alerted        bool
	LastAlertAt    time.Time
	LastValue      any
}

// Tracker manages state for all rules. Safe for concurrent use.
type Tracker struct {
	mu     sync.Mutex
	states map[string]*RuleState
}

// NewTracker creates a new state tracker.
func NewTracker() *Tracker {
	return &Tracker{
		states: make(map[string]*RuleState),
	}
}

// Update processes a new evaluation result for a rule and returns the action
// the engine should take. durationMin is the rule's duration_minutes threshold.
// repeatMin is the rule's repeat_minutes interval (0 means no repeat).
func (t *Tracker) Update(ruleName string, conditionMet bool, durationMin int, repeatMin int, now time.Time, value any) Action {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.getOrCreate(ruleName)
	s.LastValue = value

	if conditionMet {
		return t.handleConditionTrue(s, durationMin, repeatMin, now)
	}
	return t.handleConditionFalse(s)
}

// CheckThresholds examines all active rule states and returns a map of
// rule names to actions for any that have crossed a threshold since the
// last check. Called by the sweep ticker.
func (t *Tracker) CheckThresholds(rules map[string]struct{ DurationMin, RepeatMin int }, now time.Time) map[string]Action {
	t.mu.Lock()
	defer t.mu.Unlock()

	actions := make(map[string]Action)
	for name, params := range rules {
		s, exists := t.states[name]
		if !exists || !s.ConditionMet {
			continue
		}
		action := t.checkState(s, params.DurationMin, params.RepeatMin, now)
		if action != ActionNone {
			actions[name] = action
		}
	}
	return actions
}

// GetState returns a copy of the state for a rule (for testing/debugging).
func (t *Tracker) GetState(ruleName string) (RuleState, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s, exists := t.states[ruleName]
	if !exists {
		return RuleState{}, false
	}
	return *s, true
}

// RemoveRule removes state for a rule that no longer exists in config.
func (t *Tracker) RemoveRule(ruleName string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.states, ruleName)
}

// RuleNames returns all tracked rule names.
func (t *Tracker) RuleNames() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	names := make([]string, 0, len(t.states))
	for name := range t.states {
		names = append(names, name)
	}
	return names
}

func (t *Tracker) getOrCreate(ruleName string) *RuleState {
	s, exists := t.states[ruleName]
	if !exists {
		s = &RuleState{}
		t.states[ruleName] = s
	}
	return s
}

func (t *Tracker) handleConditionTrue(s *RuleState, durationMin, repeatMin int, now time.Time) Action {
	if !s.ConditionMet {
		// Condition just became true
		s.ConditionMet = true
		s.ConditionSince = now
	}

	return t.checkState(s, durationMin, repeatMin, now)
}

func (t *Tracker) handleConditionFalse(s *RuleState) Action {
	if !s.ConditionMet {
		return ActionNone
	}

	wasAlerted := s.Alerted
	// Reset state
	s.ConditionMet = false
	s.ConditionSince = time.Time{}
	s.Alerted = false
	s.LastAlertAt = time.Time{}

	if wasAlerted {
		return ActionResolve
	}
	return ActionNone
}

// checkState evaluates whether a threshold has been crossed for an active condition.
func (t *Tracker) checkState(s *RuleState, durationMin, repeatMin int, now time.Time) Action {
	elapsed := now.Sub(s.ConditionSince)
	threshold := time.Duration(durationMin) * time.Minute

	if !s.Alerted {
		// Haven't alerted yet — check if duration threshold is met
		if elapsed >= threshold {
			s.Alerted = true
			s.LastAlertAt = now
			return ActionAlert
		}
		return ActionNone
	}

	// Already alerted — check for repeat
	if repeatMin > 0 {
		repeatInterval := time.Duration(repeatMin) * time.Minute
		if now.Sub(s.LastAlertAt) >= repeatInterval {
			s.LastAlertAt = now
			return ActionRepeat
		}
	}

	return ActionNone
}
