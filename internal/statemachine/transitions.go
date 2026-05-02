package statemachine

import "fmt"

// validTransitions defines the allowed state transitions for MVP.
// Any transition not in this map is rejected at the store layer.
// Phase 1.5 will extend this with RETRYING, DEAD, CANCELLED, TIMEOUT.
var validTransitions = map[string][]string{
	"PENDING": {"RUNNING"},
	"RUNNING": {"SUCCESS", "FAILED"},
}

// IsValidTransition checks whether moving from `from` to `to` is allowed.
func IsValidTransition(from, to string) bool {
	allowed, ok := validTransitions[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

// ValidateTransition returns an error if the transition is not allowed.
// Use this at the store layer before executing any UPDATE.
func ValidateTransition(from, to string) error {
	if !IsValidTransition(from, to) {
		return fmt.Errorf("invalid state transition: %s → %s", from, to)
	}
	return nil
}
