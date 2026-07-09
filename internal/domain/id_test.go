package domain

import (
	"strings"
	"testing"
)

func TestNewActivityIDShape(t *testing.T) {
	id := NewActivityID()
	if !strings.HasPrefix(id, "act_") {
		t.Fatalf("activity id %q missing act_ prefix", id)
	}
	if got := len(strings.TrimPrefix(id, "act_")); got != 26 {
		t.Fatalf("ulid length = %d, want 26", got)
	}
}

func TestNewFlowIDShape(t *testing.T) {
	id := NewFlowID()
	if !strings.HasPrefix(id, "f_") {
		t.Fatalf("flow id %q missing f_ prefix", id)
	}
	if got := len(strings.TrimPrefix(id, "f_")); got != 26 {
		t.Fatalf("ulid length = %d, want 26", got)
	}
}

func TestULIDUsesCrockfordAlphabet(t *testing.T) {
	id := strings.TrimPrefix(NewActivityID(), "act_")
	for _, c := range id {
		if !strings.ContainsRune(crockford, c) {
			t.Fatalf("ulid %q has non-Crockford char %q", id, c)
		}
	}
}

// Ids must be unique across a tight loop; the 80 random bits make a collision
// astronomically unlikely, so any duplicate signals a broken generator.
func TestULIDUnique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for range 1000 {
		id := NewActivityID()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = struct{}{}
	}
}
