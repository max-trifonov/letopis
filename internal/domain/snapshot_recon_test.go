package domain

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/max-trifonov/letopis/internal/diff"
)

func TestShouldSnapshot(t *testing.T) {
	cases := []struct {
		version  int64
		interval int
		want     bool
	}{
		{version: 100, interval: 100, want: true},
		{version: 200, interval: 100, want: true},
		{version: 99, interval: 100, want: false},
		{version: 101, interval: 100, want: false},
		{version: 1, interval: 1, want: true},
		{version: 5, interval: 0, want: false},  // disabled
		{version: 5, interval: -3, want: false}, // disabled
		{version: 0, interval: 100, want: false},
	}
	for _, c := range cases {
		if got := ShouldSnapshot(c.version, c.interval); got != c.want {
			t.Errorf("ShouldSnapshot(%d, %d) = %v, want %v", c.version, c.interval, got, c.want)
		}
	}
}

// changeEvent builds an update event whose diff takes prev → next, the shape the
// store records and reconstruction replays.
func changeEvent(prev, next map[string]any) Event {
	return Event{Op: OpUpdate, Changes: diff.Diff(prev, next, diff.Options{})}
}

func TestReconstructFromGenesis(t *testing.T) {
	v1 := map[string]any{"title": "deal", "amount": float64(100)}
	v2 := map[string]any{"title": "deal", "amount": float64(250)}
	events := []Event{
		{Op: OpCreate, Changes: diff.Diff(map[string]any{}, v1, diff.Options{})},
		changeEvent(v1, v2),
	}

	got, deleted, err := Reconstruct(nil, false, events)
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if deleted {
		t.Fatalf("entity should not be deleted")
	}
	if !reflect.DeepEqual(got, v2) {
		t.Fatalf("state = %+v, want %+v", got, v2)
	}
}

// A snapshot anchor plus the tail must reconstruct the same state a full genesis
// replay does — the core S3-03 invariant that snapshots only speed reads.
func TestReconstructSnapshotEqualsGenesis(t *testing.T) {
	states := []map[string]any{
		{"amount": float64(1)},
		{"amount": float64(2), "tier": "gold"},
		{"amount": float64(2), "tier": "platinum"},
		{"amount": float64(9), "tier": "platinum"},
	}
	var all []Event
	prev := map[string]any{}
	for _, s := range states {
		all = append(all, changeEvent(prev, s))
		prev = s
	}

	// Full replay to the last version.
	full, _, err := Reconstruct(nil, false, all)
	if err != nil {
		t.Fatalf("genesis replay: %v", err)
	}
	// Snapshot at version 2 (states[1]) + the tail.
	snap, _, err := Reconstruct(nil, false, all[:2])
	if err != nil {
		t.Fatalf("build snapshot: %v", err)
	}
	anchored, _, err := Reconstruct(snap, false, all[2:])
	if err != nil {
		t.Fatalf("anchored replay: %v", err)
	}
	if !reflect.DeepEqual(full, anchored) {
		t.Fatalf("anchored %+v != genesis %+v", anchored, full)
	}
}

// A delete in the tail marks the result deleted and keeps the value held just
// before it (the retained last state, matching cur_*).
func TestReconstructDeleteInTail(t *testing.T) {
	v1 := map[string]any{"amount": float64(100)}
	events := []Event{
		{Op: OpCreate, Changes: diff.Diff(map[string]any{}, v1, diff.Options{})},
		{Op: OpDelete}, // delete carries no changes
	}
	got, deleted, err := Reconstruct(nil, false, events)
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if !deleted {
		t.Fatalf("delete in tail must mark deleted")
	}
	if !reflect.DeepEqual(got, v1) {
		t.Fatalf("retained state = %+v, want %+v", got, v1)
	}
}

// Reincarnation: a create after a delete diffs against an empty object, so
// reconstruction must reset to {} at the delete rather than carrying forward the
// retained fields.
func TestReconstructReincarnation(t *testing.T) {
	first := map[string]any{"a": float64(1), "b": float64(2)}
	reborn := map[string]any{"c": float64(3)}
	events := []Event{
		{Op: OpCreate, Changes: diff.Diff(map[string]any{}, first, diff.Options{})},
		{Op: OpDelete},
		{Op: OpCreate, Changes: diff.Diff(map[string]any{}, reborn, diff.Options{})},
	}
	got, deleted, err := Reconstruct(nil, false, events)
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if deleted {
		t.Fatalf("reincarnated entity should be live")
	}
	if !reflect.DeepEqual(got, reborn) {
		t.Fatalf("state = %+v, want %+v (no leaked fields from before delete)", got, reborn)
	}
}

// A snapshot taken at a delete keeps the retained value but its successor diffs
// from {}: anchoring on it must still reconstruct the reincarnation correctly.
func TestReconstructFromDeletedSnapshot(t *testing.T) {
	retained := map[string]any{"a": float64(1)}
	reborn := map[string]any{"c": float64(3)}
	tail := []Event{{Op: OpCreate, Changes: diff.Diff(map[string]any{}, reborn, diff.Options{})}}

	got, deleted, err := Reconstruct(retained, true, tail)
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if deleted {
		t.Fatalf("should be live after reincarnation")
	}
	if !reflect.DeepEqual(got, reborn) {
		t.Fatalf("state = %+v, want %+v", got, reborn)
	}
}

// Property: for a random walk of states, reconstructing from any intermediate
// snapshot version equals the full genesis replay to the same target. This is the
// apply(s, diff(s, s')) == s' invariant (S1-01) lifted to the read path.
func TestReconstructSnapshotInvariantProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	for iter := range 200 {
		n := 2 + rng.Intn(8)
		var events []Event
		prev := map[string]any{}
		for range n {
			next := randomState(rng)
			events = append(events, changeEvent(prev, next))
			prev = next
		}
		target := 1 + rng.Intn(n) // 1..n versions applied
		full, _, err := Reconstruct(nil, false, events[:target])
		if err != nil {
			t.Fatalf("iter %d genesis: %v", iter, err)
		}
		split := rng.Intn(target) // snapshot after `split` events
		snap, _, err := Reconstruct(nil, false, events[:split])
		if err != nil {
			t.Fatalf("iter %d snapshot: %v", iter, err)
		}
		anchored, _, err := Reconstruct(snap, false, events[split:target])
		if err != nil {
			t.Fatalf("iter %d anchored: %v", iter, err)
		}
		if !reflect.DeepEqual(full, anchored) {
			t.Fatalf("iter %d: anchored %+v != genesis %+v (target=%d split=%d)", iter, anchored, full, target, split)
		}
	}
}

func randomState(rng *rand.Rand) map[string]any {
	keys := []string{"amount", "title", "tier", "qty", "owner"}
	s := map[string]any{}
	for _, k := range keys {
		if rng.Intn(3) == 0 {
			continue // omit some keys so diffs include adds/removes
		}
		switch rng.Intn(3) {
		case 0:
			s[k] = float64(rng.Intn(100))
		case 1:
			s[k] = []any{float64(rng.Intn(10)), float64(rng.Intn(10))}
		default:
			s[k] = map[string]any{"n": float64(rng.Intn(10))}
		}
	}
	return s
}
