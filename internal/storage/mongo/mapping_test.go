package mongo

import (
	"reflect"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

// roundTripEvent marshals a domain event to its BSON shape and back through a
// real BSON encode/decode, so the test also catches tag/marshal mistakes, not
// just the struct mapping.
func roundTripEvent(t *testing.T, ev *domain.Event) *domain.Event {
	t.Helper()
	raw, err := bson.Marshal(toEventDoc(ev))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var d eventDoc
	if err := bson.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got, err := fromEventDoc(d)
	if err != nil {
		t.Fatalf("fromEventDoc: %v", err)
	}
	return got
}

func TestEventRoundTrip(t *testing.T) {
	ts := time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)
	ev := &domain.Event{
		EntityID:   "d-1",
		Version:    17,
		Op:         domain.OpUpdate,
		Author:     "42",
		Source:     "crm-prod",
		TSSource:   ts,
		TSReceived: ts.Add(time.Second),
		TSStored:   ts.Add(2 * time.Second),
		EventID:    "src-evt-981",
		RequestID:  "req-1",
		Changes: []diff.Change{
			{Path: "amount", Op: diff.OpChange, Old: float64(100), New: float64(250)},
			{Path: "tags.2", Op: diff.OpAdd, New: "vip"},
			{Path: "note", Op: diff.OpRemove, Old: "old"},
		},
		Meta:      map[string]any{"ip": "10.1.2.3"},
		Flow:      &domain.Flow{ID: "f_1", Step: "approved", CausedBy: []domain.FlowRef{{Collection: "crm.deals", EntityID: "d-2", Version: 3}}},
		Integrity: &domain.Integrity{Hash: "sha256:aaa", PrevHash: "sha256:bbb"},
	}

	got := roundTripEvent(t, ev)
	if !reflect.DeepEqual(got, ev) {
		t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, ev)
	}
}

func TestEventRoundTripMinimal(t *testing.T) {
	ev := &domain.Event{EntityID: "d-1", Version: 1, Op: domain.OpCreate, Changes: []diff.Change{}}
	got := roundTripEvent(t, ev)
	if got.Flow != nil || got.Integrity != nil {
		t.Fatal("optional blocks must stay nil when absent")
	}
	if got.EntityID != "d-1" || got.Version != 1 || got.Op != domain.OpCreate {
		t.Fatalf("core fields lost: %+v", got)
	}
}

func TestUnknownChangeCode(t *testing.T) {
	_, err := fromEventDoc(eventDoc{Changes: []changeDoc{{P: "x", O: "z"}}})
	if err == nil {
		t.Fatal("expected error on unknown op code")
	}
}

func TestActivityRoundTrip(t *testing.T) {
	ts := time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)
	a := &domain.Activity{
		ActivityID: "act_01J",
		Type:       "recalc.prices",
		FlowID:     "f_01J",
		// All three ref shapes must survive: activity, event-by-version,
		// event-by-event-id (FR-8.1).
		CausedBy: []domain.FlowRef{
			{ActivityID: "act_prev"},
			{Collection: "crm.deals", EntityID: "d-1", Version: 17},
			{Collection: "crm.deals", EntityID: "d-1", EventID: "src-981"},
		},
		Refs:       []domain.FlowRef{{Collection: "crm.prices", EntityID: "p-44"}},
		Author:     "42",
		Source:     "billing-svc",
		TSSource:   ts,
		TSReceived: ts.Add(time.Second),
		TSStored:   ts.Add(2 * time.Second),
		Data:       map[string]any{"recalced": float64(17)},
		Meta:       map[string]any{"ip": "10.1.2.3"},
	}

	raw, err := bson.Marshal(toActivityDoc(a))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var d activityDoc
	if err := bson.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := fromActivityDoc(d)
	if !reflect.DeepEqual(&got, a) {
		t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, a)
	}
}

func TestCurrentRoundTrip(t *testing.T) {
	st := &domain.CurrentState{
		EntityID: "d-1",
		Version:  5,
		TS:       time.Date(2026, 6, 13, 0, 0, 0, 0, time.UTC),
		Deleted:  true,
		State:    map[string]any{"amount": float64(250)},
	}
	raw, err := bson.Marshal(toCurrentDoc(st))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var d currentDoc
	if err := bson.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got := fromCurrentDoc(d); !reflect.DeepEqual(got, st) {
		t.Fatalf("round-trip mismatch:\n got=%+v\nwant=%+v", got, st)
	}
}

// TestCurrentStateDecodesToNeutralJSON guards the contract the diff engine
// relies on: state read back from Mongo must be plain map[string]any / []any,
// not the driver's bson.D / bson.A. Before the canonicalization fix a
// client-supplied diff touching a nested object or array failed to apply
// against a stored entity (durable/fast /diff), because the type assertions in
// diff.Apply reject the named BSON container types.
func TestCurrentStateDecodesToNeutralJSON(t *testing.T) {
	st := &domain.CurrentState{
		EntityID: "d-1",
		Version:  1,
		State: map[string]any{
			"status":   "processing",
			"customer": map[string]any{"tier": "gold"},
			"items":    []any{map[string]any{"sku": "A-1", "qty": float64(2)}},
		},
	}
	raw, err := bson.Marshal(toCurrentDoc(st))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var d currentDoc
	if err := bson.Unmarshal(raw, &d); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	state := fromCurrentDoc(d).State

	if _, ok := state["customer"].(map[string]any); !ok {
		t.Fatalf("nested object decoded as %T, want map[string]any", state["customer"])
	}
	if _, ok := state["items"].([]any); !ok {
		t.Fatalf("array decoded as %T, want []any", state["items"])
	}
	// The real failure mode: a diff over the nested object and array must apply.
	changes := []diff.Change{
		{Path: "customer.tier", Op: diff.OpChange, Old: "gold", New: "platinum"},
		{Path: "items.0.qty", Op: diff.OpChange, Old: float64(2), New: float64(5)},
	}
	if _, err := diff.Apply(state, changes); err != nil {
		t.Fatalf("diff over Mongo-decoded state must apply, got: %v", err)
	}
}
