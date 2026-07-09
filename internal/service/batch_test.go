package service

import (
	"errors"
	"testing"

	"github.com/max-trifonov/letopis/internal/diff"
)

func stateItem(entity string, amount float64) BatchItem {
	return BatchItem{Kind: KindState, Command: IngestCommand{Collection: "crm.deals", EntityID: entity, State: deal(amount)}}
}

// A batch of distinct entities lands in one go, each at version 1.
func TestIngestBatchDistinctEntities(t *testing.T) {
	fx := newIngestFixture(t)
	items := []BatchItem{stateItem("d-1", 100), stateItem("d-2", 200), stateItem("d-3", 300)}
	errs := fx.ing.IngestBatch(authedCtx(), items)
	for i, e := range errs {
		if e != nil {
			t.Fatalf("item %d failed: %v", i, e)
		}
	}
	for _, eid := range []string{"d-1", "d-2", "d-3"} {
		evs := fx.events.byEntity["crm.deals|"+eid]
		if len(evs) != 1 || evs[0].Version != 1 {
			t.Fatalf("%s events = %+v", eid, evs)
		}
		cur, _ := fx.current.Get(authedCtx(), "crm.deals", eid)
		if cur == nil || cur.Version != 1 {
			t.Fatalf("%s current = %+v", eid, cur)
		}
	}
}

// Several writes to the same entity in one batch must sequence versions in
// arrival order (FR-1.10) and leave the current state at the last write.
func TestIngestBatchSameEntitySequencesVersions(t *testing.T) {
	fx := newIngestFixture(t)
	items := []BatchItem{stateItem("d-1", 100), stateItem("d-1", 200), stateItem("d-1", 300)}
	errs := fx.ing.IngestBatch(authedCtx(), items)
	for i, e := range errs {
		if e != nil {
			t.Fatalf("item %d: %v", i, e)
		}
	}
	evs := fx.events.byEntity["crm.deals|d-1"]
	if len(evs) != 3 {
		t.Fatalf("events = %d, want 3", len(evs))
	}
	for i, ev := range evs {
		if ev.Version != int64(i+1) {
			t.Fatalf("event %d version = %d", i, ev.Version)
		}
	}
	cur, _ := fx.current.Get(authedCtx(), "crm.deals", "d-1")
	if cur.Version != 3 || cur.State["amount"] != float64(300) {
		t.Fatalf("final current = %+v", cur)
	}
}

// An identical re-state inside the batch is a no-op: no second event, and its
// item is reported as success (nil error) so its ticket settles as stored.
func TestIngestBatchNoOp(t *testing.T) {
	fx := newIngestFixture(t)
	errs := fx.ing.IngestBatch(authedCtx(), []BatchItem{stateItem("d-1", 100), stateItem("d-1", 100)})
	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("errs = %v", errs)
	}
	if got := len(fx.events.byEntity["crm.deals|d-1"]); got != 1 {
		t.Fatalf("no-op wrote a duplicate: %d events", got)
	}
}

// A bad diff taints only its own item; the rest of the batch still lands.
func TestIngestBatchPartialFailureIsolated(t *testing.T) {
	fx := newIngestFixture(t)
	bad := BatchItem{Kind: KindDiff, Command: IngestCommand{
		Collection: "crm.deals", EntityID: "d-2",
		Changes: []diff.Change{{Path: "missing", Op: diff.OpRemove, Old: "x"}},
	}}
	items := []BatchItem{stateItem("d-1", 100), bad, stateItem("d-3", 300)}
	errs := fx.ing.IngestBatch(authedCtx(), items)
	if errs[0] != nil || errs[2] != nil {
		t.Fatalf("good items failed: %v", errs)
	}
	if !errors.Is(errs[1], ErrInvalidDiff) {
		t.Fatalf("bad item err = %v, want ErrInvalidDiff", errs[1])
	}
	if len(fx.events.byEntity["crm.deals|d-1"]) != 1 || len(fx.events.byEntity["crm.deals|d-3"]) != 1 {
		t.Fatal("neighbours of a bad item were dropped")
	}
	if len(fx.events.byEntity["crm.deals|d-2"]) != 0 {
		t.Fatal("bad item was written")
	}
}

// Entities spread across two collections each get their own insertMany.
func TestIngestBatchMultipleCollections(t *testing.T) {
	fx := newIngestFixture(t)
	other := BatchItem{Kind: KindState, Command: IngestCommand{Collection: "crm.tasks", EntityID: "t-1", State: deal(5)}}
	errs := fx.ing.IngestBatch(authedCtx(), []BatchItem{stateItem("d-1", 100), other})
	for _, e := range errs {
		if e != nil {
			t.Fatalf("err: %v", e)
		}
	}
	if len(fx.events.byEntity["crm.deals|d-1"]) != 1 || len(fx.events.byEntity["crm.tasks|t-1"]) != 1 {
		t.Fatalf("cross-collection batch missed a write: %+v", fx.events.byEntity)
	}
}
