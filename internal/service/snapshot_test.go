package service

import (
	"errors"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

func TestSnapshotBuilderSnapsOnlyOnBoundary(t *testing.T) {
	snaps := newFakeSnapshots()
	b := NewSnapshotBuilder(snaps, nil)
	ts := time.Now().UTC()

	// Off-boundary versions write nothing; the boundary version writes one.
	for v := int64(98); v <= 100; v++ {
		b.Build(authedCtx(), "crm.deals", "d-1", v, ts, map[string]any{"v": v}, false, 100)
	}
	if snaps.saves != 1 {
		t.Fatalf("saves = %d, want 1 (only v100 crosses interval 100)", snaps.saves)
	}
	got, err := snaps.Nearest(authedCtx(), "crm.deals", "d-1", 100)
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if got.Version != 100 || got.State["v"] != int64(100) {
		t.Fatalf("snapshot wrong: %+v", got)
	}
}

func TestSnapshotBuilderPropagatesDeleted(t *testing.T) {
	snaps := newFakeSnapshots()
	b := NewSnapshotBuilder(snaps, nil)
	b.Build(authedCtx(), "crm.deals", "d-1", 100, time.Now().UTC(), map[string]any{"amount": float64(1)}, true, 100)

	got, err := snaps.Nearest(authedCtx(), "crm.deals", "d-1", 100)
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if !got.Deleted {
		t.Fatalf("snapshot at a delete must carry deleted=true: %+v", got)
	}
}

// A failing Save must not panic or surface — snapshots are best-effort (FR-2.3).
func TestSnapshotBuilderSwallowsSaveError(t *testing.T) {
	snaps := newFakeSnapshots()
	snaps.err = errors.New("mongo down")
	b := NewSnapshotBuilder(snaps, nil)
	b.Build(authedCtx(), "crm.deals", "d-1", 100, time.Now().UTC(), map[string]any{"v": int64(100)}, false, 100)
	// Reached here without a panic; nothing landed.
	if _, err := snaps.Nearest(authedCtx(), "crm.deals", "d-1", 100); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("a failed save should leave no snapshot: %v", err)
	}
}

// A nil builder is a no-op: an Ingester without snapshots wired still writes.
func TestSnapshotBuilderNilSafe(t *testing.T) {
	var b *SnapshotBuilder
	b.Build(authedCtx(), "crm.deals", "d-1", 100, time.Now().UTC(), map[string]any{}, false, 100)
}

// The Ingester drives the builder post-store: after interval writes a snapshot
// exists whose state equals cur_* at that version (S3-02 integration of the
// builder into the synchronous path).
func TestIngesterSnapshotsOnInterval(t *testing.T) {
	fx := newIngestFixture(t)
	snaps := newFakeSnapshots()
	fx.ing.snapshots = NewSnapshotBuilder(snaps, nil)
	// A small interval keeps the test fast while exercising the real boundary logic.
	fx.repo.configs["crm.deals"] = domain.WithDefaults(domain.CollectionConfig{Name: "crm.deals", SnapshotInterval: 5})
	ctx := authedCtx()

	for i := 1; i <= 12; i++ {
		if _, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(float64(i))}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	// Boundaries at v5 and v10 → two snapshots.
	if snaps.saves != 2 {
		t.Fatalf("saves = %d, want 2 (v5, v10)", snaps.saves)
	}
	got, err := snaps.Nearest(ctx, "crm.deals", "d-1", 10)
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	cur, _ := fx.current.Get(ctx, "crm.deals", "d-1")
	if got.Version != 10 {
		t.Fatalf("snapshot version = %d, want 10", got.Version)
	}
	// State at v10: amount=10 (the write that produced it).
	if got.State["amount"] != float64(10) {
		t.Fatalf("snapshot state = %+v, want amount=10", got.State)
	}
	_ = cur
}

// The fast batch path snaps every boundary a single insertMany straddles, not
// just the entity's final version (S3-02).
func TestIngestBatchSnapshotsEveryBoundary(t *testing.T) {
	fx := newIngestFixture(t)
	snaps := newFakeSnapshots()
	fx.ing.snapshots = NewSnapshotBuilder(snaps, nil)
	fx.repo.configs["crm.deals"] = domain.WithDefaults(domain.CollectionConfig{Name: "crm.deals", SnapshotInterval: 2})
	ctx := authedCtx()

	var items []BatchItem
	for i := 1; i <= 5; i++ {
		items = append(items, BatchItem{Kind: KindState, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(float64(i))}})
	}
	if errs := fx.ing.IngestBatch(ctx, items); errsAny(errs) {
		t.Fatalf("batch errors: %v", errs)
	}
	// v1..v5 with interval 2 → snapshots at v2 and v4.
	if snaps.saves != 2 {
		t.Fatalf("saves = %d, want 2 (v2, v4 within one batch)", snaps.saves)
	}
	for _, v := range []int64{2, 4} {
		got, err := snaps.Nearest(ctx, "crm.deals", "d-1", v)
		if err != nil || got.Version != v {
			t.Fatalf("missing snapshot at v%d: %+v / %v", v, got, err)
		}
	}
}

func errsAny(errs []error) bool {
	for _, e := range errs {
		if e != nil {
			return true
		}
	}
	return false
}
