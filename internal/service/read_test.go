package service

import (
	"errors"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

func TestReaderHistoryDelegates(t *testing.T) {
	events := newFakeEvents()
	ctx := authedCtx()
	_ = events.AppendEvent(ctx, "crm.deals", &domain.Event{EntityID: "d-1", Op: domain.OpCreate})
	_ = events.AppendEvent(ctx, "crm.deals", &domain.Event{EntityID: "d-1", Op: domain.OpUpdate})

	rd := NewReader(events, newFakeCurrent(), newFakeSnapshots())
	got, err := rd.History(ctx, "crm.deals", domain.EventFilter{EntityID: "d-1"})
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d events, want 2", len(got))
	}
}

func TestReaderCurrentStateNotFound(t *testing.T) {
	rd := NewReader(newFakeEvents(), newFakeCurrent(), newFakeSnapshots())
	if _, err := rd.CurrentState(authedCtx(), "crm.deals", "ghost"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// stateAtFixture seeds a history through a real Ingester (so events, cur_* and
// snapshots are mutually consistent) and returns a Reader over the same stores.
type stateAtFixture struct {
	rd      *Reader
	ing     *Ingester
	events  *fakeEvents
	current *fakeCurrent
	snaps   *fakeSnapshots
}

func newStateAtFixture(t *testing.T, interval int) stateAtFixture {
	t.Helper()
	repo := newFakeRepo()
	repo.configs["crm.deals"] = domain.WithDefaults(domain.CollectionConfig{Name: "crm.deals", SnapshotInterval: interval})
	events, current, snaps := newFakeEvents(), newFakeCurrent(), newFakeSnapshots()
	resolver := NewConfigResolver(repo, Options{AutoCreate: true})
	ing := NewIngester(resolver, events, current, WithSnapshots(NewSnapshotBuilder(snaps, nil)))
	return stateAtFixture{
		rd:      NewReader(events, current, snaps),
		ing:     ing,
		events:  events,
		current: current,
		snaps:   snaps,
	}
}

// Reconstruction at the latest version must equal cur_*, and an anchored
// reconstruction (with snapshots) must equal one without — the S3-03 invariant.
func TestStateAtMatchesCurrentAndGenesis(t *testing.T) {
	fx := newStateAtFixture(t, 3)
	ctx := authedCtx()
	for i := 1; i <= 10; i++ {
		if _, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(float64(i))}); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}

	v10 := int64(10)
	got, err := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{Version: &v10})
	if err != nil {
		t.Fatalf("StateAt v10: %v", err)
	}
	cur, _ := fx.current.Get(ctx, "crm.deals", "d-1")
	if got.State["amount"] != cur.State["amount"] || got.Version != cur.Version {
		t.Fatalf("StateAt(latest) %+v != cur_* %+v", got, cur)
	}
	// v10 with interval 3 anchors on the v9 snapshot, replaying one tail event.
	if got.SnapshotVersion != 9 || got.EventsApplied != 1 {
		t.Fatalf("reconstructed_from wrong: snap=%d applied=%d", got.SnapshotVersion, got.EventsApplied)
	}

	// A genesis-only reader (no snapshot store) reconstructs the same state.
	genesis := NewReader(fx.events, fx.current, nil)
	g, err := genesis.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{Version: &v10})
	if err != nil {
		t.Fatalf("genesis StateAt: %v", err)
	}
	if g.State["amount"] != got.State["amount"] || g.SnapshotVersion != 0 || g.EventsApplied != 10 {
		t.Fatalf("genesis path diverged: %+v", g)
	}
}

func TestStateAtMidHistoryVersion(t *testing.T) {
	fx := newStateAtFixture(t, 3)
	ctx := authedCtx()
	for i := 1; i <= 10; i++ {
		_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(float64(i))})
	}
	v4 := int64(4)
	got, err := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{Version: &v4})
	if err != nil {
		t.Fatalf("StateAt v4: %v", err)
	}
	if got.Version != 4 || got.State["amount"] != float64(4) {
		t.Fatalf("state at v4 wrong: %+v", got)
	}
	// v4 anchors on the v3 snapshot.
	if got.SnapshotVersion != 3 || got.EventsApplied != 1 {
		t.Fatalf("reconstructed_from wrong: %+v", got)
	}
}

// A version past the tip clamps to the latest (reads as "current", Ш0).
func TestStateAtVersionClampedToLatest(t *testing.T) {
	fx := newStateAtFixture(t, 100)
	ctx := authedCtx()
	for i := 1; i <= 3; i++ {
		_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(float64(i))})
	}
	future := int64(999)
	got, err := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{Version: &future})
	if err != nil {
		t.Fatalf("StateAt: %v", err)
	}
	if got.Version != 3 || got.State["amount"] != float64(3) {
		t.Fatalf("clamp wrong: %+v", got)
	}
}

func TestStateAtByTime(t *testing.T) {
	fx := newStateAtFixture(t, 100)
	ctx := authedCtx()
	// Pin event timestamps so the cutoff is deterministic (v1=13:00, v2=14:00, v3=15:00).
	base := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= 3; i++ {
		hour := i
		fx.ing.now = func() time.Time { return base.Add(time.Duration(hour) * time.Hour) }
		_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(float64(i))})
	}
	at := base.Add(2*time.Hour + 30*time.Minute) // between v2 (14:00) and v3 (15:00) → v2
	got, err := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{At: &at})
	if err != nil {
		t.Fatalf("StateAt by time: %v", err)
	}
	if got.Version != 2 || got.State["amount"] != float64(2) {
		t.Fatalf("at-resolution wrong: %+v", got)
	}
}

// A cutoff before the first event has nothing to reconstruct → ErrNotFound.
func TestStateAtTimeBeforeFirstEvent(t *testing.T) {
	fx := newStateAtFixture(t, 100)
	ctx := authedCtx()
	base := time.Date(2026, 6, 14, 12, 0, 0, 0, time.UTC)
	fx.ing.now = func() time.Time { return base }
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(1)})

	before := base.Add(-time.Hour)
	if _, err := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{At: &before}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestStateAtUnknownEntity(t *testing.T) {
	fx := newStateAtFixture(t, 100)
	v1 := int64(1)
	if _, err := fx.rd.StateAt(authedCtx(), "crm.deals", "ghost", PointInTimeQuery{Version: &v1}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// A delete at the target version marks the result deleted while retaining the
// last value; a later reincarnation reconstructs cleanly.
func TestStateAtDeleteThenReincarnate(t *testing.T) {
	fx := newStateAtFixture(t, 100)
	ctx := authedCtx()
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	_, _ = fx.ing.Delete(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1"})
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: map[string]any{"reborn": true}})

	v2 := int64(2)
	atDelete, err := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{Version: &v2})
	if err != nil {
		t.Fatalf("StateAt v2: %v", err)
	}
	if !atDelete.Deleted || atDelete.State["amount"] != float64(100) {
		t.Fatalf("state at delete wrong: %+v", atDelete)
	}
	v3 := int64(3)
	reborn, err := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{Version: &v3})
	if err != nil {
		t.Fatalf("StateAt v3: %v", err)
	}
	if reborn.Deleted || reborn.State["reborn"] != true || reborn.State["amount"] != nil {
		t.Fatalf("reincarnation leaked fields: %+v", reborn)
	}
}

// Sanity: the events the fixture wrote replay with diff.Apply from genesis to the
// same state the reader reconstructs (guards the fake store's fidelity).
func TestStateAtTailMatchesManualReplay(t *testing.T) {
	fx := newStateAtFixture(t, 4)
	ctx := authedCtx()
	for i := 1; i <= 6; i++ {
		_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(float64(i))})
	}
	all, _ := fx.events.ListEvents(ctx, "crm.deals", domain.EventFilter{EntityID: "d-1", OrderBy: domain.OrderVersion})
	var state any = map[string]any{}
	for _, ev := range all {
		state, _ = diff.Apply(state, ev.Changes)
	}
	v6 := int64(6)
	got, _ := fx.rd.StateAt(ctx, "crm.deals", "d-1", PointInTimeQuery{Version: &v6})
	if got.State["amount"] != state.(map[string]any)["amount"] {
		t.Fatalf("reconstruction %v != manual replay %v", got.State, state)
	}
}
