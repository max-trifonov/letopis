//go:build integration

package integration

import (
	"context"
	"log/slog"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/service"
	storage "github.com/max-trifonov/letopis/internal/storage/mongo"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// pitStack wires an Ingester (with the snapshot builder) and a Reader over a real
// MongoDB, with a per-collection snapshot interval seeded directly so the tests
// can cross boundaries without thousands of writes (S3-02/S3-03).
type pitStack struct {
	ing      *service.Ingester
	rd       *service.Reader
	genesis  *service.Reader // a reader with no snapshot store, to prove path-equivalence
	snaps    *storage.SnapshotRepo
	events   *storage.EventRepo
	current  *storage.CurrentRepo
	ctx      context.Context
	collname string
}

func newPITStack(t *testing.T, interval int) pitStack {
	t.Helper()
	uri := startMongo(t)
	conn, err := storage.NewConnManager(uri)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	ctx := tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: "acme"}})
	colRepo := storage.NewCollectionRepo(conn)
	const coll = "crm.deals"
	if err := colRepo.EnsurePhysical(ctx, coll); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}
	if err := colRepo.SaveConfig(ctx, &domain.CollectionConfig{Name: coll, SnapshotInterval: interval}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	events := storage.NewEventRepo(conn)
	current := storage.NewCurrentRepo(conn)
	snaps := storage.NewSnapshotRepo(conn)
	resolver := service.NewConfigResolver(colRepo, service.Options{AutoCreate: true})
	ing := service.NewIngester(resolver, events, current, service.WithSnapshots(service.NewSnapshotBuilder(snaps, slog.Default())))
	return pitStack{
		ing:      ing,
		rd:       service.NewReader(events, current, snaps),
		genesis:  service.NewReader(events, current, nil),
		snaps:    snaps,
		events:   events,
		current:  current,
		ctx:      ctx,
		collname: coll,
	}
}

func (s pitStack) write(t *testing.T, eid string, amount float64) {
	t.Helper()
	if _, err := s.ing.State(s.ctx, service.IngestCommand{Collection: s.collname, EntityID: eid, State: map[string]any{"amount": amount}}); err != nil {
		t.Fatalf("write %s=%v: %v", eid, amount, err)
	}
}

// After interval writes a snapshot exists whose state equals cur_* on that
// version (S3-02).
func TestIntegrationSnapshotMaterializesOnInterval(t *testing.T) {
	s := newPITStack(t, 5)
	for i := 1; i <= 12; i++ {
		s.write(t, "d-1", float64(i))
	}

	// Boundaries at v5 and v10.
	for _, v := range []int64{5, 10} {
		snap, err := s.snaps.Nearest(s.ctx, s.collname, "d-1", v)
		if err != nil {
			t.Fatalf("nearest v%d: %v", v, err)
		}
		if snap.Version != v {
			t.Fatalf("snapshot version = %d, want %d", snap.Version, v)
		}
		if snap.State["amount"] != float64(v) {
			t.Fatalf("snapshot state = %+v, want amount=%d", snap.State, v)
		}
	}
	// No snapshot before the first boundary.
	if _, err := s.snaps.Nearest(s.ctx, s.collname, "d-1", 4); err == nil {
		t.Fatalf("unexpected snapshot at v≤4")
	}
}

// A reprocessed event (same {eid,v}) must not duplicate the snapshot — Save
// upserts (S3-02). Re-running the builder on the same version leaves one doc.
func TestIntegrationSnapshotReprocessNoDuplicate(t *testing.T) {
	s := newPITStack(t, 5)
	for i := 1; i <= 5; i++ {
		s.write(t, "d-1", float64(i))
	}
	b := service.NewSnapshotBuilder(s.snaps, slog.Default())
	cur, err := s.current.Get(s.ctx, s.collname, "d-1")
	if err != nil {
		t.Fatalf("cur: %v", err)
	}
	// Re-snap v5 twice, as a reclaim would.
	b.Build(s.ctx, s.collname, "d-1", 5, cur.TS, cur.State, cur.Deleted, 5)
	b.Build(s.ctx, s.collname, "d-1", 5, cur.TS, cur.State, cur.Deleted, 5)

	snap, err := s.snaps.Nearest(s.ctx, s.collname, "d-1", 5)
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if snap.Version != 5 || snap.State["amount"] != float64(5) {
		t.Fatalf("re-snapped state wrong: %+v", snap)
	}
}

// Reconstruction at any version equals cur_* at the latest, and the snapshot path
// equals the genesis path (S3-03 invariant).
func TestIntegrationReconstructEquivalence(t *testing.T) {
	s := newPITStack(t, 3)
	const n = 12
	for i := 1; i <= n; i++ {
		s.write(t, "d-1", float64(i))
	}

	cur, _ := s.current.Get(s.ctx, s.collname, "d-1")
	for v := int64(1); v <= n; v++ {
		ver := v
		anchored, err := s.rd.StateAt(s.ctx, s.collname, "d-1", service.PointInTimeQuery{Version: &ver})
		if err != nil {
			t.Fatalf("anchored v%d: %v", v, err)
		}
		fromGenesis, err := s.genesis.StateAt(s.ctx, s.collname, "d-1", service.PointInTimeQuery{Version: &ver})
		if err != nil {
			t.Fatalf("genesis v%d: %v", v, err)
		}
		if anchored.State["amount"] != float64(v) || fromGenesis.State["amount"] != float64(v) {
			t.Fatalf("v%d: anchored=%v genesis=%v", v, anchored.State, fromGenesis.State)
		}
		if fromGenesis.SnapshotVersion != 0 {
			t.Fatalf("genesis reader must not use a snapshot: %+v", fromGenesis)
		}
	}
	// Latest equals cur_*.
	latest := int64(n)
	got, _ := s.rd.StateAt(s.ctx, s.collname, "d-1", service.PointInTimeQuery{Version: &latest})
	if got.State["amount"] != cur.State["amount"] {
		t.Fatalf("StateAt(latest) %v != cur_* %v", got.State, cur.State)
	}
}

// A snapshot taken at a delete reconstructs deleted=true with the retained value,
// and a reincarnation after it reconstructs cleanly (S3-02 delete + S3-03 tail).
func TestIntegrationReconstructAcrossDelete(t *testing.T) {
	s := newPITStack(t, 1) // snapshot every version, including the delete
	s.write(t, "d-1", 100)
	if _, err := s.ing.Delete(s.ctx, service.IngestCommand{Collection: s.collname, EntityID: "d-1"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.ing.State(s.ctx, service.IngestCommand{Collection: s.collname, EntityID: "d-1", State: map[string]any{"reborn": true}}); err != nil {
		t.Fatalf("reincarnate: %v", err)
	}

	// The delete version (v2) snapshot is deleted=true with the retained value.
	delSnap, err := s.snaps.Nearest(s.ctx, s.collname, "d-1", 2)
	if err != nil {
		t.Fatalf("nearest v2: %v", err)
	}
	if !delSnap.Deleted || delSnap.State["amount"] != float64(100) {
		t.Fatalf("delete snapshot wrong: %+v", delSnap)
	}

	v2 := int64(2)
	atDelete, err := s.rd.StateAt(s.ctx, s.collname, "d-1", service.PointInTimeQuery{Version: &v2})
	if err != nil {
		t.Fatalf("StateAt v2: %v", err)
	}
	if !atDelete.Deleted || atDelete.State["amount"] != float64(100) {
		t.Fatalf("reconstruct at delete wrong: %+v", atDelete)
	}
	v3 := int64(3)
	reborn, err := s.rd.StateAt(s.ctx, s.collname, "d-1", service.PointInTimeQuery{Version: &v3})
	if err != nil {
		t.Fatalf("StateAt v3: %v", err)
	}
	if reborn.Deleted || reborn.State["reborn"] != true || reborn.State["amount"] != nil {
		t.Fatalf("reincarnation leaked fields: %+v", reborn)
	}
}
