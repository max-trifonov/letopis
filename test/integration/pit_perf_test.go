//go:build integration

package integration

import (
	"context"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/service"
	storage "github.com/max-trifonov/letopis/internal/storage/mongo"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// perfHistory is the synthetic history size and snapshot interval the
// point-in-time perf test runs against. The history is deep enough that a
// genesis replay would touch thousands of events, so the difference made by
// snapshots is unmistakable; the interval matches NFR-1.5's reference value.
const (
	perfHistory  = 10_000
	perfInterval = 100
)

// seedLongHistory writes a deep, single-entity history directly through the
// repositories (bypassing the ingester) so the perf test can stand up 10k
// versions in milliseconds rather than 10k round-trips. The history is a plain
// counter: amount == version. Snapshots are materialized at every interval-th
// version exactly as the builder would (S3-02), so the read path sees a
// production-shaped sn_*/ev_* layout.
func seedLongHistory(t *testing.T, ctx context.Context, events *storage.EventRepo, current *storage.CurrentRepo, snaps *storage.SnapshotRepo, coll, eid string, n, interval int) {
	t.Helper()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	evs := make([]*domain.Event, 0, n)
	for v := 1; v <= n; v++ {
		ev := &domain.Event{
			EntityID:   eid,
			Version:    int64(v),
			Op:         domain.OpUpdate,
			TSSource:   base.Add(time.Duration(v) * time.Second),
			TSReceived: base.Add(time.Duration(v) * time.Second),
		}
		if v == 1 {
			ev.Op = domain.OpCreate
			ev.Changes = []diff.Change{{Path: "amount", Op: diff.OpAdd, New: float64(1)}}
		} else {
			ev.Changes = []diff.Change{{Path: "amount", Op: diff.OpChange, Old: float64(v - 1), New: float64(v)}}
		}
		evs = append(evs, ev)
	}

	// Insert in chunks so a single InsertMany does not balloon past the driver's
	// batch limits on a 10k history.
	const chunk = 1000
	for i := 0; i < len(evs); i += chunk {
		end := min(i+chunk, len(evs))
		if err := events.AppendEvents(ctx, coll, evs[i:end]); err != nil {
			t.Fatalf("append [%d:%d]: %v", i, end, err)
		}
	}

	for v := interval; v <= n; v += interval {
		if err := snaps.Save(ctx, coll, &domain.Snapshot{
			EntityID: eid,
			Version:  int64(v),
			TS:       base.Add(time.Duration(v) * time.Second),
			State:    map[string]any{"amount": float64(v)},
		}); err != nil {
			t.Fatalf("save snapshot v%d: %v", v, err)
		}
	}

	if err := current.Upsert(ctx, coll, &domain.CurrentState{
		EntityID: eid,
		Version:  int64(n),
		TS:       base.Add(time.Duration(n) * time.Second),
		State:    map[string]any{"amount": float64(n)},
	}); err != nil {
		t.Fatalf("upsert current: %v", err)
	}
}

// TestIntegrationPointInTimePerf is the stage-3 perf lock (NFR-1.5, NFR-3.4). It
// proves the two claims snapshots exist to make:
//
//   - NFR-3.4: reconstruction work is bounded by the snapshot interval, not the
//     history length. The strong assertion is structural — every read applies at
//     most `interval` tail events, whether the target sits early or 10k versions
//     deep — which catches a builder regression (S3-02) that a wall-clock number
//     alone would miss.
//   - NFR-1.5: with that bound, p99 reconstruction latency stays under 200 ms.
//
// It runs against a real MongoDB so the snapshot lookup and tail scan use their
// indexes (verified separately by explain in the storage suite).
func TestIntegrationPointInTimePerf(t *testing.T) {
	uri := startMongo(t)
	conn, err := storage.NewConnManager(uri)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	ctx := tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: "acme"}})
	colRepo := storage.NewCollectionRepo(conn)
	const coll, eid = "crm.deals", "d-1"
	if err := colRepo.EnsurePhysical(ctx, coll); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}

	events := storage.NewEventRepo(conn)
	current := storage.NewCurrentRepo(conn)
	snaps := storage.NewSnapshotRepo(conn)
	seedLongHistory(t, ctx, events, current, snaps, coll, eid, perfHistory, perfInterval)

	anchored := service.NewReader(events, current, snaps)
	genesis := service.NewReader(events, current, nil) // no snapshots: replays the whole prefix

	// Sample targets uniformly across the full history, plus the boundary cases
	// (a multiple of the interval, the version just after one, the tip and just
	// below it) and a few deliberately deep targets to make the
	// independent-of-depth claim explicit.
	rng := rand.New(rand.NewSource(20260615))
	targets := []int64{1, perfInterval, perfInterval + 1, perfHistory - 1, perfHistory, 9000, 9999}
	for i := 0; i < 500; i++ {
		targets = append(targets, int64(1+rng.Intn(perfHistory)))
	}

	latencies := make([]time.Duration, 0, len(targets))
	maxApplied := 0
	for _, target := range targets {
		ver := target
		start := time.Now()
		got, err := anchored.StateAt(ctx, coll, eid, service.PointInTimeQuery{Version: &ver})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("StateAt v%d: %v", target, err)
		}
		latencies = append(latencies, elapsed)

		// NFR-3.4: the replayed tail never exceeds one interval, regardless of how
		// deep the target is.
		if got.EventsApplied > perfInterval {
			t.Fatalf("v%d applied %d tail events, want ≤ %d (reconstruction must be bounded by the interval, not history depth)", target, got.EventsApplied, perfInterval)
		}
		if got.EventsApplied > maxApplied {
			maxApplied = got.EventsApplied
		}

		// The anchor is the greatest snapshot at or below the target.
		wantSnap := (target / perfInterval) * perfInterval
		if got.SnapshotVersion != wantSnap {
			t.Fatalf("v%d anchored at snapshot v%d, want v%d", target, got.SnapshotVersion, wantSnap)
		}

		// Correctness: amount == version, identical to a full genesis replay.
		if got.State["amount"] != float64(target) || got.Deleted {
			t.Fatalf("v%d reconstructed %+v deleted=%v, want amount=%d", target, got.State, got.Deleted, target)
		}
	}

	// Path equivalence on the deep history: the snapshot-anchored read and a full
	// genesis replay agree, the snapshot one merely doing far less work.
	for _, target := range []int64{1, 4321, perfHistory} {
		ver := target
		a, _ := anchored.StateAt(ctx, coll, eid, service.PointInTimeQuery{Version: &ver})
		g, err := genesis.StateAt(ctx, coll, eid, service.PointInTimeQuery{Version: &ver})
		if err != nil {
			t.Fatalf("genesis v%d: %v", target, err)
		}
		if a.State["amount"] != g.State["amount"] {
			t.Fatalf("v%d anchored=%v != genesis=%v", target, a.State, g.State)
		}
		if g.SnapshotVersion != 0 {
			t.Fatalf("genesis reader used a snapshot at v%d", g.SnapshotVersion)
		}
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p50 := latencies[len(latencies)*50/100]
	p99 := latencies[len(latencies)*99/100]
	t.Logf("point-in-time over %d versions, interval %d: %d reads, max tail applied=%d, p50=%v p99=%v",
		perfHistory, perfInterval, len(latencies), maxApplied, p50, p99)

	// NFR-1.5: p99 reconstruction under 200 ms at interval 100.
	if p99 > 200*time.Millisecond {
		t.Fatalf("p99 reconstruction = %v, want < 200ms (NFR-1.5)", p99)
	}
}
