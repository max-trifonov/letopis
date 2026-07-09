//go:build integration

// These tests need a real MongoDB and Docker; they run under
// `go test -tags integration ./...`. They cover what unit tests cannot:
// version monotonicity under contention, idempotent provisioning, and that
// the architecture §3 indexes are actually created.
package mongo

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/service"
	"github.com/max-trifonov/letopis/internal/tenant"
)

func testConn(t *testing.T) (*ConnManager, context.Context) {
	t.Helper()
	ctx := context.Background()
	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start mongo container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	conn, err := NewConnManager(uri)
	if err != nil {
		t.Fatalf("conn manager: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	// Every storage call needs a tenant in the context (FR-7.2).
	authed := tenant.NewContext(ctx, tenant.Principal{Tenant: tenant.Tenant{ID: "test"}})
	return conn, authed
}

func sampleEvent(eid string) *domain.Event {
	return &domain.Event{
		EntityID:   eid,
		Op:         domain.OpUpdate,
		Author:     "42",
		TSReceived: time.Now().UTC(),
		Changes:    []diff.Change{{Path: "amount", Op: diff.OpChange, Old: float64(1), New: float64(2)}},
	}
}

func TestIntegrationAppendMonotonic(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewEventRepo(conn)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}

	for want := int64(1); want <= 5; want++ {
		ev := sampleEvent("d-1")
		if err := repo.AppendEvent(ctx, "crm.deals", ev); err != nil {
			t.Fatalf("append: %v", err)
		}
		if ev.Version != want {
			t.Fatalf("version = %d, want %d", ev.Version, want)
		}
	}

	last, err := repo.LastEvent(ctx, "crm.deals", "d-1")
	if err != nil {
		t.Fatalf("last event: %v", err)
	}
	if last.Version != 5 {
		t.Fatalf("last version = %d, want 5", last.Version)
	}
}

// The storage idempotency barrier (FR-1.6): a repeated event_id is rejected by
// the unique sparse index and surfaced as domain.ErrDuplicateEvent, leaving a
// single document — not a second event and not an opaque write error.
func TestIntegrationAppendDuplicateEventID(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewEventRepo(conn)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}

	first := sampleEvent("d-1")
	first.EventID = "evt-1"
	if err := repo.AppendEvent(ctx, "crm.deals", first); err != nil {
		t.Fatalf("first append: %v", err)
	}
	// A second event carrying the same event_id (a reclaimed duplicate) collides.
	dup := sampleEvent("d-2")
	dup.EventID = "evt-1"
	if err := repo.AppendEvent(ctx, "crm.deals", dup); !errors.Is(err, domain.ErrDuplicateEvent) {
		t.Fatalf("duplicate event_id err = %v, want ErrDuplicateEvent", err)
	}
}

func TestIntegrationLastEventNotFound(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewEventRepo(conn)
	if _, err := repo.LastEvent(ctx, "crm.deals", "ghost"); err != domain.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// Concurrent appends to one entity must serialize through the unique {eid,v}
// index: every version 1..N appears exactly once, no gaps (FR-1.10).
func TestIntegrationConcurrentAppend(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewEventRepo(conn)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}

	const n = 25
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- repo.AppendEvent(ctx, "crm.deals", sampleEvent("d-1"))
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent append: %v", err)
		}
	}

	events, err := repo.ListEvents(ctx, "crm.deals", domain.EventFilter{EntityID: "d-1"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(events) != n {
		t.Fatalf("got %d events, want %d", len(events), n)
	}
	seen := map[int64]bool{}
	for _, ev := range events {
		if seen[ev.Version] {
			t.Fatalf("duplicate version %d", ev.Version)
		}
		seen[ev.Version] = true
	}
	for v := int64(1); v <= n; v++ {
		if !seen[v] {
			t.Fatalf("missing version %d (gap)", v)
		}
	}
}

func TestIntegrationCurrentUpsert(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewCurrentRepo(conn)

	st := &domain.CurrentState{EntityID: "d-1", Version: 1, TS: time.Now().UTC(), State: map[string]any{"amount": float64(1)}}
	if err := repo.Upsert(ctx, "crm.deals", st); err != nil {
		t.Fatalf("upsert v1: %v", err)
	}
	st2 := &domain.CurrentState{EntityID: "d-1", Version: 2, TS: time.Now().UTC(), Deleted: true, State: map[string]any{"amount": float64(2)}}
	if err := repo.Upsert(ctx, "crm.deals", st2); err != nil {
		t.Fatalf("upsert v2: %v", err)
	}

	got, err := repo.Get(ctx, "crm.deals", "d-1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Version != 2 || !got.Deleted {
		t.Fatalf("current = %+v, want v2 deleted", got)
	}
}

func TestIntegrationEnsurePhysicalIdempotent(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	for i := range 3 {
		if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
			t.Fatalf("ensure physical attempt %d: %v", i, err)
		}
	}

	db, err := conn.DBFor(ctx)
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	got := indexNames(t, ctx, db.Collection(eventsCollection("crm.deals")))
	for _, want := range []string{"_id_", idxEventVersion, idxEventID, "eid_1_ts_st_-1", "author_1_ts_st_-1", "flow.f_1_ts_rcv_1"} {
		if !got[want] {
			t.Fatalf("missing index %q; have %v", want, got)
		}
	}
}

// The {eid,v} lookup must use its index, not a collection scan (DoD: verified
// by explain).
func TestIntegrationLastEventUsesIndex(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}
	repo := NewEventRepo(conn)
	if err := repo.AppendEvent(ctx, "crm.deals", sampleEvent("d-1")); err != nil {
		t.Fatalf("append: %v", err)
	}

	db, _ := conn.DBFor(ctx)
	var res bson.M
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: eventsCollection("crm.deals")},
			{Key: "filter", Value: bson.D{{Key: "eid", Value: "d-1"}}},
			{Key: "sort", Value: bson.D{{Key: "v", Value: -1}}},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}
	if err := db.RunCommand(ctx, cmd).Decode(&res); err != nil {
		t.Fatalf("explain: %v", err)
	}
	winning := lookup(lookup(res, "queryPlanner"), "winningPlan")
	if !planUsesIndex(winning) {
		t.Fatalf("winning plan did not use an index; explain = %v", res)
	}
}

// lookup fetches a key from a decoded BSON document, which may be a bson.M or
// (for nested documents) a bson.D.
func lookup(v any, key string) any {
	switch t := v.(type) {
	case bson.M:
		return t[key]
	case bson.D:
		for _, e := range t {
			if e.Key == key {
				return e.Value
			}
		}
	}
	return nil
}

// planUsesIndex walks an explain document and reports whether any IXSCAN
// stage appears. Mongo 7's plan shape varies (classic vs SBE, nested
// queryPlan/inputStage), so a recursive search is more robust than walking a
// fixed path.
func planUsesIndex(v any) bool {
	switch t := v.(type) {
	case bson.M:
		for _, child := range t {
			if planUsesIndex(child) {
				return true
			}
		}
	case bson.D:
		for _, e := range t {
			if (e.Key == "stage" && e.Value == "IXSCAN") || planUsesIndex(e.Value) {
				return true
			}
		}
	case bson.A:
		for _, child := range t {
			if planUsesIndex(child) {
				return true
			}
		}
	case string:
		return t == "IXSCAN"
	}
	return false
}

// planHasStage walks an explain document and reports whether a named plan stage
// (e.g. "SORT") appears anywhere in it.
func planHasStage(v any, name string) bool {
	switch t := v.(type) {
	case bson.M:
		for _, child := range t {
			if planHasStage(child, name) {
				return true
			}
		}
	case bson.D:
		for _, e := range t {
			if (e.Key == "stage" && e.Value == name) || planHasStage(e.Value, name) {
				return true
			}
		}
	case bson.A:
		for _, child := range t {
			if planHasStage(child, name) {
				return true
			}
		}
	}
	return false
}

// Stats reads the newest event time via a reverse $natural scan limited to one
// document, deliberately without a ts_st index (stats_repo.go). This locks in
// that access shape: the plan must not introduce a blocking in-memory SORT
// stage, which on a large ev_* would defeat the "cheap counters" promise (FR-3.6).
func TestIntegrationStatsLastEventNoBlockingSort(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}
	repo := NewEventRepo(conn)
	if err := repo.AppendEvent(ctx, "crm.deals", sampleEvent("d-1")); err != nil {
		t.Fatalf("append: %v", err)
	}

	db, _ := conn.DBFor(ctx)
	var res bson.M
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: eventsCollection("crm.deals")},
			{Key: "filter", Value: bson.D{}},
			{Key: "sort", Value: bson.D{{Key: "$natural", Value: -1}}},
			{Key: "projection", Value: bson.D{{Key: "ts_st", Value: 1}}},
			{Key: "limit", Value: 1},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}
	if err := db.RunCommand(ctx, cmd).Decode(&res); err != nil {
		t.Fatalf("explain: %v", err)
	}
	if planHasStage(lookup(lookup(res, "queryPlanner"), "winningPlan"), "SORT") {
		t.Fatalf("last-event read introduced a blocking SORT; explain = %v", res)
	}
}

func TestIntegrationConfigRoundTrip(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)

	cfg := domain.WithDefaults(domain.CollectionConfig{Name: "crm.deals", ArrayKeys: map[string]string{"items": "sku"}})
	if err := crepo.SaveConfig(ctx, &cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}
	got, err := crepo.GetConfig(ctx, "crm.deals")
	if err != nil {
		t.Fatalf("get config: %v", err)
	}
	if got.FirstEventOp != domain.FirstEventCreate || got.ArrayKeys["items"] != "sku" {
		t.Fatalf("config round-trip lost data: %+v", got)
	}
	if _, err := crepo.GetConfig(ctx, "unknown.coll"); err != domain.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// EnsureCollection through the service must create the config and physical
// collections on first use and be a no-op (idempotent) afterwards (S1-04).
func TestIntegrationEnsureCollectionViaService(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	resolver := service.NewConfigResolver(crepo, service.Options{AutoCreate: true})

	for i := range 3 {
		cfg, err := resolver.EnsureCollection(ctx, "crm.deals")
		if err != nil {
			t.Fatalf("ensure attempt %d: %v", i, err)
		}
		if cfg.FirstEventOp != domain.FirstEventCreate {
			t.Fatalf("attempt %d: missing defaults %+v", i, cfg)
		}
		resolver.Invalidate(ctx, "crm.deals") // force re-read so we exercise storage each time
	}

	// The append path now works against the auto-created collection.
	repo := NewEventRepo(conn)
	if err := repo.AppendEvent(ctx, "crm.deals", sampleEvent("d-1")); err != nil {
		t.Fatalf("append after auto-create: %v", err)
	}

	// With auto-create off, an unknown collection is refused.
	off := service.NewConfigResolver(crepo, service.Options{AutoCreate: false})
	if _, err := off.EnsureCollection(ctx, "docs.unknown"); err != service.ErrAutoCreateDisabled {
		t.Fatalf("err = %v, want ErrAutoCreateDisabled", err)
	}
}

// A flow read must collect both its activities (ev__flow) and the change
// events tagged with the flow, fanned out across the tenant's ev_* collections
// — and must not pick up another flow's nodes (FR-8.3).
func TestIntegrationFlowFanOut(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	erepo := NewEventRepo(conn)
	frepo := NewFlowRepo(conn)

	for _, c := range []string{"crm.deals", "crm.prices"} {
		if err := crepo.EnsurePhysical(ctx, c); err != nil {
			t.Fatalf("ensure %s: %v", c, err)
		}
	}

	// A change event tagged with the flow, in each of two collections.
	dealEv := sampleEvent("d-1")
	dealEv.Flow = &domain.Flow{ID: "f-1", Step: "approved"}
	if err := erepo.AppendEvent(ctx, "crm.deals", dealEv); err != nil {
		t.Fatalf("append deal: %v", err)
	}
	priceEv := sampleEvent("p-44")
	priceEv.Flow = &domain.Flow{ID: "f-1"}
	if err := erepo.AppendEvent(ctx, "crm.prices", priceEv); err != nil {
		t.Fatalf("append price: %v", err)
	}
	// A second event in another flow must not leak in.
	otherEv := sampleEvent("d-2")
	otherEv.Flow = &domain.Flow{ID: "f-other"}
	if err := erepo.AppendEvent(ctx, "crm.deals", otherEv); err != nil {
		t.Fatalf("append other: %v", err)
	}

	// An activity in the flow.
	act := &domain.Activity{
		ActivityID: domain.NewActivityID(),
		FlowID:     "f-1",
		Type:       "recalc.prices",
		TSReceived: time.Now().UTC(),
		CausedBy:   []domain.FlowRef{{Collection: "crm.deals", EntityID: "d-1", Version: dealEv.Version}},
	}
	if err := frepo.AppendActivity(ctx, act); err != nil {
		t.Fatalf("append activity: %v", err)
	}

	events, err := frepo.FlowEvents(ctx, "f-1")
	if err != nil {
		t.Fatalf("flow events: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("got %d flow events, want 2 (other flow leaked?)", len(events))
	}
	colls := map[string]bool{}
	for _, e := range events {
		colls[e.Collection] = true
	}
	if !colls["crm.deals"] || !colls["crm.prices"] {
		t.Fatalf("fan-out missed a collection: %v", colls)
	}

	acts, err := frepo.FlowActivities(ctx, "f-1")
	if err != nil {
		t.Fatalf("flow activities: %v", err)
	}
	if len(acts) != 1 || acts[0].ActivityID != act.ActivityID {
		t.Fatalf("flow activities wrong: %+v", acts)
	}
}

func TestIntegrationFlowIndexes(t *testing.T) {
	conn, ctx := testConn(t)
	frepo := NewFlowRepo(conn)
	// First append provisions ev__flow and its indexes.
	if err := frepo.AppendActivity(ctx, &domain.Activity{ActivityID: domain.NewActivityID(), FlowID: "f-1", TSReceived: time.Now().UTC()}); err != nil {
		t.Fatalf("append: %v", err)
	}

	db, err := conn.DBFor(ctx)
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	got := indexNames(t, ctx, db.Collection(FlowActivities))
	for _, want := range []string{"_id_", "flow_1_ts_rcv_1", "refs.c_1_refs.eid_1_ts_rcv_-1", "uniq_aid"} {
		if !got[want] {
			t.Fatalf("missing index %q; have %v", want, got)
		}
	}
}

// Nearest returns the snapshot with the greatest version ≤ maxVersion, and
// ErrNotFound when none is at or below the target (FR-3.2).
func TestIntegrationSnapshotNearest(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}
	repo := NewSnapshotRepo(conn)

	for _, v := range []int64{100, 200} {
		snap := &domain.Snapshot{EntityID: "d-1", Version: v, TS: time.Now().UTC(), State: map[string]any{"v": v}}
		if err := repo.Save(ctx, "crm.deals", snap); err != nil {
			t.Fatalf("save v%d: %v", v, err)
		}
	}

	cases := []struct {
		max  int64
		want int64 // 0 means ErrNotFound
	}{
		{max: 50, want: 0},
		{max: 100, want: 100},
		{max: 150, want: 100},
		{max: 200, want: 200},
		{max: 250, want: 200},
	}
	for _, c := range cases {
		got, err := repo.Nearest(ctx, "crm.deals", "d-1", c.max)
		if c.want == 0 {
			if !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("Nearest(max=%d) err = %v, want ErrNotFound", c.max, err)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Nearest(max=%d): %v", c.max, err)
		}
		if got.Version != c.want {
			t.Fatalf("Nearest(max=%d) = v%d, want v%d", c.max, got.Version, c.want)
		}
	}

	// An entity with no snapshots at all is also ErrNotFound, not an error.
	if _, err := repo.Nearest(ctx, "crm.deals", "ghost", 1000); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Nearest(ghost) err = %v, want ErrNotFound", err)
	}
}

// Save is idempotent on {eid,v}: re-snapping a version replaces it rather than
// inserting a duplicate, so a builder re-run leaves exactly one document.
func TestIntegrationSnapshotSaveIdempotent(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}
	repo := NewSnapshotRepo(conn)

	first := &domain.Snapshot{EntityID: "d-1", Version: 100, TS: time.Now().UTC(), State: map[string]any{"amount": float64(1)}}
	if err := repo.Save(ctx, "crm.deals", first); err != nil {
		t.Fatalf("save first: %v", err)
	}
	second := &domain.Snapshot{EntityID: "d-1", Version: 100, TS: time.Now().UTC(), State: map[string]any{"amount": float64(2)}}
	if err := repo.Save(ctx, "crm.deals", second); err != nil {
		t.Fatalf("save second: %v", err)
	}

	db, _ := conn.DBFor(ctx)
	n, err := db.Collection(snapCollection("crm.deals")).CountDocuments(ctx, bson.D{{Key: "eid", Value: "d-1"}})
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("snapshot count = %d, want 1 (upsert, not duplicate)", n)
	}
	got, err := repo.Nearest(ctx, "crm.deals", "d-1", 100)
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if got.State["amount"] != float64(2) {
		t.Fatalf("state = %+v, want re-snapped amount=2", got.State)
	}
}

// EnsurePhysical provisions sn_* with the unique {eid:1,v:-1} index, and the
// Nearest query uses it rather than scanning the collection.
func TestIntegrationSnapshotProvisionAndIndex(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}

	db, err := conn.DBFor(ctx)
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	if got := indexNames(t, ctx, db.Collection(snapCollection("crm.deals"))); !got["_id_"] || !got[idxSnapVersion] {
		t.Fatalf("missing snapshot index; have %v", got)
	}

	repo := NewSnapshotRepo(conn)
	if err := repo.Save(ctx, "crm.deals", &domain.Snapshot{EntityID: "d-1", Version: 100, TS: time.Now().UTC()}); err != nil {
		t.Fatalf("save: %v", err)
	}
	var res bson.M
	cmd := bson.D{
		{Key: "explain", Value: bson.D{
			{Key: "find", Value: snapCollection("crm.deals")},
			{Key: "filter", Value: bson.D{{Key: "eid", Value: "d-1"}, {Key: "v", Value: bson.D{{Key: "$lte", Value: int64(150)}}}}},
			{Key: "sort", Value: bson.D{{Key: "v", Value: -1}}},
		}},
		{Key: "verbosity", Value: "queryPlanner"},
	}
	if err := db.RunCommand(ctx, cmd).Decode(&res); err != nil {
		t.Fatalf("explain: %v", err)
	}
	if !planUsesIndex(lookup(lookup(res, "queryPlanner"), "winningPlan")) {
		t.Fatalf("Nearest did not use an index; explain = %v", res)
	}
}

// Nested objects and arrays in state must round-trip as neutral JSON types, not
// the driver's bson.D/bson.A — the regression that bit diff replay in S2-07.
func TestIntegrationSnapshotStateRoundTrip(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}
	repo := NewSnapshotRepo(conn)

	state := map[string]any{
		"buyer": map[string]any{"name": "ACME", "tier": float64(2)},
		"items": []any{
			map[string]any{"sku": "A1", "qty": float64(3)},
			map[string]any{"sku": "B2", "qty": float64(1)},
		},
	}
	if err := repo.Save(ctx, "crm.deals", &domain.Snapshot{EntityID: "d-1", Version: 1, TS: time.Now().UTC(), State: state}); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := repo.Nearest(ctx, "crm.deals", "d-1", 1)
	if err != nil {
		t.Fatalf("nearest: %v", err)
	}
	if _, ok := got.State["buyer"].(map[string]any); !ok {
		t.Fatalf("nested object decoded as %T, want map[string]any", got.State["buyer"])
	}
	items, ok := got.State["items"].([]any)
	if !ok {
		t.Fatalf("array decoded as %T, want []any", got.State["items"])
	}
	if first, ok := items[0].(map[string]any); !ok || first["sku"] != "A1" {
		t.Fatalf("array element = %v (%T), want map with sku=A1", items[0], items[0])
	}
}

// Stats must count what was actually written (FR-3.6): entities from cur_*
// documents, events from ev_*, and last_event_at from the newest event. The
// counts come from cheap primitives, but on a fresh collection they are exact.
func TestIntegrationCollectionStats(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	erepo := NewEventRepo(conn)
	curRepo := NewCurrentRepo(conn)
	stats := NewStatsRepo(conn)

	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}

	// Two entities, three events; the last append fixes the expected activity time.
	var lastTS time.Time
	for _, eid := range []string{"d-1", "d-1", "d-2"} {
		ev := sampleEvent(eid)
		if err := erepo.AppendEvent(ctx, "crm.deals", ev); err != nil {
			t.Fatalf("append %s: %v", eid, err)
		}
		lastTS = ev.TSStored
	}
	for _, eid := range []string{"d-1", "d-2"} {
		if err := curRepo.Upsert(ctx, "crm.deals", &domain.CurrentState{EntityID: eid, Version: 1, State: map[string]any{"x": float64(1)}}); err != nil {
			t.Fatalf("upsert %s: %v", eid, err)
		}
	}

	got, err := stats.Stats(ctx, "crm.deals")
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if got.Entities != 2 {
		t.Fatalf("entities = %d, want 2", got.Entities)
	}
	if got.Events != 3 {
		t.Fatalf("events = %d, want 3", got.Events)
	}
	// Mongo stores datetimes at millisecond precision, so compare on that grain.
	if want := lastTS.Truncate(time.Millisecond); !got.LastEventAt.Equal(want) {
		t.Fatalf("last_event_at = %v, want %v", got.LastEventAt, want)
	}

	// An empty (just-provisioned) collection reports zeros, not an error.
	if err := crepo.EnsurePhysical(ctx, "crm.empty"); err != nil {
		t.Fatalf("ensure empty: %v", err)
	}
	empty, err := stats.Stats(ctx, "crm.empty")
	if err != nil {
		t.Fatalf("stats empty: %v", err)
	}
	if empty.Entities != 0 || empty.Events != 0 || !empty.LastEventAt.IsZero() {
		t.Fatalf("empty stats = %+v, want zeros", empty)
	}
}

// ListCollections is the union of physical ev_* namespaces (auto-created
// collections) and stored _collections configs (configured but unwritten),
// sorted — so neither kind is missed (FR-3.6).
func TestIntegrationListCollectionsUnion(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	stats := NewStatsRepo(conn)

	// Auto-created: physical ev_* exists, no explicit config beyond provisioning.
	if err := crepo.EnsurePhysical(ctx, "crm.deals"); err != nil {
		t.Fatalf("ensure physical: %v", err)
	}
	// Config-only: a config saved without provisioning the physical collections.
	cfg := domain.WithDefaults(domain.CollectionConfig{Name: "docs.invoices"})
	if err := crepo.SaveConfig(ctx, &cfg); err != nil {
		t.Fatalf("save config: %v", err)
	}

	names, err := stats.ListCollections(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	want := []string{"crm.deals", "docs.invoices"}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	for i, n := range want {
		if names[i] != n {
			t.Fatalf("names = %v, want %v (sorted)", names, want)
		}
	}
}

func indexNames(t *testing.T, ctx context.Context, coll *mongo.Collection) map[string]bool {
	t.Helper()
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	var specs []bson.M
	if err := cur.All(ctx, &specs); err != nil {
		t.Fatalf("decode indexes: %v", err)
	}
	names := map[string]bool{}
	for _, s := range specs {
		if name, ok := s["name"].(string); ok {
			names[name] = true
		}
	}
	return names
}

// The admin config flow (S3-05) must persist the config, provision the physical
// collections, invalidate the resolver cache so the new value is seen at once,
// and leave an audit record in ev__system.
func TestIntegrationConfigAdminUpdate(t *testing.T) {
	conn, ctx := testConn(t)
	crepo := NewCollectionRepo(conn)
	resolver := service.NewConfigResolver(crepo, service.Options{AutoCreate: true})
	audit := NewSystemAuditRepo(conn)
	svc := service.NewCollectionConfigService(crepo, resolver, audit, slog.Default())

	// Prime the resolver cache with a first config so we can prove invalidation.
	if _, err := svc.Update(ctx, &domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityDurable}); err != nil {
		t.Fatalf("first update: %v", err)
	}
	if cfg, err := resolver.Get(ctx, "crm.deals"); err != nil || cfg.ReliabilityMode != domain.ReliabilityDurable {
		t.Fatalf("cached config = %+v, err = %v", cfg, err)
	}

	// Change the mode; the resolver must return the new value without waiting for
	// the TTL (cache invalidated).
	if _, err := svc.Update(ctx, &domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityStrict, SnapshotInterval: 7}); err != nil {
		t.Fatalf("second update: %v", err)
	}
	cfg, err := resolver.Get(ctx, "crm.deals")
	if err != nil {
		t.Fatalf("get after update: %v", err)
	}
	if cfg.ReliabilityMode != domain.ReliabilityStrict || cfg.SnapshotInterval != 7 {
		t.Fatalf("resolver served stale config: %+v", cfg)
	}

	// The stored record is raw (only the fields the admin set); the resolver fills
	// the rest with defaults on read, which is how GET marks them.
	stored, err := crepo.GetConfig(ctx, "crm.deals")
	if err != nil {
		t.Fatalf("get stored: %v", err)
	}
	if stored.MaxEventSizeBytes != 0 {
		t.Fatalf("stored config should be raw, got %+v", stored)
	}
	if cfg.MaxEventSizeBytes != domain.DefaultMaxEventSizeBytes {
		t.Fatalf("resolver did not apply default max size: %+v", cfg)
	}

	// Physical collections provisioned (ev_/cur_/sn_).
	db, _ := conn.DBFor(ctx)
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		t.Fatalf("list collections: %v", err)
	}
	have := map[string]bool{}
	for _, n := range names {
		have[n] = true
	}
	for _, want := range []string{eventsCollection("crm.deals"), currentCollection("crm.deals"), snapCollection("crm.deals")} {
		if !have[want] {
			t.Errorf("physical collection %q not provisioned; have %v", want, names)
		}
	}

	// Two updates → two audit records in ev__system.
	count, err := db.Collection(SystemEvents).CountDocuments(ctx, bson.D{{Key: "action", Value: "collection.config.updated"}})
	if err != nil {
		t.Fatalf("count audit: %v", err)
	}
	if count != 2 {
		t.Fatalf("audit records = %d, want 2", count)
	}
	var doc auditDoc
	if err := db.Collection(SystemEvents).FindOne(ctx, bson.D{{Key: "collection", Value: "crm.deals"}}).Decode(&doc); err != nil {
		t.Fatalf("decode audit: %v", err)
	}
	if doc.Actor != "test" || doc.ID == "" || doc.TS.IsZero() {
		t.Fatalf("audit doc malformed: %+v", doc)
	}

	// ev__system is a system collection: it must not surface in the stats listing.
	for _, n := range names {
		if n == SystemEvents && isUserEventCollection(n) {
			t.Fatalf("ev__system wrongly classed as a user collection")
		}
	}
}
