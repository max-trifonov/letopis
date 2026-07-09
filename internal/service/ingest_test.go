package service

import (
	"context"
	"errors"
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

// fakeEvents is an in-memory EventRepository: it assigns monotonic versions
// per entity like the real store, so the use-case can be tested without Mongo.
type fakeEvents struct {
	mu       sync.Mutex
	byEntity map[string][]domain.Event
	seenEID  map[string]bool // event_id → seen, models the {event_id} unique index
}

func newFakeEvents() *fakeEvents {
	return &fakeEvents{byEntity: map[string][]domain.Event{}, seenEID: map[string]bool{}}
}

func (f *fakeEvents) AppendEvent(_ context.Context, collection string, ev *domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Mirror the unique sparse {event_id} index (FR-1.6): a repeat is rejected with
	// the same sentinel the Mongo store surfaces, so the use-case's dedup handling
	// can be exercised without a real backend.
	if ev.EventID != "" && f.seenEID[ev.EventID] {
		return domain.ErrDuplicateEvent
	}
	key := collection + "|" + ev.EntityID
	ev.Version = int64(len(f.byEntity[key])) + 1
	ev.TSStored = time.Now().UTC()
	f.byEntity[key] = append(f.byEntity[key], *ev)
	if ev.EventID != "" {
		f.seenEID[ev.EventID] = true
	}
	return nil
}

// AppendEvents stores a pre-versioned batch as-is, mirroring the real store's
// InsertMany: versions are assigned by the caller (the fast batcher), not here.
func (f *fakeEvents) AppendEvents(_ context.Context, collection string, evs []*domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ev := range evs {
		key := collection + "|" + ev.EntityID
		if ev.TSStored.IsZero() {
			ev.TSStored = time.Now().UTC()
		}
		f.byEntity[key] = append(f.byEntity[key], *ev)
	}
	return nil
}

func (f *fakeEvents) LastEvent(_ context.Context, collection, entityID string) (*domain.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	evs := f.byEntity[collection+"|"+entityID]
	if len(evs) == 0 {
		return nil, domain.ErrNotFound
	}
	last := evs[len(evs)-1]
	return &last, nil
}

// ListEvents honours the subset of the filter the read paths exercise: the
// received-time window, the version cursor/upper bound (S3-03 tails), version
// ordering and the limit. Stored order is ascending by version.
func (f *fakeEvents) ListEvents(_ context.Context, collection string, fl domain.EventFilter) ([]domain.Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []domain.Event
	for _, ev := range f.byEntity[collection+"|"+fl.EntityID] {
		if !fl.From.IsZero() && ev.TSReceived.Before(fl.From) {
			continue
		}
		if !fl.To.IsZero() && ev.TSReceived.After(fl.To) {
			continue
		}
		if fl.After != nil && ev.Version <= fl.After.Version {
			continue
		}
		if fl.MaxVersion > 0 && ev.Version > fl.MaxVersion {
			continue
		}
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Version < out[j].Version })
	if fl.Order == domain.OrderDesc {
		slices.Reverse(out)
	}
	if fl.Limit > 0 && len(out) > fl.Limit {
		out = out[:fl.Limit]
	}
	return out, nil
}

type fakeCurrent struct {
	mu     sync.Mutex
	states map[string]domain.CurrentState
}

func newFakeCurrent() *fakeCurrent { return &fakeCurrent{states: map[string]domain.CurrentState{}} }

func (f *fakeCurrent) Get(_ context.Context, collection, entityID string) (*domain.CurrentState, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	st, ok := f.states[collection+"|"+entityID]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &st, nil
}

func (f *fakeCurrent) Upsert(_ context.Context, collection string, st *domain.CurrentState) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.states[collection+"|"+st.EntityID] = *st
	return nil
}

// fakeSnapshots is an in-memory SnapshotRepository keyed by {entity,version}, so
// Save upserts and Nearest scans for the greatest version ≤ target — the two
// behaviours the builder (S3-02) and point-in-time read (S3-03) depend on.
type fakeSnapshots struct {
	mu    sync.Mutex
	saved map[string]map[int64]domain.Snapshot // entity → version → snapshot
	saves int
	err   error // when set, Save fails (best-effort builder must swallow it)
}

func newFakeSnapshots() *fakeSnapshots {
	return &fakeSnapshots{saved: map[string]map[int64]domain.Snapshot{}}
}

func (f *fakeSnapshots) Save(_ context.Context, _ string, snap *domain.Snapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves++
	if f.err != nil {
		return f.err
	}
	if f.saved[snap.EntityID] == nil {
		f.saved[snap.EntityID] = map[int64]domain.Snapshot{}
	}
	f.saved[snap.EntityID][snap.Version] = *snap
	return nil
}

func (f *fakeSnapshots) Nearest(_ context.Context, _, entityID string, maxVersion int64) (*domain.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	best := int64(-1)
	for v := range f.saved[entityID] {
		if v <= maxVersion && v > best {
			best = v
		}
	}
	if best < 0 {
		return nil, domain.ErrNotFound
	}
	snap := f.saved[entityID][best]
	return &snap, nil
}

// ingestFixture wires a real Ingester over in-memory repositories. AutoCreate
// is on so the first write provisions the collection (S1-04).
type ingestFixture struct {
	ing     *Ingester
	events  *fakeEvents
	current *fakeCurrent
	repo    *fakeRepo
}

func newIngestFixture(t *testing.T) ingestFixture {
	t.Helper()
	repo := newFakeRepo()
	events := newFakeEvents()
	current := newFakeCurrent()
	resolver := NewConfigResolver(repo, Options{AutoCreate: true})
	return ingestFixture{ing: NewIngester(resolver, events, current), events: events, current: current, repo: repo}
}

func deal(amount float64) map[string]any { return map[string]any{"title": "d", "amount": amount} }

func TestIngestStateCreatesFirstEvent(t *testing.T) {
	fx := newIngestFixture(t)
	res, err := fx.ing.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if res.Version != 1 || res.Op != domain.OpCreate || res.ChangesCount == 0 || res.NoChanges {
		t.Fatalf("first event wrong: %+v", res)
	}
	cur, _ := fx.current.Get(authedCtx(), "crm.deals", "d-1")
	if cur.Version != 1 || cur.Deleted || cur.State["amount"] != float64(100) {
		t.Fatalf("current state wrong: %+v", cur)
	}
}

func TestIngestStateNoChanges(t *testing.T) {
	fx := newIngestFixture(t)
	ctx := authedCtx()
	if _, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	res, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if !res.NoChanges || res.Version != 1 {
		t.Fatalf("identical state must be no-op at v1: %+v", res)
	}
	if got := len(fx.events.byEntity["crm.deals|d-1"]); got != 1 {
		t.Fatalf("no-op wrote an event: %d events", got)
	}
}

func TestIngestStateUpdateBumpsVersion(t *testing.T) {
	fx := newIngestFixture(t)
	ctx := authedCtx()
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	res, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(250)})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if res.Version != 2 || res.Op != domain.OpUpdate {
		t.Fatalf("update wrong: %+v", res)
	}
}

func TestIngestFirstEventOpUpdate(t *testing.T) {
	fx := newIngestFixture(t)
	// Seed a config whose first_event_op is update (FR-1.11).
	fx.repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals", FirstEventOp: domain.FirstEventUpdate}
	res, err := fx.ing.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if res.Op != domain.OpUpdate {
		t.Fatalf("op = %s, want update (first_event_op)", res.Op)
	}
}

func TestIngestExplicitOpOverrides(t *testing.T) {
	fx := newIngestFixture(t)
	res, err := fx.ing.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", Op: domain.OpUpdate, State: deal(100)})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if res.Op != domain.OpUpdate {
		t.Fatalf("explicit op ignored: %+v", res)
	}
}

func TestIngestExpectedVersionConflict(t *testing.T) {
	fx := newIngestFixture(t)
	ctx := authedCtx()
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	stale := int64(0)
	_, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", ExpectedVersion: &stale, State: deal(250)})
	if !errors.Is(err, domain.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
	good := int64(1)
	if _, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", ExpectedVersion: &good, State: deal(250)}); err != nil {
		t.Fatalf("matching expected_version: %v", err)
	}
}

func TestIngestDiffApplies(t *testing.T) {
	fx := newIngestFixture(t)
	ctx := authedCtx()
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	res, err := fx.ing.Diff(ctx, IngestCommand{
		Collection: "crm.deals", EntityID: "d-1",
		Changes: []diff.Change{{Path: "amount", Op: diff.OpChange, Old: float64(100), New: float64(250)}},
	})
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	cur, _ := fx.current.Get(ctx, "crm.deals", "d-1")
	if res.Version != 2 || cur.State["amount"] != float64(250) {
		t.Fatalf("diff not applied to current: %+v / %+v", res, cur)
	}
}

func TestIngestDiffInvalid(t *testing.T) {
	fx := newIngestFixture(t)
	ctx := authedCtx()
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	// Removing a field that does not exist cannot apply.
	_, err := fx.ing.Diff(ctx, IngestCommand{
		Collection: "crm.deals", EntityID: "d-1",
		Changes: []diff.Change{{Path: "missing", Op: diff.OpRemove, Old: "x"}},
	})
	if !errors.Is(err, ErrInvalidDiff) {
		t.Fatalf("err = %v, want ErrInvalidDiff", err)
	}
}

func TestIngestDeleteThenReincarnate(t *testing.T) {
	fx := newIngestFixture(t)
	ctx := authedCtx()
	_, _ = fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})

	del, err := fx.ing.Delete(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1"})
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if del.Version != 2 || del.Op != domain.OpDelete {
		t.Fatalf("delete wrong: %+v", del)
	}
	cur, _ := fx.current.Get(ctx, "crm.deals", "d-1")
	if !cur.Deleted {
		t.Fatalf("current not flagged deleted: %+v", cur)
	}
	// History is preserved: the create and the delete are both there.
	if got := len(fx.events.byEntity["crm.deals|d-1"]); got != 2 {
		t.Fatalf("history not preserved: %d events", got)
	}

	// A create after delete reincarnates: op create, version keeps climbing.
	res, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(500)})
	if err != nil {
		t.Fatalf("reincarnate: %v", err)
	}
	if res.Version != 3 || res.Op != domain.OpCreate {
		t.Fatalf("reincarnation wrong: %+v", res)
	}
}

func TestIngestServerMetadata(t *testing.T) {
	fx := newIngestFixture(t)
	fixed := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	fx.ing.now = func() time.Time { return fixed }

	_, err := fx.ing.State(authedCtx(), IngestCommand{
		Collection: "crm.deals", EntityID: "d-1", RequestID: "req-7", ClientIP: "10.1.2.3",
		Meta:  map[string]any{"session": "abc"},
		State: deal(100),
	})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	ev := fx.events.byEntity["crm.deals|d-1"][0]
	if ev.RequestID != "req-7" || !ev.TSReceived.Equal(fixed) || ev.TSStored.IsZero() {
		t.Fatalf("server metadata missing: %+v", ev)
	}
	if ev.Meta["ip"] != "10.1.2.3" || ev.Meta["session"] != "abc" {
		t.Fatalf("meta merge wrong: %+v", ev.Meta)
	}
}

// The caller IP must not override a client-supplied ip, and the client's map
// must not be mutated (it may be shared/reused by the caller).
func TestIngestServerMetadataDoesNotClobber(t *testing.T) {
	fx := newIngestFixture(t)
	clientMeta := map[string]any{"ip": "203.0.113.9"}
	_, err := fx.ing.State(authedCtx(), IngestCommand{
		Collection: "crm.deals", EntityID: "d-1", ClientIP: "10.1.2.3", Meta: clientMeta, State: deal(100),
	})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	ev := fx.events.byEntity["crm.deals|d-1"][0]
	if ev.Meta["ip"] != "203.0.113.9" {
		t.Fatalf("client ip was clobbered: %+v", ev.Meta)
	}
	if len(clientMeta) != 1 {
		t.Fatalf("client meta map was mutated: %+v", clientMeta)
	}
}

func TestIngestAutoCreateDisabled(t *testing.T) {
	repo := newFakeRepo()
	resolver := NewConfigResolver(repo, Options{AutoCreate: false})
	ing := NewIngester(resolver, newFakeEvents(), newFakeCurrent())
	if _, err := ing.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}); !errors.Is(err, ErrAutoCreateDisabled) {
		t.Fatalf("err = %v, want ErrAutoCreateDisabled", err)
	}
}
