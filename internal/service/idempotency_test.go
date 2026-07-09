package service

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
)

// fakeIdempotency is an in-memory domain.IdempotencyStore. Reserve is atomic
// under the mutex, so the race test has a single deterministic winner — exactly
// the guarantee a real SET NX gives.
type fakeIdempotency struct {
	mu       sync.Mutex
	m        map[string]domain.IdempotencyRecord
	err      error // when set, every Reserve fails (models Redis down)
	released int
	reserveN int
}

func newFakeIdempotency() *fakeIdempotency {
	return &fakeIdempotency{m: map[string]domain.IdempotencyRecord{}}
}

func (f *fakeIdempotency) Reserve(_ context.Context, key string, rec domain.IdempotencyRecord) (bool, domain.IdempotencyRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reserveN++
	if f.err != nil {
		return false, domain.IdempotencyRecord{}, f.err
	}
	if existing, ok := f.m[key]; ok {
		return false, existing, nil
	}
	f.m[key] = rec
	return true, domain.IdempotencyRecord{}, nil
}

func (f *fakeIdempotency) Release(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.released++
	delete(f.m, key)
	return nil
}

// newDedupFixture wires an AsyncIngester with a receipt-dedup store over the
// shared in-memory repos.
func newDedupFixture(t *testing.T, idemp domain.IdempotencyStore) asyncFixture {
	t.Helper()
	repo := newFakeRepo()
	events := newFakeEvents()
	resolver := NewConfigResolver(repo, Options{AutoCreate: true})
	ing := NewIngester(resolver, events, newFakeCurrent())
	durable, fast := &stubQueue{}, &stubQueue{}
	tickets := newFakeTickets()
	return asyncFixture{
		async:   NewAsyncIngester(ing, NewPipeline(durable, fast, nil), NewTicketService(tickets), AsyncOptions{Idempotency: idemp}),
		durable: durable, fast: fast, tickets: tickets, events: events,
	}
}

// Two deliveries of the same event_id collapse to one accept: same ticket, one
// enqueue (the first idempotency barrier, FR-1.6).
func TestAsyncDedupReceiptSameEventID(t *testing.T) {
	fx := newDedupFixture(t, newFakeIdempotency())
	cmd := IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-1", State: deal(100)}

	first, err := fx.async.State(authedCtx(), cmd)
	if err != nil || !first.Async || first.Ticket == "" {
		t.Fatalf("first accept = %+v err=%v", first, err)
	}
	second, err := fx.async.State(authedCtx(), cmd)
	if err != nil {
		t.Fatalf("second accept: %v", err)
	}
	if !second.Deduplicated || second.Ticket != first.Ticket {
		t.Fatalf("repeat must replay the first ticket: %+v (first %s)", second, first.Ticket)
	}
	if fx.durable.count() != 1 {
		t.Fatalf("repeat enqueued again: count = %d, want 1", fx.durable.count())
	}
}

// Distinct event_ids are independent: two enqueues, two tickets.
func TestAsyncDedupDifferentEventIDs(t *testing.T) {
	fx := newDedupFixture(t, newFakeIdempotency())
	a, _ := fx.async.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-1", State: deal(100)})
	b, _ := fx.async.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-2", State: deal(200)})
	if a.Ticket == b.Ticket || a.Deduplicated || b.Deduplicated {
		t.Fatalf("distinct event_ids deduped: a=%+v b=%+v", a, b)
	}
	if fx.durable.count() != 2 {
		t.Fatalf("enqueue count = %d, want 2", fx.durable.count())
	}
}

// With no event_id the receipt barrier does not apply: every delivery enqueues.
func TestAsyncDedupNoEventID(t *testing.T) {
	idemp := newFakeIdempotency()
	fx := newDedupFixture(t, idemp)
	cmd := IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}
	_, _ = fx.async.State(authedCtx(), cmd)
	_, _ = fx.async.State(authedCtx(), cmd)
	if fx.durable.count() != 2 {
		t.Fatalf("no-key writes deduped: count = %d, want 2", fx.durable.count())
	}
	if idemp.reserveN != 0 {
		t.Fatalf("Reserve called without an event_id: %d", idemp.reserveN)
	}
}

// Redis down (Reserve errors) degrades open: the write is accepted undeduped
// rather than refused (architecture §7). The storage barrier is the backstop.
func TestAsyncDedupDegradesWhenStoreDown(t *testing.T) {
	idemp := &fakeIdempotency{m: map[string]domain.IdempotencyRecord{}, err: errors.New("redis down")}
	fx := newDedupFixture(t, idemp)
	cmd := IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-1", State: deal(100)}

	res, err := fx.async.State(authedCtx(), cmd)
	if err != nil || !res.Async || res.Ticket == "" {
		t.Fatalf("degraded accept = %+v err=%v", res, err)
	}
	if fx.durable.count() != 1 {
		t.Fatalf("degraded write not enqueued: count = %d", fx.durable.count())
	}
}

// A failed publish after a reservation releases the key, so a retry is not
// pinned to a ticket that never shipped.
func TestAsyncDedupReleasesOnPublishFailure(t *testing.T) {
	idemp := newFakeIdempotency()
	repo := newFakeRepo()
	resolver := NewConfigResolver(repo, Options{AutoCreate: true})
	ing := NewIngester(resolver, newFakeEvents(), newFakeCurrent())
	failing := &failQueue{}
	async := NewAsyncIngester(ing, NewPipeline(failing, nil, nil), NewTicketService(newFakeTickets()), AsyncOptions{Idempotency: idemp})

	_, err := async.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-1", State: deal(100)})
	if err == nil {
		t.Fatal("expected publish failure to surface")
	}
	if idemp.released != 1 {
		t.Fatalf("reservation not released after failed enqueue: released=%d", idemp.released)
	}
}

// Concurrent identical deliveries race the reservation: exactly one wins and
// enqueues, the rest replay it. Run under -race.
func TestAsyncDedupRace(t *testing.T) {
	fx := newDedupFixture(t, newFakeIdempotency())
	cmd := IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-1", State: deal(100)}

	const n = 16
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, _ = fx.async.State(authedCtx(), cmd)
		}()
	}
	wg.Wait()
	if fx.durable.count() != 1 {
		t.Fatalf("race enqueued %d times, want exactly 1", fx.durable.count())
	}
}

// The storage barrier (second rubicon): a repeat event_id that reaches the
// Ingester — the worker path — folds into a no-op rather than failing, leaving
// history with a single event (FR-1.6).
func TestIngesterStorageBarrierDeduplicates(t *testing.T) {
	fx := newIngestFixture(t)
	ctx := authedCtx()
	cmd := IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-1", State: deal(100)}

	if _, err := fx.ing.State(ctx, cmd); err != nil {
		t.Fatalf("first write: %v", err)
	}
	// A reclaimed duplicate re-applies against a now-empty diff via a fresh entity
	// to force the append (so the no-change short-circuit does not mask the test):
	// a different entity but the same event_id collides on the unique index.
	res, err := fx.ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-2", EventID: "evt-1", State: deal(200)})
	if err != nil {
		t.Fatalf("duplicate event_id must not error: %v", err)
	}
	if !res.Deduplicated {
		t.Fatalf("duplicate not reported: %+v", res)
	}
	if got := len(fx.events.byEntity["crm.deals|d-2"]); got != 0 {
		t.Fatalf("duplicate wrote an event: %d", got)
	}
}

// failQueue fails every Publish, to exercise the producer's failure path.
type failQueue struct{ stubQueue }

func (q *failQueue) Publish(context.Context, queue.Message) error {
	return queue.ErrQueueUnavailable
}
