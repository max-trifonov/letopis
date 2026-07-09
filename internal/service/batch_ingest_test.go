package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

// denyLimiter is a QueueLimiter that always refuses, for the backpressure test.
type denyLimiter struct{ retry time.Duration }

func (d denyLimiter) Allow(domain.ReliabilityMode) (bool, time.Duration) { return false, d.retry }

// failOpenTickets is a TicketStore whose Create fails, to prove the receipt is a
// best-effort side effect.
type failOpenTickets struct{}

func (failOpenTickets) Create(context.Context, *domain.Ticket) error {
	return errors.New("redis down")
}

func (failOpenTickets) Get(context.Context, string) (*domain.Ticket, error) {
	return nil, domain.ErrTicketNotFound
}

func (failOpenTickets) Save(context.Context, *domain.Ticket) error { return nil }

// newBatchFixture wraps the async fixture with a BatchIngester sharing its ticket
// store, so a test can assert both the accept outcome and the receipt.
func newBatchFixture(t *testing.T) (*BatchIngester, asyncFixture) {
	t.Helper()
	fx := newAsyncFixture(t)
	b := NewBatchIngester(fx.async, NewTicketService(fx.tickets), nil)
	return b, fx
}

func stateEntry(index int, collection, entityID string, amount float64) BatchEntry {
	return BatchEntry{
		Index:   index,
		Kind:    KindState,
		Command: IngestCommand{Collection: collection, EntityID: entityID, State: deal(amount)},
	}
}

// A batch of valid durable items: every one enqueues, the receipt is accepted,
// nothing is written synchronously.
func TestBatchAllAccepted(t *testing.T) {
	b, fx := newBatchFixture(t)
	entries := []BatchEntry{
		stateEntry(0, "crm.deals", "d-1", 100),
		stateEntry(1, "crm.deals", "d-2", 200),
		stateEntry(2, "crm.tasks", "t-1", 1),
	}
	res, err := b.Ingest(authedCtx(), entries, nil, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Accepted != 3 || len(res.Rejected) != 0 {
		t.Fatalf("accepted=%d rejected=%d, want 3/0", res.Accepted, len(res.Rejected))
	}
	if fx.durable.count() != 3 {
		t.Fatalf("durable enqueue count = %d, want 3", fx.durable.count())
	}
	tk, err := fx.tickets.Get(authedCtx(), res.Ticket)
	if err != nil || tk.Status != domain.TicketAccepted {
		t.Fatalf("receipt = %+v err=%v, want accepted", tk, err)
	}
}

// A mix of transport rejects and a valid item: the valid one is accepted, the
// prior rejects flow through, and the receipt is partial. Rejects come back in
// index order.
func TestBatchPartialReceipt(t *testing.T) {
	b, fx := newBatchFixture(t)
	prior := []BatchReject{
		{Index: 2, Code: "invalid_type", Message: "bad"},
		{Index: 0, Code: "invalid_entity_id", Message: "bad"},
	}
	res, err := b.Ingest(authedCtx(), []BatchEntry{stateEntry(1, "crm.deals", "d-1", 100)}, prior, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Accepted != 1 || len(res.Rejected) != 2 {
		t.Fatalf("accepted=%d rejected=%d, want 1/2", res.Accepted, len(res.Rejected))
	}
	if res.Rejected[0].Index != 0 || res.Rejected[1].Index != 2 {
		t.Fatalf("rejects not sorted by index: %+v", res.Rejected)
	}
	tk, _ := fx.tickets.Get(authedCtx(), res.Ticket)
	if tk.Status != domain.TicketPartial {
		t.Fatalf("receipt status = %s, want partial", tk.Status)
	}
}

// An item larger than the collection's max_event_size is rejected before it is
// enqueued (FR-1.9).
func TestBatchItemTooLarge(t *testing.T) {
	b, fx := newBatchFixture(t)
	fx.async.sync.cfg.repo.(*fakeRepo).configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals", MaxEventSizeBytes: 10}
	e := stateEntry(0, "crm.deals", "d-1", 100)
	e.BodySize = 100
	res, err := b.Ingest(authedCtx(), []BatchEntry{e}, nil, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Accepted != 0 || len(res.Rejected) != 1 || res.Rejected[0].Code != batchRejectTooLarge {
		t.Fatalf("want one too_large reject, got accepted=%d rejected=%+v", res.Accepted, res.Rejected)
	}
	if fx.durable.count() != 0 {
		t.Fatalf("oversized item was enqueued: %d", fx.durable.count())
	}
}

// A strict collection writes batch items synchronously and independently — no
// enqueue, a real event each.
func TestBatchStrictWritesSynchronously(t *testing.T) {
	b, fx := newBatchFixture(t)
	fx.async.sync.cfg.repo.(*fakeRepo).configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityStrict}
	res, err := b.Ingest(authedCtx(), []BatchEntry{stateEntry(0, "crm.deals", "d-1", 100)}, nil, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Accepted != 1 || fx.durable.count() != 0 {
		t.Fatalf("strict batch should write sync: accepted=%d enqueued=%d", res.Accepted, fx.durable.count())
	}
	if got := len(fx.events.byEntity["crm.deals|d-1"]); got != 1 {
		t.Fatalf("strict batch wrote %d events, want 1", got)
	}
}

// A strict item carrying a diff that does not apply is rejected with a typed
// code, on its own — the rest of the batch is unaffected.
func TestBatchStrictInvalidDiffRejected(t *testing.T) {
	b, fx := newBatchFixture(t)
	fx.async.sync.cfg.repo.(*fakeRepo).configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityStrict}
	// A remove on a path that does not exist in the (empty) base cannot apply.
	bad := BatchEntry{
		Index:   0,
		Kind:    KindDiff,
		Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", Op: domain.OpUpdate, Changes: []diff.Change{{Path: "missing", Op: diff.OpRemove, Old: "x"}}},
	}
	res, err := b.Ingest(authedCtx(), []BatchEntry{bad}, nil, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if len(res.Rejected) != 1 || res.Rejected[0].Code != batchRejectInvalidDiff {
		t.Fatalf("want invalid_diff reject, got %+v", res.Rejected)
	}
}

// Backpressure refuses the whole batch up front (NFR-2.3): a batch is one
// admission decision, so nothing is accepted or enqueued.
func TestBatchBackpressureRefusesWholeBatch(t *testing.T) {
	fx := newAsyncFixture(t)
	fx.async.limiter = denyLimiter{retry: 2 * time.Second}
	b := NewBatchIngester(fx.async, NewTicketService(fx.tickets), nil)

	_, err := b.Ingest(authedCtx(), []BatchEntry{stateEntry(0, "crm.deals", "d-1", 100)}, nil, "")
	var bp *BackpressureError
	if !errors.As(err, &bp) {
		t.Fatalf("err = %v, want *BackpressureError", err)
	}
	if fx.durable.count() != 0 {
		t.Fatalf("refused batch still enqueued: %d", fx.durable.count())
	}
}

// A failed receipt open must not fail an accept whose items already shipped.
func TestBatchReceiptOpenFailureSwallowed(t *testing.T) {
	fx := newAsyncFixture(t)
	b := NewBatchIngester(fx.async, NewTicketService(failOpenTickets{}), nil)
	res, err := b.Ingest(authedCtx(), []BatchEntry{stateEntry(0, "crm.deals", "d-1", 100)}, nil, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Accepted != 1 || fx.durable.count() != 1 {
		t.Fatalf("accept should stand despite receipt failure: accepted=%d enqueued=%d", res.Accepted, fx.durable.count())
	}
}

// A nil ticket service (no Redis) still accepts and returns a receipt id.
func TestBatchNoTicketStore(t *testing.T) {
	fx := newAsyncFixture(t)
	b := NewBatchIngester(fx.async, nil, nil)
	res, err := b.Ingest(authedCtx(), []BatchEntry{stateEntry(0, "crm.deals", "d-1", 100)}, nil, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Accepted != 1 || res.Ticket == "" {
		t.Fatalf("want accepted with a receipt id, got %+v", res)
	}
}
