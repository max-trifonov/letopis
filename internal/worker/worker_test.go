package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/queue/memory"
	"github.com/max-trifonov/letopis/internal/service"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// stubIngester records the commands it is asked to apply and can be told to
// fail the first N calls, to exercise the no-ack/reclaim path.
type stubIngester struct {
	mu        sync.Mutex
	recorded  []record
	failFirst int
}

type record struct {
	kind     service.Kind
	cmd      service.IngestCommand
	tenantID string
}

func (s *stubIngester) apply(kind service.Kind, ctx context.Context, cmd service.IngestCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, _ := tenant.FromContext(ctx)
	n := len(s.recorded)
	s.recorded = append(s.recorded, record{kind: kind, cmd: cmd, tenantID: p.Tenant.ID})
	if n < s.failFirst {
		return errors.New("stub: transient failure")
	}
	return nil
}

func (s *stubIngester) State(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error) {
	return service.IngestResult{}, s.apply(service.KindState, ctx, cmd)
}

func (s *stubIngester) Diff(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error) {
	return service.IngestResult{}, s.apply(service.KindDiff, ctx, cmd)
}

func (s *stubIngester) Delete(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error) {
	return service.IngestResult{}, s.apply(service.KindDelete, ctx, cmd)
}

func (s *stubIngester) snapshot() []record {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]record(nil), s.recorded...)
}

func testWorker(q queue.Queue, ing Ingester) *Worker {
	return New(q, ing, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		ReclaimInterval: 20 * time.Millisecond,
		ReclaimMinIdle:  time.Millisecond,
	})
}

func mustEncode(t *testing.T, task service.Task) []byte {
	t.Helper()
	b, err := service.Encode(task)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

// recvOne subscribes and returns the first delivery, for the handle-level tests
// that drive a single message synchronously.
func recvOne(t *testing.T, ctx context.Context, q queue.Queue, shard int) queue.Delivery {
	t.Helper()
	sub, err := q.Subscribe(ctx, shard)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case d := <-sub:
		return d
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for delivery")
		return queue.Delivery{}
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	task := service.Task{
		Tenant: service.TenantRef{ID: "acme", DBName: "hm_t_acme"},
		Kind:   service.KindState,
		Command: service.IngestCommand{
			Collection: "crm.deals",
			EntityID:   "d-1",
			State:      map[string]any{"amount": float64(250)},
		},
	}
	got, err := service.Decode(mustEncode(t, task))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Tenant.ID != "acme" || got.Kind != service.KindState {
		t.Errorf("round-trip header = %+v", got)
	}
	if got.Command.EntityID != "d-1" || got.Command.State["amount"] != float64(250) {
		t.Errorf("round-trip command = %+v", got.Command)
	}
}

func TestHandleAcksOnSuccess(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubIngester{}
	w := testWorker(q, ing)
	ctx := t.Context()

	task := service.Task{Tenant: service.TenantRef{ID: "acme"}, Kind: service.KindState, Command: service.IngestCommand{Collection: "c", EntityID: "e"}}
	if err := q.Publish(ctx, queue.Message{Key: "acme|c|e", Payload: mustEncode(t, task)}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	d := recvOne(t, ctx, q, queue.Shard("acme|c|e", 1))

	w.handle(ctx, d)

	if got := ing.snapshot(); len(got) != 1 || got[0].kind != service.KindState || got[0].tenantID != "acme" {
		t.Fatalf("ingester calls = %+v", got)
	}
	// Acked: nothing left pending.
	left, _ := q.Reclaim(ctx, d.Shard, 0)
	if len(left) != 0 {
		t.Errorf("pending after success = %d, want 0 (should be acked)", len(left))
	}
}

func TestHandleLeavesUnackedOnError(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubIngester{failFirst: 1}
	w := testWorker(q, ing)
	ctx := t.Context()

	task := service.Task{Tenant: service.TenantRef{ID: "acme"}, Kind: service.KindDiff, Command: service.IngestCommand{Collection: "c", EntityID: "e"}}
	_ = q.Publish(ctx, queue.Message{Key: "acme|c|e", Payload: mustEncode(t, task)})
	d := recvOne(t, ctx, q, 0)

	w.handle(ctx, d)

	// Not acked: still reclaimable for a retry.
	left, _ := q.Reclaim(ctx, 0, 0)
	if len(left) != 1 {
		t.Fatalf("pending after error = %d, want 1 (should not be acked)", len(left))
	}
}

func TestHandleDropsPoison(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubIngester{}
	w := testWorker(q, ing)
	ctx := t.Context()

	// A payload that is not a valid envelope can never succeed.
	_ = q.Publish(ctx, queue.Message{Key: "acme|c|e", Payload: []byte("not json")})
	d := recvOne(t, ctx, q, 0)

	w.handle(ctx, d)

	if got := ing.snapshot(); len(got) != 0 {
		t.Errorf("poison reached ingester: %+v", got)
	}
	// Acked away so it does not loop forever.
	left, _ := q.Reclaim(ctx, 0, 0)
	if len(left) != 0 {
		t.Errorf("poison still pending = %d, want 0 (should be dropped)", len(left))
	}
}

func TestRunProcessesAcrossShards(t *testing.T) {
	q := memory.New(8)
	ing := &stubIngester{}
	w := testWorker(q, ing)
	ctx := t.Context()

	go func() { _ = w.Run(ctx) }()

	const n = 50
	for i := range n {
		key := keyFor(i)
		task := service.Task{Tenant: service.TenantRef{ID: "acme"}, Kind: service.KindState, Command: service.IngestCommand{Collection: "c", EntityID: key}}
		if err := q.Publish(ctx, queue.Message{Key: key, Payload: mustEncode(t, task)}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	waitFor(t, 3*time.Second, func() bool { return len(ing.snapshot()) == n })
}

func TestRunPreservesOrderWithinShard(t *testing.T) {
	q := memory.New(4)
	ing := &stubIngester{}
	w := testWorker(q, ing)
	ctx := t.Context()

	go func() { _ = w.Run(ctx) }()

	const (
		key = "acme|c|same-entity"
		n   = 30
	)
	for v := range n {
		task := service.Task{Tenant: service.TenantRef{ID: "acme"}, Kind: service.KindState, Command: service.IngestCommand{Collection: "c", EntityID: "same-entity", Author: strconv.Itoa(v)}}
		_ = q.Publish(ctx, queue.Message{Key: key, Payload: mustEncode(t, task)})
	}

	waitFor(t, 3*time.Second, func() bool { return len(ing.snapshot()) == n })
	for i, r := range ing.snapshot() {
		if r.cmd.Author != strconv.Itoa(i) {
			t.Fatalf("message %d out of order: author=%q", i, r.cmd.Author)
		}
	}
}

func TestRunReclaimsAndRetries(t *testing.T) {
	q := memory.New(1)
	ing := &stubIngester{failFirst: 1} // first attempt fails, retry succeeds
	w := testWorker(q, ing)
	ctx := t.Context()

	go func() { _ = w.Run(ctx) }()

	task := service.Task{Tenant: service.TenantRef{ID: "acme"}, Kind: service.KindState, Command: service.IngestCommand{Collection: "c", EntityID: "e"}}
	_ = q.Publish(ctx, queue.Message{Key: "acme|c|e", Payload: mustEncode(t, task)})

	// Two attempts: the failed first, then the reclaimed retry.
	waitFor(t, 3*time.Second, func() bool { return len(ing.snapshot()) >= 2 })
}

// errIngester always fails with a fixed error, for the ticket-failure paths.
type errIngester struct{ err error }

func (e errIngester) State(context.Context, service.IngestCommand) (service.IngestResult, error) {
	return service.IngestResult{}, e.err
}

func (e errIngester) Diff(context.Context, service.IngestCommand) (service.IngestResult, error) {
	return service.IngestResult{}, e.err
}

func (e errIngester) Delete(context.Context, service.IngestCommand) (service.IngestResult, error) {
	return service.IngestResult{}, e.err
}

func ticketWorker(q queue.Queue, ing Ingester, tickets TicketUpdater) *Worker {
	return New(q, ing, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		ReclaimInterval: 20 * time.Millisecond,
		ReclaimMinIdle:  time.Millisecond,
		Tickets:         tickets,
	})
}

// A successful durable write drives the ticket processing→stored.
func TestDurableTicketStored(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	tickets := newStubTickets()
	w := ticketWorker(q, &stubIngester{}, tickets)
	ctx := t.Context()

	task := service.Task{Tenant: service.TenantRef{ID: "acme"}, Kind: service.KindState, TicketID: "t1", Command: service.IngestCommand{Collection: "c", EntityID: "e"}}
	_ = q.Publish(ctx, queue.Message{Key: "acme|c|e", Payload: mustEncode(t, task)})
	w.handle(ctx, recvOne(t, ctx, q, 0))

	if got := tickets.get("t1"); got != domain.TicketStored {
		t.Fatalf("ticket = %s, want stored", got)
	}
}

// An unapplyable diff is permanent: the ticket fails and the message is acked
// away rather than reclaimed forever.
func TestDurableTicketFailedOnBadDiff(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	tickets := newStubTickets()
	w := ticketWorker(q, errIngester{err: service.ErrInvalidDiff}, tickets)
	ctx := t.Context()

	task := service.Task{Tenant: service.TenantRef{ID: "acme"}, Kind: service.KindDiff, TicketID: "t1", Command: service.IngestCommand{Collection: "c", EntityID: "e"}}
	_ = q.Publish(ctx, queue.Message{Key: "acme|c|e", Payload: mustEncode(t, task)})
	w.handle(ctx, recvOne(t, ctx, q, 0))

	if got := tickets.get("t1"); got != domain.TicketFailed {
		t.Fatalf("ticket = %s, want failed", got)
	}
	left, _ := q.Reclaim(ctx, 0, 0)
	if len(left) != 0 {
		t.Fatalf("bad-diff message still pending = %d, want 0 (permanent → dropped)", len(left))
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func keyFor(i int) string { return "acme|c|e-" + strconv.Itoa(i) }
