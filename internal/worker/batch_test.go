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
)

// stubBatch records every IngestBatch call and can fail named entities to drive
// the failed-ticket path.
type stubBatch struct {
	mu      sync.Mutex
	batches [][]service.BatchItem
	fail    map[string]bool
}

func (s *stubBatch) IngestBatch(_ context.Context, items []service.BatchItem) []error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := append([]service.BatchItem(nil), items...)
	s.batches = append(s.batches, cp)
	errs := make([]error, len(items))
	for i, it := range items {
		if s.fail[it.Command.EntityID] {
			errs[i] = errors.New("stub: write failed")
		}
	}
	return errs
}

func (s *stubBatch) snapshot() [][]service.BatchItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]service.BatchItem(nil), s.batches...)
}

func (s *stubBatch) totalItems() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, b := range s.batches {
		n += len(b)
	}
	return n
}

// stubTickets records the last status set per ticket id.
type stubTickets struct {
	mu     sync.Mutex
	status map[string]domain.TicketStatus
}

func newStubTickets() *stubTickets { return &stubTickets{status: map[string]domain.TicketStatus{}} }

func (s *stubTickets) Mark(_ context.Context, id string, status domain.TicketStatus, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[id] = status
	return nil
}

func (s *stubTickets) get(id string) domain.TicketStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status[id]
}

func testBatchWorker(q queue.Queue, ing BatchIngester, opts BatchOptions) *BatchWorker {
	return NewBatchWorker(q, ing, slog.New(slog.NewTextHandler(io.Discard, nil)), opts)
}

func fastTask(entity string, ticket string, seq int) service.Task {
	return service.Task{
		Tenant:   service.TenantRef{ID: "acme"},
		Kind:     service.KindState,
		Mode:     domain.ReliabilityFast,
		TicketID: ticket,
		Command:  service.IngestCommand{Collection: "crm.deals", EntityID: entity, Author: strconv.Itoa(seq)},
	}
}

// A full batch flushes on size, before the linger deadline.
func TestBatchFlushesOnSize(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubBatch{}
	w := testBatchWorker(q, ing, BatchOptions{Size: 3, Linger: time.Hour})
	ctx := t.Context()
	go func() { _ = w.Run(ctx) }()

	for i := range 3 {
		_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|d-1", Payload: mustEncode(t, fastTask("d-1", "", i))})
	}
	waitFor(t, 2*time.Second, func() bool { return ing.totalItems() == 3 })
	if got := len(ing.snapshot()); got != 1 {
		t.Fatalf("flushed in %d batches, want 1 (size-triggered)", got)
	}
}

// A partial batch flushes on the linger deadline.
func TestBatchFlushesOnLinger(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubBatch{}
	w := testBatchWorker(q, ing, BatchOptions{Size: 100, Linger: 30 * time.Millisecond})
	ctx := t.Context()
	go func() { _ = w.Run(ctx) }()

	_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|d-1", Payload: mustEncode(t, fastTask("d-1", "", 0))})
	waitFor(t, 2*time.Second, func() bool { return ing.totalItems() == 1 })
}

// Order within a shard is preserved across the fold.
func TestBatchPreservesOrder(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubBatch{}
	w := testBatchWorker(q, ing, BatchOptions{Size: 10, Linger: 20 * time.Millisecond})
	ctx := t.Context()
	go func() { _ = w.Run(ctx) }()

	const n = 10
	for i := range n {
		_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|d-1", Payload: mustEncode(t, fastTask("d-1", "", i))})
	}
	waitFor(t, 2*time.Second, func() bool { return ing.totalItems() == n })

	var seqs []string
	for _, b := range ing.snapshot() {
		for _, it := range b {
			seqs = append(seqs, it.Command.Author)
		}
	}
	for i, s := range seqs {
		if s != strconv.Itoa(i) {
			t.Fatalf("item %d out of order: author=%q", i, s)
		}
	}
}

// Tickets settle directly to stored on success and failed on a write error.
func TestBatchSettlesTickets(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubBatch{fail: map[string]bool{"bad": true}}
	tickets := newStubTickets()
	w := testBatchWorker(q, ing, BatchOptions{Size: 2, Linger: time.Hour, Tickets: tickets})
	ctx := t.Context()
	go func() { _ = w.Run(ctx) }()

	_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|ok", Payload: mustEncode(t, fastTask("ok", "tkt-ok", 0))})
	_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|bad", Payload: mustEncode(t, fastTask("bad", "tkt-bad", 1))})

	waitFor(t, 2*time.Second, func() bool {
		return tickets.get("tkt-ok") == domain.TicketStored && tickets.get("tkt-bad") == domain.TicketFailed
	})
}

// On shutdown the buffered tail is flushed rather than dropped.
func TestBatchFlushesOnShutdown(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubBatch{}
	w := testBatchWorker(q, ing, BatchOptions{Size: 100, Linger: time.Hour})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = w.Run(ctx); close(done) }()

	_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|d-1", Payload: mustEncode(t, fastTask("d-1", "", 0))})
	// Give the loop a moment to buffer the delivery, then stop it.
	waitFor(t, 2*time.Second, func() bool { return q.Shards() == 1 }) // settle goroutine start
	time.Sleep(30 * time.Millisecond)
	cancel()
	<-done

	if ing.totalItems() != 1 {
		t.Fatalf("shutdown dropped buffered items: total=%d", ing.totalItems())
	}
}

// After a flush nothing stays pending: fast acks regardless of outcome.
func TestBatchAcksEverything(t *testing.T) {
	q := memory.New(1)
	t.Cleanup(func() { _ = q.Close() })
	ing := &stubBatch{fail: map[string]bool{"bad": true}}
	w := testBatchWorker(q, ing, BatchOptions{Size: 1, Linger: time.Hour})
	ctx := t.Context()
	go func() { _ = w.Run(ctx) }()

	_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|bad", Payload: mustEncode(t, fastTask("bad", "", 0))})
	waitFor(t, 2*time.Second, func() bool { return ing.totalItems() == 1 })
	waitFor(t, 2*time.Second, func() bool {
		left, _ := q.Reclaim(ctx, 0, 0)
		return len(left) == 0
	})
}
