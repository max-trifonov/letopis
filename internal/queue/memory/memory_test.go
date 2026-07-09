package memory

import (
	"context"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/queue/queuetest"
)

func TestMemoryContract(t *testing.T) {
	queuetest.RunContract(t, func(t *testing.T) queue.Queue {
		q := New(4)
		t.Cleanup(func() { _ = q.Close() })
		return q
	})
}

// Depth reflects buffered-but-unconsumed plus delivered-but-unacked messages —
// the backlog backpressure trips on (S2-06).
func TestMemoryDepth(t *testing.T) {
	q := New(2)
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()

	if d, _ := q.Depth(ctx); d != 0 {
		t.Fatalf("empty depth = %d, want 0", d)
	}
	for i := range 3 {
		if err := q.Publish(ctx, queue.Message{Key: "k", Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}
	if d, _ := q.Depth(ctx); d != 3 {
		t.Fatalf("depth after 3 publishes = %d, want 3", d)
	}
}

// TestReclaimCountsRedeliveries is memory-specific: the in-memory backend keeps
// an exact delivery count (Redis surfaces this only via XPENDING, deferred to
// S2-06), so we assert it here where the contract suite stays lenient.
func TestReclaimCountsRedeliveries(t *testing.T) {
	q := New(1)
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()

	if err := q.Publish(ctx, queue.Message{Key: "k", Payload: []byte("v")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	sub, err := q.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	first := <-sub
	if first.Deliveries != 1 {
		t.Fatalf("first delivery count = %d, want 1", first.Deliveries)
	}

	// Left unacked, it ages into reclaimable with an incremented count.
	ds, err := q.Reclaim(ctx, 0, 0)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(ds) != 1 {
		t.Fatalf("reclaimed %d, want 1", len(ds))
	}
	if ds[0].Deliveries != 2 {
		t.Errorf("reclaimed delivery count = %d, want 2", ds[0].Deliveries)
	}
}

func TestPublishAfterCloseFails(t *testing.T) {
	q := New(2)
	_ = q.Close()
	err := q.Publish(context.Background(), queue.Message{Key: "k"})
	if err != queue.ErrClosed {
		t.Errorf("publish after close = %v, want ErrClosed", err)
	}
}

func TestMinIdleHoldsBackFreshDeliveries(t *testing.T) {
	q := New(1)
	t.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()
	_ = q.Publish(ctx, queue.Message{Key: "k", Payload: []byte("v")})
	sub, _ := q.Subscribe(ctx, 0)
	<-sub

	// Just delivered: a non-zero min-idle must not reclaim it yet.
	ds, _ := q.Reclaim(ctx, 0, time.Hour)
	if len(ds) != 0 {
		t.Errorf("reclaimed %d fresh deliveries, want 0", len(ds))
	}
}

func BenchmarkPublish(b *testing.B) {
	q := New(16)
	b.Cleanup(func() { _ = q.Close() })
	ctx := context.Background()
	// Drain so the buffer never blocks the publisher under benchmark.
	for s := range q.Shards() {
		sub, _ := q.Subscribe(ctx, s)
		go func() {
			for range sub {
			}
		}()
	}
	msg := queue.Message{Key: "tenant|crm.deals|d-1", Payload: []byte(`{"v":1}`)}
	for b.Loop() {
		if err := q.Publish(ctx, msg); err != nil {
			b.Fatal(err)
		}
	}
}
