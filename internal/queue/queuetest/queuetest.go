// Package queuetest holds the contract suite every queue.Queue implementation
// must satisfy. It lives in its own package (not a _test file) so both the
// memory unit test and the redis-streams integration test can run the exact
// same assertions against their backend.
package queuetest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/queue"
)

// RunContract exercises the behaviours the worker relies on against a freshly
// built queue. newQueue must return an empty, ready queue with at least two
// shards; cleanup (if any) is the caller's via t.Cleanup.
func RunContract(t *testing.T, newQueue func(t *testing.T) queue.Queue) {
	t.Run("publish then subscribe round-trips payload", func(t *testing.T) {
		q := newQueue(t)
		ctx := t.Context()

		msg := queue.Message{Key: "t|c|e1", Payload: []byte("hello"), Attrs: map[string]string{"event_id": "evt-1"}}
		if err := q.Publish(ctx, msg); err != nil {
			t.Fatalf("publish: %v", err)
		}
		d := recvOne(t, ctx, q, queue.Shard(msg.Key, q.Shards()))
		if string(d.Payload) != "hello" {
			t.Errorf("payload = %q, want hello", d.Payload)
		}
		if d.Key != msg.Key {
			t.Errorf("key = %q, want %q", d.Key, msg.Key)
		}
		if d.Attrs["event_id"] != "evt-1" {
			t.Errorf("attr event_id = %q, want evt-1", d.Attrs["event_id"])
		}
	})

	t.Run("order preserved within a key", func(t *testing.T) {
		q := newQueue(t)
		ctx := t.Context()

		const key = "t|c|same"
		shard := queue.Shard(key, q.Shards())
		const n = 20
		for i := range n {
			if err := q.Publish(ctx, queue.Message{Key: key, Payload: fmt.Appendf(nil, "%d", i)}); err != nil {
				t.Fatalf("publish %d: %v", i, err)
			}
		}
		sub, err := q.Subscribe(ctx, shard)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		for i := range n {
			d := recv(t, sub)
			if got := string(d.Payload); got != fmt.Sprint(i) {
				t.Fatalf("message %d out of order: got %q", i, got)
			}
			if err := q.Ack(ctx, d); err != nil {
				t.Fatalf("ack: %v", err)
			}
		}
	})

	t.Run("ack removes from pending, unacked is reclaimable", func(t *testing.T) {
		q := newQueue(t)
		ctx := t.Context()

		// Same key keeps both messages on one shard and one FIFO subscription,
		// so the test never races two consumers over a shard. We ack the first
		// and leave the second pending.
		const key = "t|c|pe"
		shard := queue.Shard(key, q.Shards())
		_ = q.Publish(ctx, queue.Message{Key: key, Payload: []byte("a")})
		_ = q.Publish(ctx, queue.Message{Key: key, Payload: []byte("k")})

		sub, err := q.Subscribe(ctx, shard)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		dAck := recv(t, sub)
		if err := q.Ack(ctx, dAck); err != nil {
			t.Fatalf("ack: %v", err)
		}
		dKeep := recv(t, sub)

		// Reclaim with minIdle=0 returns everything still pending. The acked one
		// must be gone; the kept one must come back.
		reclaimed := reclaimAll(t, ctx, q)
		if _, ok := reclaimed[dAck.ID]; ok {
			t.Errorf("acked message %s still pending", dAck.ID)
		}
		got, ok := reclaimed[dKeep.ID]
		if !ok {
			t.Fatalf("unacked message %s not reclaimable", dKeep.ID)
		}
		if string(got.Payload) != "k" {
			t.Errorf("reclaimed payload = %q, want k", got.Payload)
		}
		// Acking the reclaimed message clears it.
		if err := q.Ack(ctx, got); err != nil {
			t.Fatalf("ack reclaimed: %v", err)
		}
		if left := reclaimAll(t, ctx, q); len(left) != 0 {
			t.Errorf("pending after final ack = %d, want 0", len(left))
		}
	})

	t.Run("different keys may land on different shards", func(t *testing.T) {
		q := newQueue(t)
		// With enough distinct keys at least two shards are used; this guards the
		// hash from collapsing everything onto one shard.
		seen := map[int]bool{}
		for i := range 100 {
			seen[queue.Shard(fmt.Sprintf("t|c|e%d", i), q.Shards())] = true
		}
		if len(seen) < 2 {
			t.Errorf("100 keys hashed to %d shard(s), want >=2", len(seen))
		}
	})

	t.Run("close ends subscribe", func(t *testing.T) {
		q := newQueue(t)
		ctx := context.Background()
		sub, err := q.Subscribe(ctx, 0)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		if err := q.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		select {
		case _, ok := <-sub:
			if ok {
				// A buffered message draining first is fine; the channel must
				// still close eventually. Drain then re-check.
				select {
				case _, ok2 := <-sub:
					if ok2 {
						t.Error("subscribe channel did not close after Close")
					}
				case <-time.After(time.Second):
					t.Error("subscribe channel did not close after Close")
				}
			}
		case <-time.After(time.Second):
			t.Error("subscribe channel did not close after Close")
		}
	})
}

// recvOne subscribes to a shard and returns the first delivery.
func recvOne(t *testing.T, ctx context.Context, q queue.Queue, shard int) queue.Delivery {
	t.Helper()
	sub, err := q.Subscribe(ctx, shard)
	if err != nil {
		t.Fatalf("subscribe shard %d: %v", shard, err)
	}
	return recv(t, sub)
}

func recv(t *testing.T, sub <-chan queue.Delivery) queue.Delivery {
	t.Helper()
	select {
	case d, ok := <-sub:
		if !ok {
			t.Fatal("subscribe channel closed unexpectedly")
		}
		return d
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for delivery")
		return queue.Delivery{}
	}
}

// reclaimAll reclaims pending messages across every shard, keyed by id.
func reclaimAll(t *testing.T, ctx context.Context, q queue.Queue) map[string]queue.Delivery {
	t.Helper()
	out := map[string]queue.Delivery{}
	for s := 0; s < q.Shards(); s++ {
		ds, err := q.Reclaim(ctx, s, 0)
		if err != nil {
			t.Fatalf("reclaim shard %d: %v", s, err)
		}
		for _, d := range ds {
			out[d.ID] = d
		}
	}
	return out
}
