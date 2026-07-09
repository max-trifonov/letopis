//go:build integration

// These tests need a real Redis and Docker; they run under
// `go test -tags integration ./...`. They cover what unit tests cannot: the
// actual XADD/XREADGROUP/XACK/XAUTOCLAIM round-trip, that unacked messages
// survive a consumer "restart", and that an unreachable Redis surfaces as
// ErrQueueUnavailable.
package redisstreams

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/queue/queuetest"
)

func testClient(t *testing.T) redis.UniversalClient {
	t.Helper()
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })

	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	opt, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func TestIntegrationContract(t *testing.T) {
	rdb := testClient(t)
	var n int
	queuetest.RunContract(t, func(t *testing.T) queue.Queue {
		// A unique prefix per sub-test isolates streams on the shared instance.
		n++
		q := New(rdb, 4, fmt.Sprintf("test:ingest:%d", n), "workers")
		t.Cleanup(func() { _ = q.Close() })
		return q
	})
}

func TestIntegrationPendingSurvivesRestart(t *testing.T) {
	rdb := testClient(t)
	ctx := context.Background()
	const (
		prefix = "test:restart"
		group  = "workers"
		key    = "t|c|e1"
	)
	shard := queue.Shard(key, 4)

	// First consumer reads but never acks — the process "crashes".
	q1 := New(rdb, 4, prefix, group)
	if err := q1.Publish(ctx, queue.Message{Key: key, Payload: []byte("payload")}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	sub, err := q1.Subscribe(ctx, shard)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case d := <-sub:
		if string(d.Payload) != "payload" {
			t.Fatalf("payload = %q", d.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}
	_ = q1.Close() // stop the loop without acking

	// A fresh consumer (new name, same group) reclaims the stuck message.
	q2 := New(rdb, 4, prefix, group)
	t.Cleanup(func() { _ = q2.Close() })
	reclaimed, err := q2.Reclaim(ctx, shard, 0)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 || string(reclaimed[0].Payload) != "payload" {
		t.Fatalf("reclaimed = %+v, want one message with payload", reclaimed)
	}
	if err := q2.Ack(ctx, reclaimed[0]); err != nil {
		t.Fatalf("ack reclaimed: %v", err)
	}
	// Nothing left pending.
	again, err := q2.Reclaim(ctx, shard, 0)
	if err != nil {
		t.Fatalf("reclaim again: %v", err)
	}
	if len(again) != 0 {
		t.Errorf("still %d pending after ack, want 0", len(again))
	}
}

func TestIntegrationUnavailableRedis(t *testing.T) {
	// Nothing is listening here, with a short dial timeout so the test is quick.
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 200 * time.Millisecond})
	t.Cleanup(func() { _ = rdb.Close() })
	q := New(rdb, 4, "test:down", "workers")

	err := q.Publish(context.Background(), queue.Message{Key: "k", Payload: []byte("v")})
	if !errors.Is(err, queue.ErrQueueUnavailable) {
		t.Errorf("publish error = %v, want ErrQueueUnavailable", err)
	}
}

// Depth reflects the unprocessed backlog: it rises as messages are published and
// falls as they are consumed and acked, staying accurate even though acked
// entries are never trimmed from the stream (S2-06).
func TestIntegrationDepth(t *testing.T) {
	rdb := testClient(t)
	ctx := context.Background()
	const (
		prefix = "test:depth"
		group  = "workers"
		key    = "t|c|e1"
	)
	shard := queue.Shard(key, 4)
	q := New(rdb, 4, prefix, group)
	t.Cleanup(func() { _ = q.Close() })

	// Subscribe first so the consumer group exists, then publish a backlog.
	sub, err := q.Subscribe(ctx, shard)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	const n = 5
	for i := range n {
		if err := q.Publish(ctx, queue.Message{Key: key, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("publish: %v", err)
		}
	}

	// Consume and ack one; the backlog (lag + pending) should net to n-1.
	select {
	case d := <-sub:
		if err := q.Ack(ctx, d); err != nil {
			t.Fatalf("ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no delivery")
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		depth, err := q.Depth(ctx)
		if err != nil {
			t.Fatalf("depth: %v", err)
		}
		if depth == n-1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("depth = %d, want %d", depth, n-1)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
