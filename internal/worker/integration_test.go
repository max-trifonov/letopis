//go:build integration

// These tests need real Redis and MongoDB plus Docker; they run under
// `go test -tags integration ./...`. They cover the at-least-once contract the
// loop exists for: a crash before ack leaves work that a second consumer
// reclaims and finishes without writing a duplicate, and a graceful shutdown
// loses nothing.
package worker

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/queue/memory"
	"github.com/max-trifonov/letopis/internal/queue/redisstreams"
	"github.com/max-trifonov/letopis/internal/service"
	storage "github.com/max-trifonov/letopis/internal/storage/mongo"
	"github.com/max-trifonov/letopis/internal/tenant"
)

const testTenant = "acme"

func newIngester(t *testing.T) (*service.Ingester, *storage.EventRepo) {
	t.Helper()
	ctx := context.Background()
	container, err := tcmongo.Run(ctx, "mongo:7")
	if err != nil {
		t.Fatalf("start mongo: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("mongo uri: %v", err)
	}
	conn, err := storage.NewConnManager(uri)
	if err != nil {
		t.Fatalf("conn manager: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	eventRepo := storage.NewEventRepo(conn)
	cfg := service.NewConfigResolver(storage.NewCollectionRepo(conn), service.Options{AutoCreate: true})
	ing := service.NewIngester(cfg, eventRepo, storage.NewCurrentRepo(conn))
	return ing, eventRepo
}

func newRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("redis uri: %v", err)
	}
	opt, err := redis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	rdb := redis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func authCtx() context.Context {
	return tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: testTenant}})
}

func stateTask(entity string, amount float64) service.Task {
	return service.Task{
		Tenant: service.TenantRef{ID: testTenant},
		Kind:   service.KindState,
		Command: service.IngestCommand{
			Collection: "crm.deals",
			EntityID:   entity,
			State:      map[string]any{"amount": amount},
		},
	}
}

func countEvents(t *testing.T, repo *storage.EventRepo, entity string) int {
	t.Helper()
	evs, err := repo.ListEvents(authCtx(), "crm.deals", domain.EventFilter{EntityID: entity})
	if err != nil {
		t.Fatalf("list events: %v", err)
	}
	return len(evs)
}

// TestIntegrationLoopWritesAndStopsCleanly drives a task end-to-end through the
// loop and then shuts it down by cancelling the context.
func TestIntegrationLoopWritesAndStopsCleanly(t *testing.T) {
	ing, eventRepo := newIngester(t)
	q := redisstreams.New(newRedis(t), 1, "test:e2e", "workers")
	w := New(q, ing, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		ReclaimInterval: 100 * time.Millisecond,
		ReclaimMinIdle:  time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	payload, err := service.Encode(stateTask("d-1", 100))
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := q.Publish(ctx, queue.Message{Key: "acme|crm.deals|d-1", Payload: payload}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool { return countEvents(t, eventRepo, "d-1") == 1 })

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v on graceful shutdown, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
	if got := countEvents(t, eventRepo, "d-1"); got != 1 {
		t.Errorf("events after shutdown = %d, want 1", got)
	}
}

// TestIntegrationReclaimNoDuplicate simulates a worker that writes then crashes
// before acking. A second consumer reclaims the message and reprocesses it; the
// write must not duplicate — a full-state re-apply is a no-op and the unique
// {eid,v} index is the final guard (event_id dedup is S2-05).
func TestIntegrationReclaimNoDuplicate(t *testing.T) {
	ing, eventRepo := newIngester(t)
	rdb := newRedis(t)
	ctx := context.Background()

	const (
		prefix = "test:reclaim"
		group  = "workers"
		key    = "acme|crm.deals|d-1"
	)
	task := stateTask("d-1", 100)
	payload, err := service.Encode(task)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Consumer 1 reads and writes, but "crashes" before ack.
	q1 := redisstreams.New(rdb, 1, prefix, group)
	if err := q1.Publish(ctx, queue.Message{Key: key, Payload: payload}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	sub, err := q1.Subscribe(ctx, 0)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case d := <-sub:
		if _, err := ing.State(authCtx(), mustDecode(t, d.Payload).Command); err != nil {
			t.Fatalf("first apply: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first delivery")
	}
	_ = q1.Close() // crash before ack
	if got := countEvents(t, eventRepo, "d-1"); got != 1 {
		t.Fatalf("events after first write = %d, want 1", got)
	}

	// Consumer 2 reclaims the still-pending message and reprocesses it.
	q2 := redisstreams.New(rdb, 1, prefix, group)
	t.Cleanup(func() { _ = q2.Close() })
	reclaimed, err := q2.Reclaim(ctx, 0, 0)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if len(reclaimed) != 1 {
		t.Fatalf("reclaimed %d, want 1", len(reclaimed))
	}
	if _, err := ing.State(authCtx(), mustDecode(t, reclaimed[0].Payload).Command); err != nil {
		t.Fatalf("reprocess: %v", err)
	}
	if err := q2.Ack(ctx, reclaimed[0]); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if got := countEvents(t, eventRepo, "d-1"); got != 1 {
		t.Errorf("events after reclaim+reprocess = %d, want 1 (no duplicate)", got)
	}
}

func mustDecode(t *testing.T, b []byte) service.Task {
	t.Helper()
	task, err := service.Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	return task
}

// TestIntegrationFastBatchWritesToMongo drives the fast path end-to-end: many
// tasks land on the in-memory queue, the batch worker folds them into
// insertMany writes, and every event reaches Mongo with a correct per-entity
// version sequence.
func TestIntegrationFastBatchWritesToMongo(t *testing.T) {
	ing, eventRepo := newIngester(t)
	q := memory.New(4)
	bw := NewBatchWorker(q, ing, slog.New(slog.NewTextHandler(io.Discard, nil)), BatchOptions{
		Size:   50,
		Linger: 20 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = bw.Run(ctx) }()

	// Three versions for one entity (must sequence) plus a spread of others.
	for v := range 3 {
		_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|d-1", Payload: mustEncodeI(t, stateTask("d-1", float64(v)))})
	}
	for i := 1; i <= 20; i++ {
		eid := "e-" + strconv.Itoa(i)
		_ = q.Publish(ctx, queue.Message{Key: "acme|crm.deals|" + eid, Payload: mustEncodeI(t, stateTask(eid, 1))})
	}

	waitFor(t, 10*time.Second, func() bool { return countEvents(t, eventRepo, "d-1") == 3 })
	for i := 1; i <= 20; i++ {
		if got := countEvents(t, eventRepo, "e-"+strconv.Itoa(i)); got != 1 {
			t.Fatalf("e-%d events = %d, want 1", i, got)
		}
	}
	// The repeated entity's versions must be a clean 1,2,3.
	evs, err := eventRepo.ListEvents(authCtx(), "crm.deals", domain.EventFilter{EntityID: "d-1"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	for i, ev := range evs {
		if ev.Version != int64(i+1) {
			t.Fatalf("d-1 event %d version = %d", i, ev.Version)
		}
	}
}

// batchState is a BatchEntry carrying a full-state write, for the batch accept
// integration test.
func batchState(index int, collection, entityID string, amount float64, eventID string) service.BatchEntry {
	return service.BatchEntry{
		Index: index,
		Kind:  service.KindState,
		Command: service.IngestCommand{
			Collection: collection,
			EntityID:   entityID,
			EventID:    eventID,
			State:      map[string]any{"amount": amount},
		},
	}
}

func countEventsIn(t *testing.T, repo *storage.EventRepo, collection, entity string) int {
	t.Helper()
	evs, err := repo.ListEvents(authCtx(), collection, domain.EventFilter{EntityID: entity})
	if err != nil {
		t.Fatalf("list events %s/%s: %v", collection, entity, err)
	}
	return len(evs)
}

// TestIntegrationBatchAcceptWritesToMongo drives the batch accept end-to-end over
// the durable path (S3-06): a batch of self-contained events is accepted onto the
// queue and a worker writes each to its own collection. Items of different
// collections land in their own ev_*, and two items sharing an event_id produce a
// single event — the storage idempotency barrier dedups the repeat (S2-05).
func TestIntegrationBatchAcceptWritesToMongo(t *testing.T) {
	ing, eventRepo := newIngester(t)
	durable := memory.New(4)
	w := New(durable, ing, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	async := service.NewAsyncIngester(ing, service.NewPipeline(durable, nil, nil), nil, service.AsyncOptions{})
	batch := service.NewBatchIngester(async, nil, slog.New(slog.NewTextHandler(io.Discard, nil)))

	entries := []service.BatchEntry{
		batchState(0, "crm.deals", "d-1", 100, ""),
		batchState(1, "crm.tasks", "t-1", 5, ""),
		batchState(2, "crm.deals", "d-dup", 1, "evt-x"),
		batchState(3, "crm.deals", "d-dup", 2, "evt-x"),
	}
	res, err := batch.Ingest(authCtx(), entries, nil, "")
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.Accepted != 4 || len(res.Rejected) != 0 {
		t.Fatalf("accepted=%d rejected=%d, want 4/0", res.Accepted, len(res.Rejected))
	}

	waitFor(t, 10*time.Second, func() bool {
		return countEventsIn(t, eventRepo, "crm.deals", "d-1") == 1 &&
			countEventsIn(t, eventRepo, "crm.tasks", "t-1") == 1 &&
			countEventsIn(t, eventRepo, "crm.deals", "d-dup") == 1
	})
	// The duplicate event_id must not have produced a second event.
	if got := countEventsIn(t, eventRepo, "crm.deals", "d-dup"); got != 1 {
		t.Fatalf("d-dup events = %d, want 1 (event_id dedup)", got)
	}
}

func mustEncodeI(t *testing.T, task service.Task) []byte {
	t.Helper()
	b, err := service.Encode(task)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}
