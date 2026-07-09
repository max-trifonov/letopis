// Package queue is the event-pipeline port: a small set of primitives —
// publish, subscribe, ack, reclaim — with interchangeable in-memory and Redis
// Streams backends. The domain and service layers depend on the Queue interface,
// never on a concrete backend.
//
// Ordering within an entity is preserved by sharding: every message is routed to
// a shard by hash(Key), so all events for one entity land in the same shard —
// one shard, one stream, one consumer goroutine, in order.
//
// Message.Payload is opaque []byte; the queue never inspects it.
package queue

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Driver names. They are also accepted in the config block; kept as constants
// here so wiring code refers to the source of truth.
const (
	DriverMemory       = "memory"
	DriverRedisStreams = "redis-streams"
)

// Backend errors. Callers match with errors.Is and translate at their own
// boundary (e.g. 503 / retry), never on message text.
var (
	// ErrQueueUnavailable means the backend could not be reached (Redis down). In
	// durable mode this surfaces to the API so it can refuse rather than silently
	// drop the write.
	ErrQueueUnavailable = errors.New("queue: backend unavailable")
	// ErrClosed is returned by Publish after Close.
	ErrClosed = errors.New("queue: closed")
)

// Message is what a producer hands to the queue. Key is the shard key
// ("tenant|collection|entity_id"), built by the calling layer; the queue only
// hashes it. Payload is opaque. Attrs carries small string side-channels (e.g.
// event_id for idempotency dedup) without parsing the payload.
type Message struct {
	Key     string
	Payload []byte
	Attrs   map[string]string
}

// Delivery is a Message handed to a consumer, tagged with the id the queue
// assigned, the shard it came from (needed to Ack/Reclaim), and how many times
// it has been delivered (1 on first read; higher after a Reclaim).
type Delivery struct {
	Message
	ID         string
	Shard      int
	Deliveries int64
}

// Queue is the event-pipeline port. Implementations are safe for concurrent
// use. Subscribe and Reclaim honor context cancellation so a worker can shut
// down cleanly.
type Queue interface {
	// Publish routes msg to its shard and appends it. It blocks only until the
	// message is durably accepted by the backend (XADD for redis, buffered for
	// memory).
	Publish(ctx context.Context, msg Message) error
	// Subscribe returns a channel of new (never-yet-delivered) messages for one
	// shard. The channel closes when ctx is cancelled or the queue is closed.
	// Stuck, already-delivered messages are not redelivered here — that is
	// Reclaim's job.
	Subscribe(ctx context.Context, shard int) (<-chan Delivery, error)
	// Ack confirms a delivery so it is removed from the pending set. The worker
	// calls it only after the write is committed (at-least-once).
	Ack(ctx context.Context, d Delivery) error
	// Reclaim returns deliveries that have been pending (delivered but unacked)
	// longer than minIdle, so a crashed consumer's work is picked up by another.
	Reclaim(ctx context.Context, shard int, minIdle time.Duration) ([]Delivery, error)
	// Shards is the configured shard count; a worker spawns one goroutine per
	// shard.
	Shards() int
	// Close releases the queue's own resources (memory: stops delivery
	// goroutines). It does not close a Redis client passed in from outside —
	// that is owned by the caller.
	Close() error
}

// Observable is the queue's depth side-channel: the current backlog, used by
// backpressure and the queue_depth gauge. It is separate from Queue so a backend
// that cannot answer cheaply is never forced to; callers type-assert and degrade
// gracefully. Both shipped backends implement it.
type Observable interface {
	// Depth returns the number of messages accepted but not yet acked across all
	// shards. A backend computes it however it can (channel occupancy for memory,
	// consumer-group lag plus pending for redis-streams); the figure is a recent
	// estimate, not a linearizable count.
	Depth(ctx context.Context) (int64, error)
}

// Shard maps a key to a shard in [0,n) deterministically and uniformly. FNV-1a
// is stable across runs and processes, which matters: producer and consumer
// must agree on the mapping. The empty key and n<=1 both collapse to shard 0.
func Shard(key string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// StreamName resolves a shard to its Redis stream name. Single source of truth
// so producer and consumer never disagree on the resource.
func StreamName(prefix string, shard int) string {
	return fmt.Sprintf("%s:%d", prefix, shard)
}

// Settings is the backend-agnostic input to New, mapped from config.Queue by
// the wiring layer so the queue package does not depend on config.
type Settings struct {
	Driver        string
	Shards        int
	StreamPrefix  string
	ConsumerGroup string
}

// Factory builds a Queue from settings and an optional Redis client (nil for
// the memory driver). Drivers register themselves under their name.
type Factory func(s Settings, rdb redis.UniversalClient) (Queue, error)

var drivers = map[string]Factory{}

// Register wires a driver into the factory. Called from a driver's init; the
// application blank-imports the drivers it wants (database/sql pattern), keeping
// this package free of import cycles with its sub-packages.
func Register(name string, f Factory) {
	if f == nil {
		panic("queue: Register called with nil factory for " + name)
	}
	drivers[name] = f
}

// New selects the implementation by Driver. The redis client is required for
// redis-streams and ignored by memory; it is owned by the caller (the queue
// does not close it).
func New(s Settings, rdb redis.UniversalClient) (Queue, error) {
	f, ok := drivers[s.Driver]
	if !ok {
		return nil, fmt.Errorf("queue: unknown or unregistered driver %q (did you blank-import its package?)", s.Driver)
	}
	return f(s, rdb)
}
