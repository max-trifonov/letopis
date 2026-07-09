// Package redisstreams is the production Queue backend: one Redis stream per
// shard, a consumer group shared by all workers, XACK after the write. It
// survives process restarts — unacked messages stay in the pending list and are
// reclaimed via XAUTOCLAIM.
package redisstreams

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/max-trifonov/letopis/internal/queue"
)

func init() {
	queue.Register(queue.DriverRedisStreams, func(s queue.Settings, rdb redis.UniversalClient) (queue.Queue, error) {
		if rdb == nil {
			return nil, fmt.Errorf("queue: redis-streams driver requires a redis client")
		}
		return New(rdb, s.Shards, s.StreamPrefix, s.ConsumerGroup), nil
	})
}

// blockTimeout bounds how long XREADGROUP parks server-side before returning
// empty, so the subscribe loop gets back to check for cancellation.
const blockTimeout = 200 * time.Millisecond

// payloadField and keyField are the stream entry fields; attribute fields are
// prefixed so they round-trip without colliding with the reserved two.
const (
	keyField     = "key"
	payloadField = "payload"
	attrPrefix   = "a:"
)

// Queue is the Redis Streams backend. The redis client is injected and owned by
// the caller; Close here is a no-op.
type Queue struct {
	rdb      redis.UniversalClient
	shards   int
	prefix   string
	group    string
	consumer string

	closeOnce sync.Once
	done      chan struct{}
}

// New builds a redis-streams queue. The consumer name is unique per process so
// pending ownership is attributable; reclamation (XAUTOCLAIM) crosses consumers
// regardless, so a crashed process's work is still recoverable.
func New(rdb redis.UniversalClient, shards int, prefix, group string) *Queue {
	if shards < 1 {
		shards = 1
	}
	return &Queue{rdb: rdb, shards: shards, prefix: prefix, group: group, consumer: newConsumerName(), done: make(chan struct{})}
}

func (q *Queue) Shards() int { return q.shards }

func (q *Queue) Publish(ctx context.Context, msg queue.Message) error {
	sh := queue.Shard(msg.Key, q.shards)
	values := map[string]any{keyField: msg.Key, payloadField: msg.Payload}
	for k, v := range msg.Attrs {
		values[attrPrefix+k] = v
	}
	if err := q.rdb.XAdd(ctx, &redis.XAddArgs{Stream: queue.StreamName(q.prefix, sh), Values: values}).Err(); err != nil {
		return unavailable(err)
	}
	return nil
}

func (q *Queue) Subscribe(ctx context.Context, shard int) (<-chan queue.Delivery, error) {
	if shard < 0 || shard >= q.shards {
		return nil, fmt.Errorf("queue: shard %d out of range [0,%d)", shard, q.shards)
	}
	stream := queue.StreamName(q.prefix, shard)
	if err := q.ensureGroup(ctx, stream); err != nil {
		return nil, err
	}
	out := make(chan queue.Delivery)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-q.done:
				return
			default:
			}
			res, err := q.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    q.group,
				Consumer: q.consumer,
				Streams:  []string{stream, ">"},
				Count:    1,
				Block:    blockTimeout,
			}).Result()
			switch {
			case err == redis.Nil:
				continue // block timed out, no new messages
			case err != nil:
				if ctx.Err() != nil {
					return
				}
				// Transient backend error: brief backoff, then retry. A hard outage
				// keeps looping until shutdown; backpressure surfaces it upstream.
				select {
				case <-ctx.Done():
					return
				case <-q.done:
					return
				case <-time.After(blockTimeout):
				}
				continue
			}
			for _, s := range res {
				for _, m := range s.Messages {
					select {
					case out <- decode(shard, m, 1):
					case <-ctx.Done():
						return
					case <-q.done:
						return
					}
				}
			}
		}
	}()
	return out, nil
}

func (q *Queue) Ack(ctx context.Context, d queue.Delivery) error {
	return unavailable(q.rdb.XAck(ctx, queue.StreamName(q.prefix, d.Shard), q.group, d.ID).Err())
}

func (q *Queue) Reclaim(ctx context.Context, shard int, minIdle time.Duration) ([]queue.Delivery, error) {
	stream := queue.StreamName(q.prefix, shard)
	msgs, _, err := q.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    q.group,
		Consumer: q.consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    100,
	}).Result()
	if err != nil {
		// A shard that has never been subscribed has no group/stream yet, so
		// there is nothing pending on it — not an error.
		if strings.Contains(err.Error(), "NOGROUP") {
			return nil, nil
		}
		return nil, unavailable(err)
	}
	out := make([]queue.Delivery, 0, len(msgs))
	for _, m := range msgs {
		// XAUTOCLAIM does not return the retry count; Deliveries=0 signals "reclaimed, count unknown".
		out = append(out, decode(shard, m, 0))
	}
	return out, nil
}

// Depth is the backlog across all shards: for each stream's consumer group, the
// undelivered lag plus the delivered-but-unacked pending count. This stays
// accurate even though acked entries are never trimmed — lag and pending both
// track unprocessed work, not total stream length. A shard with no group yet
// (never subscribed) contributes nothing. Best-effort: one XINFO GROUPS per
// shard, tolerates a redis hiccup on any of them.
func (q *Queue) Depth(ctx context.Context) (int64, error) {
	var total int64
	for shard := range q.shards {
		groups, err := q.rdb.XInfoGroups(ctx, queue.StreamName(q.prefix, shard)).Result()
		if err != nil {
			// An absent stream (no writes/subscribe yet) has no backlog; any other
			// error means we cannot price this shard, so surface it rather than
			// under-report a depth backpressure relies on.
			if strings.Contains(err.Error(), "no such key") || strings.Contains(err.Error(), "NOGROUP") {
				continue
			}
			return 0, unavailable(err)
		}
		for _, g := range groups {
			if g.Name != q.group {
				continue
			}
			if g.Lag > 0 { // -1 means "undetermined"; pending still counts
				total += g.Lag
			}
			total += g.Pending
		}
	}
	return total, nil
}

var _ queue.Observable = (*Queue)(nil)

// Close stops the subscribe loops. It does not disconnect the redis client —
// that is owned by the caller (app) and closed during shutdown.
func (q *Queue) Close() error {
	q.closeOnce.Do(func() { close(q.done) })
	return nil
}

// ensureGroup creates the consumer group (and the stream) if missing. A
// pre-existing group surfaces as BUSYGROUP, which is benign.
func (q *Queue) ensureGroup(ctx context.Context, stream string) error {
	err := q.rdb.XGroupCreateMkStream(ctx, stream, q.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return unavailable(err)
	}
	return nil
}

func decode(shard int, m redis.XMessage, deliveries int64) queue.Delivery {
	d := queue.Delivery{
		Message:    queue.Message{Attrs: map[string]string{}},
		ID:         m.ID,
		Shard:      shard,
		Deliveries: deliveries,
	}
	for k, v := range m.Values {
		s, _ := v.(string)
		switch {
		case k == keyField:
			d.Key = s
		case k == payloadField:
			d.Payload = []byte(s)
		case strings.HasPrefix(k, attrPrefix):
			d.Attrs[strings.TrimPrefix(k, attrPrefix)] = s
		}
	}
	return d
}

// unavailable wraps a backend error as ErrQueueUnavailable so callers can react
// uniformly (the precise driver error is preserved for logs).
func unavailable(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", queue.ErrQueueUnavailable, err)
}

func newConsumerName() string {
	host, _ := os.Hostname()
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b[:]))
}
