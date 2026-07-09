// Package memory is the in-process Queue backend: one buffered channel per shard,
// FIFO within a shard. It backs single-node deployments and the fast path, and
// stands in for Redis in unit and acceptance tests. Because it shares nothing
// between processes it is only valid for role=all.
package memory

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/max-trifonov/letopis/internal/queue"
)

func init() {
	queue.Register(queue.DriverMemory, func(s queue.Settings, _ redis.UniversalClient) (queue.Queue, error) {
		return New(s.Shards), nil
	})
}

// pending is a delivered-but-unacked message and when it was last handed out,
// so Reclaim can age it.
type pending struct {
	d     queue.Delivery
	since time.Time
}

// Queue is the in-memory backend. The pending map mirrors a Redis consumer
// group's PEL: a message leaves the channel on delivery and only leaves
// pending on Ack; until then Reclaim can hand it out again.
type Queue struct {
	shards int
	chans  []chan queue.Delivery
	done   chan struct{}
	seq    atomic.Uint64

	mu      sync.Mutex
	pending []map[string]*pending
	closed  bool
}

// New builds a memory queue with the given shard count (clamped to >=1). The
// per-shard buffer bounds how many unconsumed messages Publish accepts before
// it blocks; the default is generous.
func New(shards int) *Queue {
	if shards < 1 {
		shards = 1
	}
	q := &Queue{
		shards:  shards,
		chans:   make([]chan queue.Delivery, shards),
		done:    make(chan struct{}),
		pending: make([]map[string]*pending, shards),
	}
	for i := range q.chans {
		q.chans[i] = make(chan queue.Delivery, 1024)
		q.pending[i] = map[string]*pending{}
	}
	return q
}

func (q *Queue) Shards() int { return q.shards }

func (q *Queue) Publish(ctx context.Context, msg queue.Message) error {
	// Check closed first: a buffered channel with free space would otherwise
	// race the done signal in the select below and let a publish slip through
	// after Close.
	select {
	case <-q.done:
		return queue.ErrClosed
	default:
	}
	sh := queue.Shard(msg.Key, q.shards)
	d := queue.Delivery{
		Message:    msg,
		ID:         strconv.FormatUint(q.seq.Add(1), 10),
		Shard:      sh,
		Deliveries: 1,
	}
	select {
	case q.chans[sh] <- d:
		return nil
	case <-q.done:
		return queue.ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe pumps a shard's channel into the returned channel, recording each
// message as pending before it is handed out. A message dropped on shutdown
// after being recorded stays pending and is recovered by Reclaim, preserving
// at-least-once.
func (q *Queue) Subscribe(ctx context.Context, shard int) (<-chan queue.Delivery, error) {
	if shard < 0 || shard >= q.shards {
		return nil, queue.ErrClosed
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
			case d := <-q.chans[shard]:
				q.mu.Lock()
				if !q.closed {
					q.pending[shard][d.ID] = &pending{d: d, since: time.Now()}
				}
				q.mu.Unlock()
				select {
				case out <- d:
				case <-ctx.Done():
					return
				case <-q.done:
					return
				}
			}
		}
	}()
	return out, nil
}

func (q *Queue) Ack(_ context.Context, d queue.Delivery) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.pending[d.Shard], d.ID)
	return nil
}

func (q *Queue) Reclaim(_ context.Context, shard int, minIdle time.Duration) ([]queue.Delivery, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	var out []queue.Delivery
	for _, p := range q.pending[shard] {
		if now.Sub(p.since) >= minIdle {
			p.since = now
			p.d.Deliveries++
			out = append(out, p.d)
		}
	}
	return out, nil
}

// Depth is the backlog across all shards: messages still buffered in the
// channels plus those delivered but not yet acked (the pending set). It is the
// in-memory analogue of redis-streams' lag+pending.
func (q *Queue) Depth(_ context.Context) (int64, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	var total int64
	for i := range q.shards {
		total += int64(len(q.chans[i])) + int64(len(q.pending[i]))
	}
	return total, nil
}

var _ queue.Observable = (*Queue)(nil)

func (q *Queue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.done)
	}
	return nil
}
