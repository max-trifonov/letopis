package metrics

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
)

// defaultSampleInterval is how often the sampler re-reads queue depth. Sampling
// bounds the load of the depth query and damps backpressure so a burst
// straddling the threshold does not flap 429s on and off.
const defaultSampleInterval = time.Second

// source is one observed queue: its label, the backend to read depth from, and
// the backpressure threshold. depth caches the last sample so Allow is a lock-free
// atomic read on the hot ingest path.
type source struct {
	name       string
	obs        queue.Observable
	maxDepth   int64
	retryAfter time.Duration
	depth      atomic.Int64
}

// Sampler periodically reads each registered queue's depth, publishes it to the
// queue_depth gauge, and answers backpressure (Allow) from the cached value. It
// implements service.QueueLimiter. One sampler serves every queue; sources are
// keyed by the reliability mode that routes to them, so Allow maps a request's
// mode straight to its threshold.
type Sampler struct {
	m        *Metrics
	log      *slog.Logger
	interval time.Duration
	sources  map[domain.ReliabilityMode]*source
}

func NewSampler(m *Metrics, log *slog.Logger, interval time.Duration) *Sampler {
	if interval <= 0 {
		interval = defaultSampleInterval
	}
	return &Sampler{m: m, log: log.With("component", "queue-sampler"), interval: interval, sources: map[domain.ReliabilityMode]*source{}}
}

func (s *Sampler) Add(mode domain.ReliabilityMode, name string, obs queue.Observable, maxDepth int64, retryAfter time.Duration) {
	s.sources[mode] = &source{name: name, obs: obs, maxDepth: maxDepth, retryAfter: retryAfter}
}

// Run samples every registered queue until the context is cancelled. A clean
// shutdown returns nil so the surrounding errgroup stays clean.
func (s *Sampler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	s.sampleAll(ctx) // prime the gauges immediately rather than after one interval
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.sampleAll(ctx)
		}
	}
}

func (s *Sampler) sampleAll(ctx context.Context) {
	for _, src := range s.sources {
		d, err := src.obs.Depth(ctx)
		if err != nil {
			// Keep the last good sample rather than zeroing the gauge (and silently
			// lifting backpressure) on a transient read error.
			s.log.Warn("queue depth sample failed", "queue", src.name, "err", err)
			continue
		}
		src.depth.Store(d)
		s.m.SetQueueDepth(src.name, float64(d))
	}
}

// Allow reports whether a write in mode may be enqueued. An unknown or ungated
// (maxDepth<=0) mode is always allowed. A refusal increments the backpressure
// counter and returns the source's Retry-After hint.
func (s *Sampler) Allow(mode domain.ReliabilityMode) (bool, time.Duration) {
	src, ok := s.sources[mode]
	if !ok || src.maxDepth <= 0 {
		return true, 0
	}
	if src.depth.Load() >= src.maxDepth {
		s.m.IncBackpressure(src.name)
		return false, src.retryAfter
	}
	return true, 0
}
