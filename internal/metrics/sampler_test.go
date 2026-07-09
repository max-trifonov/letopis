package metrics

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/max-trifonov/letopis/internal/domain"
)

// fakeObservable returns a settable depth, standing in for a real queue.
type fakeObservable struct{ depth int64 }

func (f *fakeObservable) Depth(context.Context) (int64, error) { return f.depth, nil }

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestSampler(t *testing.T) (*Sampler, *Metrics, prometheus.Gatherer) {
	t.Helper()
	reg := prometheus.NewRegistry()
	m := New(reg)
	return NewSampler(m, discardLog(), time.Hour), m, reg
}

// Below the threshold a write is allowed; at or above it is refused with the
// configured Retry-After and the backpressure counter ticks (NFR-2.3).
func TestSamplerAllowThreshold(t *testing.T) {
	s, _, reg := newTestSampler(t)
	obs := &fakeObservable{depth: 5}
	s.Add(domain.ReliabilityDurable, "durable", obs, 10, 3*time.Second)

	s.sampleAll(context.Background())
	if ok, _ := s.Allow(domain.ReliabilityDurable); !ok {
		t.Fatal("depth 5 < 10 must be allowed")
	}

	obs.depth = 10
	s.sampleAll(context.Background())
	ok, retryAfter := s.Allow(domain.ReliabilityDurable)
	if ok || retryAfter != 3*time.Second {
		t.Fatalf("depth 10 >= 10 must be refused with 3s: ok=%v ra=%v", ok, retryAfter)
	}
	if got := testutil.ToFloat64(s.m.backpressure.WithLabelValues("durable")); got != 1 {
		t.Fatalf("backpressure counter = %v, want 1", got)
	}
	if got := testutil.ToFloat64(s.m.queueDepth.WithLabelValues("durable")); got != 10 {
		t.Fatalf("queue_depth gauge = %v, want 10", got)
	}
	_ = reg
}

// A zero threshold disables gating but still samples the gauge.
func TestSamplerUngatedStillSamples(t *testing.T) {
	s, _, _ := newTestSampler(t)
	obs := &fakeObservable{depth: 999}
	s.Add(domain.ReliabilityDurable, "durable", obs, 0, time.Second)
	s.sampleAll(context.Background())

	if ok, _ := s.Allow(domain.ReliabilityDurable); !ok {
		t.Fatal("maxDepth 0 must never gate")
	}
	if got := testutil.ToFloat64(s.m.queueDepth.WithLabelValues("durable")); got != 999 {
		t.Fatalf("gauge not sampled: %v", got)
	}
}

// An unregistered mode is always allowed (e.g. strict never reaches the limiter).
func TestSamplerUnknownModeAllowed(t *testing.T) {
	s, _, _ := newTestSampler(t)
	if ok, _ := s.Allow(domain.ReliabilityFast); !ok {
		t.Fatal("unknown mode must be allowed")
	}
}
