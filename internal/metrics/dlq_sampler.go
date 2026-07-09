package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

const defaultDLQSampleInterval = 15 * time.Second

// DLQSampler periodically counts the dead-letter queue and publishes the
// webhook_dlq_size gauge. Runs in the worker role alongside the queue-depth
// sampler.
type DLQSampler struct {
	repo     domain.DLQRepository
	met      *Metrics
	log      *slog.Logger
	interval time.Duration
}

func NewDLQSampler(repo domain.DLQRepository, met *Metrics, log *slog.Logger) *DLQSampler {
	return &DLQSampler{
		repo:     repo,
		met:      met,
		log:      log.With("component", "dlq-sampler"),
		interval: defaultDLQSampleInterval,
	}
}

// Run samples the DLQ size until ctx is cancelled. A sampling error is logged
// and skipped rather than propagated so a Mongo blip does not crash the worker.
func (s *DLQSampler) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.sample(ctx)
		}
	}
}

func (s *DLQSampler) sample(ctx context.Context) {
	n, err := s.repo.Count(ctx, "")
	if err != nil {
		s.log.Warn("dlq count failed", "err", err)
		return
	}
	s.met.SetDLQSize(float64(n))
}
