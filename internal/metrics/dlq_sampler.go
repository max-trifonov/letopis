package metrics

import (
	"context"
	"log/slog"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

const defaultDLQSampleInterval = 15 * time.Second

// DLQSampler periodically counts the dead-letter queue across every
// configured tenant and publishes the total on the webhook_dlq_size gauge.
// Runs in the worker role alongside the queue-depth sampler.
type DLQSampler struct {
	repo     domain.DLQRepository
	tenants  []tenant.Tenant
	met      *Metrics
	log      *slog.Logger
	interval time.Duration
}

// NewDLQSampler builds a sampler over the given tenants. DLQRepository.Count
// resolves its database from a tenant in ctx (ADR-010: one database per
// tenant), so the sampler must supply one context per tenant rather than a
// single bare context — there is no "default tenant" to fall back to.
func NewDLQSampler(repo domain.DLQRepository, tenants []tenant.Tenant, met *Metrics, log *slog.Logger) *DLQSampler {
	return &DLQSampler{
		repo:     repo,
		tenants:  tenants,
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

// sample sums the DLQ size across every configured tenant. One tenant's
// failure is logged and skipped so it does not blind the gauge for the rest.
func (s *DLQSampler) sample(ctx context.Context) {
	var total int64
	for _, t := range s.tenants {
		tctx := tenant.NewContext(ctx, tenant.Principal{Tenant: t})
		n, err := s.repo.Count(tctx, "")
		if err != nil {
			s.log.Warn("dlq count failed", "tenant", t.ID, "err", err)
			continue
		}
		total += n
	}
	s.met.SetDLQSize(float64(total))
}
