package metrics

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// fakeDLQRepo answers Count per-tenant from a fixed map and fails without a
// tenant in ctx, mirroring the real Mongo-backed repo (DBFor requires one).
type fakeDLQRepo struct {
	counts map[string]int64
	fail   map[string]error
}

func (f *fakeDLQRepo) Save(context.Context, domain.DeadLetter) error { return nil }

func (f *fakeDLQRepo) List(context.Context, string, int, *domain.DLQCursor) ([]domain.DeadLetter, error) {
	return nil, nil
}

func (f *fakeDLQRepo) Get(context.Context, string) (*domain.DeadLetter, error) { return nil, nil }

func (f *fakeDLQRepo) Delete(context.Context, string) error { return nil }

func (f *fakeDLQRepo) Count(ctx context.Context, _ string) (int64, error) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return 0, errors.New("mongo: no tenant in context")
	}
	if err, bad := f.fail[p.Tenant.ID]; bad {
		return 0, err
	}
	return f.counts[p.Tenant.ID], nil
}

// The sampler must inject a tenant context per configured tenant. Calling
// Count with the bare ctx (the pre-fix behaviour) always errors here, so
// this also pins the regression: without per-tenant contexts, the sum stays 0.
func TestDLQSamplerSumsAcrossConfiguredTenants(t *testing.T) {
	m := New(prometheus.NewRegistry())
	repo := &fakeDLQRepo{counts: map[string]int64{"acme": 3, "globex": 5}}
	tenants := []tenant.Tenant{{ID: "acme"}, {ID: "globex"}}

	s := NewDLQSampler(repo, tenants, m, discardLog())
	s.sample(context.Background())

	if got := testutil.ToFloat64(m.dlqSize); got != 8 {
		t.Fatalf("webhook_dlq_size = %v, want 8 (sum across tenants)", got)
	}
}

// One tenant's Count failing (e.g. a transient Mongo blip) must not blind
// the gauge for the rest — the other tenants' counts still land.
func TestDLQSamplerSkipsFailingTenantButSumsRest(t *testing.T) {
	m := New(prometheus.NewRegistry())
	repo := &fakeDLQRepo{
		counts: map[string]int64{"globex": 5},
		fail:   map[string]error{"acme": errors.New("boom")},
	}
	tenants := []tenant.Tenant{{ID: "acme"}, {ID: "globex"}}

	s := NewDLQSampler(repo, tenants, m, discardLog())
	s.sample(context.Background())

	if got := testutil.ToFloat64(m.dlqSize); got != 5 {
		t.Fatalf("webhook_dlq_size = %v, want 5 (acme failed, globex still counted)", got)
	}
}

func TestDLQSamplerNoTenantsSetsZero(t *testing.T) {
	m := New(prometheus.NewRegistry())
	repo := &fakeDLQRepo{counts: map[string]int64{}}

	s := NewDLQSampler(repo, nil, m, discardLog())
	s.sample(context.Background())

	if got := testutil.ToFloat64(m.dlqSize); got != 0 {
		t.Fatalf("webhook_dlq_size = %v, want 0", got)
	}
}
