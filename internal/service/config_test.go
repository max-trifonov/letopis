package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// fakeRepo is an in-memory CollectionRepository that records calls, so the
// service can be tested without MongoDB (real provisioning is covered by the
// integration test).
type fakeRepo struct {
	mu          sync.Mutex
	configs     map[string]domain.CollectionConfig
	getCalls    int
	saveCalls   int
	ensureCalls int
	getErr      error
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{configs: map[string]domain.CollectionConfig{}}
}

func (f *fakeRepo) GetConfig(_ context.Context, collection string) (*domain.CollectionConfig, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getErr != nil {
		return nil, f.getErr
	}
	cfg, ok := f.configs[collection]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return &cfg, nil
}

func (f *fakeRepo) SaveConfig(_ context.Context, cfg *domain.CollectionConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveCalls++
	f.configs[cfg.Name] = *cfg
	return nil
}

func (f *fakeRepo) EnsurePhysical(_ context.Context, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ensureCalls++
	return nil
}

func authedCtx() context.Context {
	return tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: "acme"}})
}

func TestGetRequiresTenant(t *testing.T) {
	cr := NewConfigResolver(newFakeRepo(), Options{})
	if _, err := cr.Get(context.Background(), "crm.deals"); !errors.Is(err, tenant.ErrNoKey) {
		t.Fatalf("err = %v, want ErrNoKey", err)
	}
}

func TestGetAppliesDefaults(t *testing.T) {
	repo := newFakeRepo()
	repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals"} // no fields set
	cr := NewConfigResolver(repo, Options{})

	cfg, err := cr.Get(authedCtx(), "crm.deals")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if cfg.FirstEventOp != domain.FirstEventCreate || cfg.SnapshotInterval != domain.DefaultSnapshotInterval {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}

func TestGetCaches(t *testing.T) {
	repo := newFakeRepo()
	repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals"}
	cr := NewConfigResolver(repo, Options{CacheTTL: time.Minute})

	ctx := authedCtx()
	for range 3 {
		if _, err := cr.Get(ctx, "crm.deals"); err != nil {
			t.Fatalf("Get: %v", err)
		}
	}
	if repo.getCalls != 1 {
		t.Fatalf("repo hit %d times, want 1 (cached)", repo.getCalls)
	}
}

func TestCacheExpiresAndInvalidates(t *testing.T) {
	repo := newFakeRepo()
	repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals"}
	now := time.Unix(0, 0)
	cr := NewConfigResolver(repo, Options{CacheTTL: time.Minute, Now: func() time.Time { return now }})

	ctx := authedCtx()
	_, _ = cr.Get(ctx, "crm.deals")
	now = now.Add(2 * time.Minute) // past TTL
	_, _ = cr.Get(ctx, "crm.deals")
	if repo.getCalls != 2 {
		t.Fatalf("expected re-read after TTL, getCalls=%d", repo.getCalls)
	}

	cr.Invalidate(ctx, "crm.deals")
	_, _ = cr.Get(ctx, "crm.deals")
	if repo.getCalls != 3 {
		t.Fatalf("expected re-read after invalidate, getCalls=%d", repo.getCalls)
	}
}

// Two tenants with the same collection name must not share a cache entry.
func TestCacheIsTenantScoped(t *testing.T) {
	repo := newFakeRepo()
	repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals"}
	cr := NewConfigResolver(repo, Options{CacheTTL: time.Minute})

	ctxA := tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: "a"}})
	ctxB := tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: "b"}})
	_, _ = cr.Get(ctxA, "crm.deals")
	_, _ = cr.Get(ctxB, "crm.deals")
	if repo.getCalls != 2 {
		t.Fatalf("tenants shared a cache entry, getCalls=%d", repo.getCalls)
	}
}

func TestEnsureCollectionAutoCreates(t *testing.T) {
	repo := newFakeRepo()
	cr := NewConfigResolver(repo, Options{AutoCreate: true})

	cfg, err := cr.EnsureCollection(authedCtx(), "crm.deals")
	if err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}
	if cfg.FirstEventOp != domain.FirstEventCreate {
		t.Fatalf("created config missing defaults: %+v", cfg)
	}
	if repo.saveCalls != 1 || repo.ensureCalls != 1 {
		t.Fatalf("expected one save and one ensure, got save=%d ensure=%d", repo.saveCalls, repo.ensureCalls)
	}
}

func TestEnsureCollectionExisting(t *testing.T) {
	repo := newFakeRepo()
	repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals"}
	cr := NewConfigResolver(repo, Options{AutoCreate: true})

	if _, err := cr.EnsureCollection(authedCtx(), "crm.deals"); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}
	if repo.saveCalls != 0 || repo.ensureCalls != 0 {
		t.Fatalf("existing collection must not be re-created: save=%d ensure=%d", repo.saveCalls, repo.ensureCalls)
	}
}

func TestEnsureCollectionDisabled(t *testing.T) {
	repo := newFakeRepo()
	cr := NewConfigResolver(repo, Options{AutoCreate: false})

	if _, err := cr.EnsureCollection(authedCtx(), "crm.deals"); !errors.Is(err, ErrAutoCreateDisabled) {
		t.Fatalf("err = %v, want ErrAutoCreateDisabled", err)
	}
	if repo.saveCalls != 0 {
		t.Fatal("must not save when auto-create is off")
	}
}

func TestEnsureCollectionPropagatesError(t *testing.T) {
	repo := newFakeRepo()
	repo.getErr = errors.New("boom")
	cr := NewConfigResolver(repo, Options{AutoCreate: true})

	if _, err := cr.EnsureCollection(authedCtx(), "crm.deals"); err == nil || errors.Is(err, ErrAutoCreateDisabled) {
		t.Fatalf("expected propagated repo error, got %v", err)
	}
}
