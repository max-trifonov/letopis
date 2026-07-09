// Package service holds the use-case orchestration layer. It depends on domain
// ports, never on MongoDB or HTTP directly.
package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// ErrAutoCreateDisabled is returned when a write targets an unknown collection
// and auto-create is off. Transport maps it to 404.
var ErrAutoCreateDisabled = errors.New("service: collection does not exist and auto-create is disabled")

// ConfigResolver resolves per-collection config with defaults applied, backed by
// a per-tenant TTL cache. The cache key is scoped by tenant database so two
// tenants' same-named collections never alias.
type ConfigResolver struct {
	repo       domain.CollectionRepository
	autoCreate bool
	ttl        time.Duration
	now        func() time.Time

	mu    sync.Mutex
	cache map[string]cacheEntry
}

type cacheEntry struct {
	cfg     domain.CollectionConfig
	expires time.Time
}

// Options configure a ConfigResolver. AutoCreate mirrors the instance/config flag;
// CacheTTL bounds staleness of a cached config.
type Options struct {
	AutoCreate bool
	CacheTTL   time.Duration
	// Now is injectable for tests; defaults to time.Now.
	Now func() time.Time
}

const defaultCacheTTL = 30 * time.Second

func NewConfigResolver(repo domain.CollectionRepository, opts Options) *ConfigResolver {
	if opts.CacheTTL <= 0 {
		opts.CacheTTL = defaultCacheTTL
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &ConfigResolver{
		repo:       repo,
		autoCreate: opts.AutoCreate,
		ttl:        opts.CacheTTL,
		now:        opts.Now,
		cache:      map[string]cacheEntry{},
	}
}

// Get returns the collection's config with defaults applied, serving from cache
// when fresh. Returns domain.ErrNotFound when the collection has no stored
// config; callers that may create it should use EnsureCollection.
func (cr *ConfigResolver) Get(ctx context.Context, collection string) (*domain.CollectionConfig, error) {
	key, err := cacheKey(ctx, collection)
	if err != nil {
		return nil, err
	}
	if cfg, ok := cr.cached(key); ok {
		return cfg, nil
	}
	stored, err := cr.repo.GetConfig(ctx, collection)
	if err != nil {
		return nil, err
	}
	cfg := domain.WithDefaults(*stored)
	cr.store(key, cfg)
	return &cfg, nil
}

// EnsureCollection resolves the config, creating the collection on first use when
// auto-create is enabled: persists a default config and provisions the physical
// ev_*/cur_* collections and indexes (idempotent). Returns ErrAutoCreateDisabled
// with auto-create off.
func (cr *ConfigResolver) EnsureCollection(ctx context.Context, collection string) (*domain.CollectionConfig, error) {
	cfg, err := cr.Get(ctx, collection)
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, domain.ErrNotFound) {
		return nil, err
	}
	if !cr.autoCreate {
		return nil, ErrAutoCreateDisabled
	}

	created := domain.WithDefaults(domain.CollectionConfig{Name: collection})
	if err := cr.repo.SaveConfig(ctx, &created); err != nil {
		return nil, err
	}
	if err := cr.repo.EnsurePhysical(ctx, collection); err != nil {
		return nil, err
	}
	key, err := cacheKey(ctx, collection)
	if err != nil {
		return nil, err
	}
	cr.store(key, created)
	return &created, nil
}

// Invalidate drops the cached config for a collection. Safe to call for an
// uncached key.
func (cr *ConfigResolver) Invalidate(ctx context.Context, collection string) {
	key, err := cacheKey(ctx, collection)
	if err != nil {
		return
	}
	cr.mu.Lock()
	delete(cr.cache, key)
	cr.mu.Unlock()
}

func (cr *ConfigResolver) cached(key string) (*domain.CollectionConfig, bool) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	e, ok := cr.cache[key]
	if !ok || !cr.now().Before(e.expires) {
		return nil, false
	}
	cfg := e.cfg
	return &cfg, true
}

func (cr *ConfigResolver) store(key string, cfg domain.CollectionConfig) {
	cr.mu.Lock()
	cr.cache[key] = cacheEntry{cfg: cfg, expires: cr.now().Add(cr.ttl)}
	cr.mu.Unlock()
}

// cacheKey scopes a collection by the request's tenant database, so the cache
// cannot leak one tenant's config to another.
func cacheKey(ctx context.Context, collection string) (string, error) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return "", tenant.ErrNoKey
	}
	return p.Tenant.DatabaseName() + "|" + collection, nil
}
