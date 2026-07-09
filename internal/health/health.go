// Package health aggregates readiness checks. Liveness is a separate,
// unconditional concern handled by the HTTP layer; readiness fails when
// any registered check fails.
package health

import (
	"context"
	"sync"
	"time"
)

// Check reports whether one dependency is usable. It must respect the
// context deadline — readiness is polled by orchestrators and cannot
// afford to hang.
type Check func(ctx context.Context) error

const checkTimeout = 2 * time.Second

type Registry struct {
	mu     sync.RWMutex
	checks map[string]Check
}

func NewRegistry() *Registry {
	return &Registry{checks: map[string]Check{}}
}

func (r *Registry) Register(name string, c Check) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checks[name] = c
}

// Run executes all checks and returns the failures by name. An empty
// map means ready.
func (r *Registry) Run(ctx context.Context) map[string]error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	failures := map[string]error{}
	for name, check := range r.checks {
		cctx, cancel := context.WithTimeout(ctx, checkTimeout)
		if err := check(cctx); err != nil {
			failures[name] = err
		}
		cancel()
	}
	return failures
}
