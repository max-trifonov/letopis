package mongo

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/tenant"
)

// ErrNoTenant means storage was reached without an authenticated principal in
// the context. This is a programming error — every storage path runs behind
// auth, and there is no "default tenant".
var ErrNoTenant = errors.New("mongo: no tenant in context")

// ConnManager resolves the MongoDB database for the request's tenant. Tenants
// on the shared cluster share one client; a tenant with its own URI gets a
// dedicated client, created lazily on first use. Clients are safe for
// concurrent use, so the cache only contends on first-touch.
type ConnManager struct {
	defaultURI string

	mu      sync.Mutex
	clients map[string]*mongo.Client
}

// NewConnManager connects the default cluster eagerly so /readyz reflects
// its health from startup. The driver's Connect does not dial immediately;
// reachability is verified by Ping.
func NewConnManager(defaultURI string) (*ConnManager, error) {
	m := &ConnManager{defaultURI: defaultURI, clients: map[string]*mongo.Client{}}
	if _, err := m.client(defaultURI); err != nil {
		return nil, err
	}
	return m, nil
}

// DBFor returns the database for the request's tenant. The tenant and its
// database name come only from the context.
func (m *ConnManager) DBFor(ctx context.Context) (*mongo.Database, error) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return nil, ErrNoTenant
	}
	uri := p.Tenant.Database.URI
	if uri == "" {
		uri = m.defaultURI
	}
	cl, err := m.client(uri)
	if err != nil {
		return nil, err
	}
	return cl.Database(p.Tenant.DatabaseName()), nil
}

func (m *ConnManager) client(uri string) (*mongo.Client, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cl, ok := m.clients[uri]; ok {
		return cl, nil
	}
	cl, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, fmt.Errorf("mongo: connect %s: %w", uri, err)
	}
	m.clients[uri] = cl
	return cl, nil
}

// Ping reports the readiness of every connected cluster, aggregating failures
// so /readyz fails if any tenant's storage is unreachable.
func (m *ConnManager) Ping(ctx context.Context) error {
	m.mu.Lock()
	clients := make(map[string]*mongo.Client, len(m.clients))
	maps.Copy(clients, m.clients)
	m.mu.Unlock()

	var errs []error
	for uri, cl := range clients {
		if err := cl.Ping(ctx, nil); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", uri, err))
		}
	}
	return errors.Join(errs...)
}

func (m *ConnManager) Close(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for _, cl := range m.clients {
		if err := cl.Disconnect(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
