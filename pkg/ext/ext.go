// Package ext is the compile-time extension surface. The open-source binary
// runs on Defaults(); a distribution supplies its own implementations and
// builds its own cmd around app.Run.
package ext

import (
	"context"
	"errors"

	"github.com/max-trifonov/letopis/internal/plugin"
	"github.com/max-trifonov/letopis/internal/plugin/hashchain"
)

// Plugin hook aliases — distributions implement these and register them in Registry.
type (
	PreStorePlugin  = plugin.PreStorePlugin
	PostStorePlugin = plugin.PostStorePlugin
	ActionPlugin    = plugin.ActionPlugin
	EventDraft      = plugin.EventDraft
	EntityView      = plugin.EntityView
)

// ErrUnknownKey is returned by TenantProvider when an API key doesn't match any tenant.
var ErrUnknownKey = errors.New("ext: unknown api key")

// Tenant is the resolved owner of a request.
type Tenant struct {
	ID string

	// MongoURI and Database override the default cluster for tenants on their
	// own infrastructure. Empty means "use the default cluster".
	MongoURI string
	Database string
}

// TenantProvider resolves an API key (by its SHA-256 hash, never plaintext) to a tenant.
type TenantProvider interface {
	ResolveKey(ctx context.Context, keyHash string) (*Tenant, error)
}

// Metering receives usage signals on the hot path. Must be cheap and non-blocking.
type Metering interface {
	Record(ctx context.Context, tenantID, op string, n int)
}

// Registry is the set of pluggable implementations an app is built with.
// Tenants and Metering are required; plugin slices are optional and empty by default.
type Registry struct {
	Tenants  TenantProvider
	Metering Metering

	// PreStore/PostStore/Actions run in slice order. A plugin only participates
	// in a collection when that collection's config enables it by Name (pre/post)
	// or a rule names its Type (actions).
	PreStore  []PreStorePlugin
	PostStore []PostStorePlugin
	Actions   []ActionPlugin
}

// Defaults returns the registry the open-source binary ships with. The hash-chain
// plugin is registered but stays inert until a collection enables it via
// plugins.hash_chain.enabled, so default behaviour is unchanged.
func Defaults() *Registry {
	return &Registry{
		Tenants:  emptyTenants{},
		Metering: noopMetering{},
		PreStore: []PreStorePlugin{hashchain.New()},
	}
}

type emptyTenants struct{}

func (emptyTenants) ResolveKey(context.Context, string) (*Tenant, error) {
	return nil, ErrUnknownKey
}

type noopMetering struct{}

func (noopMetering) Record(context.Context, string, string, int) {}
