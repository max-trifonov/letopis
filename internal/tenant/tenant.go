// Package tenant resolves API keys to principals and answers authorization
// questions. It has no knowledge of HTTP or MongoDB; transport puts the
// principal in the context and storage reads it back to pick the database.
// The tenant is never taken from the URL or request body.
package tenant

import (
	"context"
	"fmt"
	"slices"
	"strings"
)

// Scope is a coarse permission attached to an API key. A key carries the set
// of scopes it was granted; handlers gate on them before touching storage.
type Scope string

const (
	ScopeWrite Scope = "write"
	ScopeRead  Scope = "read"
	ScopeAdmin Scope = "admin"
)

// dbPrefix is the global prefix for per-tenant databases on the shared cluster.
// A tenant may override the whole name via its own Database config.
const dbPrefix = "hm_t_"

// Database optionally pins a tenant to its own cluster and/or database name.
// The zero value means "default cluster, derived name".
type Database struct {
	URI  string
	Name string
}

// Tenant is one isolated customer. Its data lives in a dedicated MongoDB
// database, by default hm_t_{id} on the shared cluster.
type Tenant struct {
	ID       string
	Database Database
}

// DatabaseName returns the physical database the tenant's data lives in:
// the explicit override if set, otherwise the derived hm_t_{id}.
func (t Tenant) DatabaseName() string {
	if t.Database.Name != "" {
		return t.Database.Name
	}
	return dbPrefix + t.ID
}

// APIKey is a resolved credential: which tenant it speaks for, what it may
// do (scopes), and which collections it may touch. Collections is a set of
// glob-ish patterns ("crm.*"); an empty set grants no collections.
type APIKey struct {
	Scopes      []Scope
	Collections []string
}

// Principal is the authenticated identity of a request: the tenant plus the
// concrete key that authorized it. It travels in the request context and is
// the only source layers consult for "who is this" — never the URL or body.
type Principal struct {
	Tenant Tenant
	Key    APIKey
}

// HasScope reports whether the key was granted scope s.
func (p Principal) HasScope(s Scope) bool {
	return slices.Contains(p.Key.Scopes, s)
}

// CanAccess reports whether the key's collection mask admits the given
// logical collection name (e.g. "crm.deals"). Pure so it can be unit-tested
// in isolation.
func (p Principal) CanAccess(collection string) bool {
	return matchCollection(p.Key.Collections, collection)
}

// matchCollection tests a collection name against a set of patterns. A bare
// "*" matches anything; a trailing ".*" matches a prefix segment-wise
// ("crm.*" admits "crm.deals" but not "docs.x"); anything else must match
// exactly. Keeping this a free function makes the mask logic testable
// without constructing a Principal.
func matchCollection(patterns []string, name string) bool {
	for _, pat := range patterns {
		switch {
		case pat == "*":
			return true
		case strings.HasSuffix(pat, ".*"):
			if prefix := strings.TrimSuffix(pat, ".*"); name == prefix || strings.HasPrefix(name, prefix+".") {
				return true
			}
		case pat == name:
			return true
		}
	}
	return false
}

type ctxKey struct{}

// NewContext returns a copy of ctx carrying the principal. Transport calls
// this after a successful resolve; downstream layers read it via
// FromContext.
func NewContext(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext returns the principal placed by NewContext. The boolean is
// false on an unauthenticated context; storage treats that as a programming
// error rather than a default tenant.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// ParseScope validates and converts a config string to a Scope.
func ParseScope(s string) (Scope, error) {
	switch Scope(s) {
	case ScopeWrite, ScopeRead, ScopeAdmin:
		return Scope(s), nil
	default:
		return "", fmt.Errorf("tenant: unknown scope %q (want write, read or admin)", s)
	}
}
