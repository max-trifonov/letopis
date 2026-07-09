package tenant

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Resolve errors. Callers match with errors.Is and translate to 401/403 at
// the transport boundary.
var (
	ErrNoKey      = errors.New("tenant: no API key presented")
	ErrUnknownKey = errors.New("tenant: API key not recognized")
)

const hashPrefix = "sha256:"

// Spec is the config-shaped input to NewResolver: one tenant with its keys,
// kept separate from the domain model so the YAML/JSON boundary does not
// leak inward. The mapping to Tenant/APIKey happens here, explicitly.
type Spec struct {
	ID     string
	DBURI  string
	DBName string
	Keys   []KeySpec
}

// KeySpec is one credential as configured. Exactly one of Hash ("sha256:…")
// or Plaintext is expected; Plaintext is a dev convenience and is reported
// as a warning so it never slips into production unnoticed.
type KeySpec struct {
	Hash        string
	Plaintext   string
	Scopes      []string
	Collections []string
}

// Resolver maps a raw bearer key to its Principal. It is read-only after
// construction, so concurrent Resolve calls need no synchronization.
type Resolver struct {
	byHash map[string]Principal
}

// NewResolver builds a resolver from the configured tenants. It returns the
// human-readable warnings it accumulated (e.g. plaintext keys) so the caller
// can log them; a hard misconfiguration (bad scope, duplicate key hash,
// empty id) is a returned error and must stop startup.
func NewResolver(specs []Spec) (*Resolver, []string, error) {
	byHash := make(map[string]Principal)
	var warnings []string

	for _, s := range specs {
		if s.ID == "" {
			return nil, nil, errors.New("tenant: tenant id is required")
		}
		t := Tenant{ID: s.ID, Database: Database{URI: s.DBURI, Name: s.DBName}}

		for ki, ks := range s.Keys {
			hash, warn, err := keyHash(s.ID, ki, ks)
			if err != nil {
				return nil, nil, err
			}
			if warn != "" {
				warnings = append(warnings, warn)
			}
			scopes, err := parseScopes(s.ID, ks.Scopes)
			if err != nil {
				return nil, nil, err
			}
			if _, dup := byHash[hash]; dup {
				return nil, nil, fmt.Errorf("tenant %q: duplicate API key hash %s", s.ID, hash)
			}
			byHash[hash] = Principal{
				Tenant: t,
				Key:    APIKey{Scopes: scopes, Collections: ks.Collections},
			}
		}
	}
	return &Resolver{byHash: byHash}, warnings, nil
}

// Resolve turns the raw key from an Authorization header into a Principal.
// Lookup is by SHA-256 so plaintext keys are never held in memory after
// construction.
func (r *Resolver) Resolve(raw string) (Principal, error) {
	if raw == "" {
		return Principal{}, ErrNoKey
	}
	sum := sha256.Sum256([]byte(raw))
	p, ok := r.byHash[hashPrefix+hex.EncodeToString(sum[:])]
	if !ok {
		return Principal{}, ErrUnknownKey
	}
	return p, nil
}

func keyHash(tenantID string, idx int, ks KeySpec) (hash, warning string, err error) {
	switch {
	case ks.Hash != "" && ks.Plaintext != "":
		return "", "", fmt.Errorf("tenant %q key #%d: set either key_hash or key, not both", tenantID, idx)
	case ks.Hash != "":
		h := strings.ToLower(strings.TrimSpace(ks.Hash))
		if !strings.HasPrefix(h, hashPrefix) || len(h) != len(hashPrefix)+64 {
			return "", "", fmt.Errorf("tenant %q key #%d: key_hash must be %q + 64 hex chars", tenantID, idx, hashPrefix)
		}
		return h, "", nil
	case ks.Plaintext != "":
		sum := sha256.Sum256([]byte(ks.Plaintext))
		w := fmt.Sprintf("tenant %q key #%d: plaintext key in config; acceptable for dev only, prefer key_hash", tenantID, idx)
		return hashPrefix + hex.EncodeToString(sum[:]), w, nil
	default:
		return "", "", fmt.Errorf("tenant %q key #%d: neither key_hash nor key set", tenantID, idx)
	}
}

func parseScopes(tenantID string, raw []string) ([]Scope, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("tenant %q: a key needs at least one scope", tenantID)
	}
	scopes := make([]Scope, 0, len(raw))
	for _, s := range raw {
		sc, err := ParseScope(s)
		if err != nil {
			return nil, fmt.Errorf("tenant %q: %w", tenantID, err)
		}
		scopes = append(scopes, sc)
	}
	return scopes, nil
}
