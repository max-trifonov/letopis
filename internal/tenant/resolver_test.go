package tenant

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func hashOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hashPrefix + hex.EncodeToString(sum[:])
}

func TestResolveByHash(t *testing.T) {
	r, warns, err := NewResolver([]Spec{{
		ID:   "acme",
		Keys: []KeySpec{{Hash: hashOf("secret-a"), Scopes: []string{"write", "read"}, Collections: []string{"crm.*"}}},
	}})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if len(warns) != 0 {
		t.Fatalf("unexpected warnings: %v", warns)
	}

	p, err := r.Resolve("secret-a")
	if err != nil {
		t.Fatalf("Resolve hit: %v", err)
	}
	if p.Tenant.ID != "acme" {
		t.Fatalf("tenant = %q, want acme", p.Tenant.ID)
	}
	if !p.HasScope(ScopeWrite) || !p.CanAccess("crm.deals") {
		t.Fatal("resolved key lost scopes/collections")
	}
}

func TestResolveMiss(t *testing.T) {
	r, _, err := NewResolver([]Spec{{ID: "acme", Keys: []KeySpec{{Hash: hashOf("secret-a"), Scopes: []string{"read"}}}}})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if _, err := r.Resolve("wrong"); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("Resolve(wrong) err = %v, want ErrUnknownKey", err)
	}
	if _, err := r.Resolve(""); !errors.Is(err, ErrNoKey) {
		t.Fatalf("Resolve(empty) err = %v, want ErrNoKey", err)
	}
}

func TestPlaintextKeyWarns(t *testing.T) {
	r, warns, err := NewResolver([]Spec{{
		ID:   "dev",
		Keys: []KeySpec{{Plaintext: "hm_dev_plain", Scopes: []string{"admin"}}},
	}})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	if len(warns) != 1 || !strings.Contains(warns[0], "plaintext") {
		t.Fatalf("expected one plaintext warning, got %v", warns)
	}
	if _, err := r.Resolve("hm_dev_plain"); err != nil {
		t.Fatalf("Resolve plaintext: %v", err)
	}
}

func TestNewResolverRejects(t *testing.T) {
	tests := []struct {
		name  string
		specs []Spec
	}{
		{"empty id", []Spec{{Keys: []KeySpec{{Hash: hashOf("x"), Scopes: []string{"read"}}}}}},
		{"both forms", []Spec{{ID: "a", Keys: []KeySpec{{Hash: hashOf("x"), Plaintext: "y", Scopes: []string{"read"}}}}}},
		{"no key material", []Spec{{ID: "a", Keys: []KeySpec{{Scopes: []string{"read"}}}}}},
		{"bad hash", []Spec{{ID: "a", Keys: []KeySpec{{Hash: "sha256:short", Scopes: []string{"read"}}}}}},
		{"no scopes", []Spec{{ID: "a", Keys: []KeySpec{{Hash: hashOf("x")}}}}},
		{"unknown scope", []Spec{{ID: "a", Keys: []KeySpec{{Hash: hashOf("x"), Scopes: []string{"superuser"}}}}}},
		{"duplicate hash", []Spec{
			{ID: "a", Keys: []KeySpec{{Hash: hashOf("x"), Scopes: []string{"read"}}}},
			{ID: "b", Keys: []KeySpec{{Hash: hashOf("x"), Scopes: []string{"read"}}}},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, err := NewResolver(tt.specs); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

// Isolation is the core guarantee (FR-7.2): a key for one tenant resolves
// only to that tenant and its own collection mask, never another's.
func TestResolverIsolation(t *testing.T) {
	r, _, err := NewResolver([]Spec{
		{ID: "a", Keys: []KeySpec{{Hash: hashOf("key-a"), Scopes: []string{"read"}, Collections: []string{"crm.*"}}}},
		{ID: "b", Keys: []KeySpec{{Hash: hashOf("key-b"), Scopes: []string{"read"}, Collections: []string{"docs.*"}}}},
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	pa, _ := r.Resolve("key-a")
	if pa.Tenant.ID != "a" || pa.CanAccess("docs.secret") {
		t.Fatal("tenant A key leaked into tenant B's namespace")
	}
}
