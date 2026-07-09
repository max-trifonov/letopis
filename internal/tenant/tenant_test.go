package tenant

import (
	"context"
	"testing"
)

func TestTenantDatabaseName(t *testing.T) {
	tests := []struct {
		name string
		ten  Tenant
		want string
	}{
		{"derived", Tenant{ID: "acme"}, "hm_t_acme"},
		{"override name", Tenant{ID: "acme", Database: Database{Name: "acme_history"}}, "acme_history"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.ten.DatabaseName(); got != tt.want {
				t.Fatalf("DatabaseName() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPrincipalHasScope(t *testing.T) {
	p := Principal{Key: APIKey{Scopes: []Scope{ScopeWrite, ScopeRead}}}
	if !p.HasScope(ScopeWrite) {
		t.Error("expected write scope")
	}
	if p.HasScope(ScopeAdmin) {
		t.Error("did not expect admin scope")
	}
}

func TestPrincipalCanAccess(t *testing.T) {
	tests := []struct {
		name        string
		collections []string
		target      string
		want        bool
	}{
		{"wildcard all", []string{"*"}, "crm.deals", true},
		{"prefix admits child", []string{"crm.*"}, "crm.deals", true},
		{"prefix admits bare prefix", []string{"crm.*"}, "crm", true},
		{"prefix rejects other namespace", []string{"crm.*"}, "docs.x", false},
		{"prefix is not substring", []string{"crm.*"}, "crmx.deals", false},
		{"exact match", []string{"crm.deals"}, "crm.deals", true},
		{"exact rejects sibling", []string{"crm.deals"}, "crm.leads", false},
		{"empty mask grants nothing", nil, "crm.deals", false},
		{"multiple patterns", []string{"docs.*", "crm.deals"}, "crm.deals", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Principal{Key: APIKey{Collections: tt.collections}}
			if got := p.CanAccess(tt.target); got != tt.want {
				t.Fatalf("CanAccess(%q) = %v, want %v", tt.target, got, tt.want)
			}
		})
	}
}

func TestContextRoundTrip(t *testing.T) {
	want := Principal{Tenant: Tenant{ID: "acme"}}
	ctx := NewContext(context.Background(), want)
	got, ok := FromContext(ctx)
	if !ok {
		t.Fatal("FromContext: not found")
	}
	if got.Tenant.ID != "acme" {
		t.Fatalf("tenant id = %q, want acme", got.Tenant.ID)
	}
}

func TestFromContextMissing(t *testing.T) {
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("expected no principal on bare context")
	}
}
