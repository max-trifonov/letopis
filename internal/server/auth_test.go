package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/max-trifonov/letopis/internal/tenant"
)

func testResolver(t *testing.T) *tenant.Resolver {
	t.Helper()
	r, _, err := tenant.NewResolver([]tenant.Spec{
		{ID: "a", Keys: []tenant.KeySpec{{Plaintext: "key-a", Scopes: []string{"read", "write"}, Collections: []string{"crm.*"}}}},
		{ID: "b", Keys: []tenant.KeySpec{{Plaintext: "key-b", Scopes: []string{"read"}, Collections: []string{"docs.*"}}}},
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	return r
}

// collectionGuard mimics how an ingest/read handler will enforce the key's
// collection mask (FR-7.2): the collection comes from the path, the
// principal from the context, and a mismatch is 403.
func collectionGuard(w http.ResponseWriter, req *http.Request) {
	p, _ := tenant.FromContext(req.Context())
	coll := chi.URLParam(req, "collection")
	if !p.CanAccess(coll) {
		writeError(w, http.StatusForbidden, "collection not allowed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"tenant": p.Tenant.ID, "collection": coll})
}

func authedRouter(t *testing.T) http.Handler {
	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(RequireAuth(testResolver(t)))
		r.With(RequireScope(tenant.ScopeWrite)).Get("/write/{collection}", collectionGuard)
		r.Get("/read/{collection}", collectionGuard)
	})
	return r
}

func do(t *testing.T, h http.Handler, method, path, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAuthNoKey(t *testing.T) {
	rec := do(t, authedRouter(t), http.MethodGet, "/read/crm.deals", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestAuthBadKey(t *testing.T) {
	rec := do(t, authedRouter(t), http.MethodGet, "/read/crm.deals", "Bearer nope")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestAuthValidPutsTenantInContext(t *testing.T) {
	rec := do(t, authedRouter(t), http.MethodGet, "/read/crm.deals", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
}

func TestAuthInsufficientScope(t *testing.T) {
	// key-b has only read; the write route must be 403.
	rec := do(t, authedRouter(t), http.MethodGet, "/write/docs.x", "Bearer key-b")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

// Isolation: tenant A's key must not reach tenant B's collection namespace,
// no matter the path (FR-7.2).
func TestAuthCollectionIsolation(t *testing.T) {
	rec := do(t, authedRouter(t), http.MethodGet, "/read/docs.secret", "Bearer key-a")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (A must not read docs.*)", rec.Code)
	}
}
