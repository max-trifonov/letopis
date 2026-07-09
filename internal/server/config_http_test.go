package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// fakeConfig implements ConfigService, recording the config it was handed and
// returning canned results.
type fakeConfig struct {
	stored     *domain.CollectionConfig
	getErr     error
	updated    *domain.CollectionConfig // captured Update input
	updateResp *domain.CollectionConfig
	updateErr  error
}

func (f *fakeConfig) GetStored(_ context.Context, _ string) (*domain.CollectionConfig, error) {
	return f.stored, f.getErr
}

func (f *fakeConfig) Update(_ context.Context, cfg *domain.CollectionConfig) (*domain.CollectionConfig, error) {
	f.updated = cfg
	if f.updateErr != nil {
		return nil, f.updateErr
	}
	if f.updateResp != nil {
		return f.updateResp, nil
	}
	eff := domain.WithDefaults(*cfg)
	return &eff, nil
}

// adminResolver wires a key with the admin scope (and the crm.* mask) plus a
// read+write-only key, so the same router can exercise both the happy path and
// the missing-scope 403.
func adminResolver(t *testing.T) *tenant.Resolver {
	t.Helper()
	r, _, err := tenant.NewResolver([]tenant.Spec{
		{ID: "a", Keys: []tenant.KeySpec{
			{Plaintext: "key-admin", Scopes: []string{"read", "write", "admin"}, Collections: []string{"crm.*"}},
			{Plaintext: "key-rw", Scopes: []string{"read", "write"}, Collections: []string{"crm.*"}},
		}},
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	return r
}

func configRouter(t *testing.T, svc ConfigService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), adminResolver(t), nil, nil, nil, nil, nil, svc, nil, nil, nil)
}

const configPath = "/api/v1/collections/crm.deals/config"

func put(t *testing.T, h http.Handler, path, auth, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestConfigPutValid(t *testing.T) {
	svc := &fakeConfig{}
	body := `{"reliability_mode":"strict","snapshot_interval":50,"retention":{"type":"days","days":365},"first_event_op":"update","ordering":{"mode":"received"}}`
	rec := put(t, configRouter(t, svc), configPath, "Bearer key-admin", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	// The name comes from the path, never the body.
	if svc.updated == nil || svc.updated.Name != "crm.deals" {
		t.Fatalf("service got %+v, want name crm.deals", svc.updated)
	}
	if svc.updated.ReliabilityMode != domain.ReliabilityStrict || svc.updated.SnapshotInterval != 50 {
		t.Fatalf("mapped config = %+v", svc.updated)
	}
	if svc.updated.Retention.Type != domain.RetentionDays || svc.updated.Retention.Days != 365 {
		t.Fatalf("retention = %+v", svc.updated.Retention)
	}

	var resp struct {
		Config map[string]any `json:"config"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Config["reliability_mode"] != "strict" || resp.Config["snapshot_interval"] != float64(50) {
		t.Fatalf("effective config = %+v", resp.Config)
	}
}

func TestConfigGetMarksDefaults(t *testing.T) {
	// Only reliability_mode is stored; everything else is a default.
	svc := &fakeConfig{stored: &domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityFast}}
	rec := do(t, configRouter(t, svc), http.MethodGet, configPath, "Bearer key-admin")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var resp struct {
		Config   map[string]any `json:"config"`
		Defaults []string       `json:"defaults"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Effective config carries the stored value plus filled defaults.
	if resp.Config["reliability_mode"] != "fast" || resp.Config["snapshot_interval"] != float64(domain.DefaultSnapshotInterval) {
		t.Fatalf("config = %+v", resp.Config)
	}
	// reliability_mode was set, so it must NOT be flagged as default.
	defaults := map[string]bool{}
	for _, f := range resp.Defaults {
		defaults[f] = true
	}
	if defaults["reliability_mode"] {
		t.Errorf("reliability_mode wrongly marked default")
	}
	for _, f := range []string{"snapshot_interval", "retention", "max_event_size_bytes", "first_event_op", "ordering"} {
		if !defaults[f] {
			t.Errorf("%s should be marked default; defaults=%v", f, resp.Defaults)
		}
	}
}

func TestConfigGetUnknownIs404(t *testing.T) {
	svc := &fakeConfig{getErr: domain.ErrNotFound}
	rec := do(t, configRouter(t, svc), http.MethodGet, configPath, "Bearer key-admin")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// An invalid enum surfaces from the service as *domain.ConfigError → 400.
func TestConfigPutInvalidEnum(t *testing.T) {
	svc := &fakeConfig{updateErr: &domain.ConfigError{Field: "reliability_mode", Reason: "unknown mode"}}
	rec := put(t, configRouter(t, svc), configPath, "Bearer key-admin", `{"reliability_mode":"loud"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

// A non-positive number is rejected at the transport boundary, before the
// service is touched.
func TestConfigPutNonPositiveNumber(t *testing.T) {
	svc := &fakeConfig{}
	rec := put(t, configRouter(t, svc), configPath, "Bearer key-admin", `{"snapshot_interval":0}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
	if svc.updated != nil {
		t.Fatal("service must not be called on a bad request")
	}
}

func TestConfigPutBadJSON(t *testing.T) {
	rec := put(t, configRouter(t, &fakeConfig{}), configPath, "Bearer key-admin", `{not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// The admin scope is required (NFR-5.2): a read+write key without it is 403, for
// both GET and PUT.
func TestConfigRequiresAdminScope(t *testing.T) {
	h := configRouter(t, &fakeConfig{stored: &domain.CollectionConfig{Name: "crm.deals"}})
	for _, m := range []string{http.MethodGet, http.MethodPut} {
		rec := put(t, h, configPath, "Bearer key-rw", "{}")
		if m == http.MethodGet {
			rec = do(t, h, http.MethodGet, configPath, "Bearer key-rw")
		}
		if rec.Code != http.StatusForbidden {
			t.Fatalf("%s status = %d, want 403", m, rec.Code)
		}
	}
}

// The key's collection mask is enforced: an admin key scoped to crm.* may not
// configure docs.* (FR-7.2).
func TestConfigPutOutsideMask(t *testing.T) {
	svc := &fakeConfig{}
	rec := put(t, configRouter(t, svc), "/api/v1/collections/docs.secret/config", "Bearer key-admin", "{}")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if svc.updated != nil {
		t.Fatal("masked collection must not reach the service")
	}
}
