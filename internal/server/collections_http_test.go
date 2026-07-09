package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/tenant"
)

var errCatalog = errors.New("catalog boom")

// fakeCatalog implements CatalogService with canned summaries.
type fakeCatalog struct {
	summaries []domain.CollectionSummary
	err       error
}

func (f *fakeCatalog) ListCollections(context.Context) ([]domain.CollectionSummary, error) {
	return f.summaries, f.err
}

func catalogRouter(t *testing.T, svc CatalogService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), testResolver(t), nil, nil, nil, nil, svc, nil, nil, nil, nil)
}

const collectionsPath = "/api/v1/collections"

func sampleSummaries() []domain.CollectionSummary {
	return []domain.CollectionSummary{
		{
			Name:   "crm.deals",
			Stats:  domain.CollectionStats{Entities: 3, Events: 12, LastEventAt: time.Unix(1700, 0).UTC()},
			Config: domain.WithDefaults(domain.CollectionConfig{Name: "crm.deals"}),
		},
		{
			Name:   "docs.invoices", // outside key-a's crm.* mask
			Stats:  domain.CollectionStats{Entities: 0, Events: 0},
			Config: domain.WithDefaults(domain.CollectionConfig{Name: "docs.invoices"}),
		},
	}
}

func TestCollectionsListGolden(t *testing.T) {
	h := catalogRouter(t, &fakeCatalog{summaries: sampleSummaries()})
	rec := do(t, h, http.MethodGet, collectionsPath, "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var body struct {
		Collections []struct {
			Name        string         `json:"name"`
			Entities    int64          `json:"entities"`
			Events      int64          `json:"events"`
			LastEventAt *time.Time     `json:"last_event_at"`
			Config      map[string]any `json:"config"`
		} `json:"collections"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// key-a's mask is crm.*, so docs.invoices must be filtered out (FR-6.2).
	if len(body.Collections) != 1 {
		t.Fatalf("collections = %d, want 1 (mask applied); body=%s", len(body.Collections), rec.Body.String())
	}
	c := body.Collections[0]
	if c.Name != "crm.deals" || c.Entities != 3 || c.Events != 12 {
		t.Fatalf("summary = %+v", c)
	}
	if c.LastEventAt == nil || !c.LastEventAt.Equal(time.Unix(1700, 0).UTC()) {
		t.Fatalf("last_event_at = %v", c.LastEventAt)
	}
	if c.Config["reliability_mode"] != "durable" || c.Config["snapshot_interval"] != float64(100) {
		t.Fatalf("config = %+v", c.Config)
	}
}

// A collection with no events reports zeroed counters and a null last_event_at,
// not an error (FR-3.6).
func TestCollectionsListEmptyCollection(t *testing.T) {
	h := catalogRouter(t, &fakeCatalog{summaries: []domain.CollectionSummary{{
		Name:   "crm.empty",
		Config: domain.WithDefaults(domain.CollectionConfig{Name: "crm.empty"}),
	}}})
	rec := do(t, h, http.MethodGet, collectionsPath, "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Collections []struct {
			Events      int64      `json:"events"`
			Entities    int64      `json:"entities"`
			LastEventAt *time.Time `json:"last_event_at"`
		} `json:"collections"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Collections) != 1 {
		t.Fatalf("collections = %d, want 1", len(body.Collections))
	}
	c := body.Collections[0]
	if c.Events != 0 || c.Entities != 0 || c.LastEventAt != nil {
		t.Fatalf("want zeros and null last_event_at, got %+v", c)
	}
}

// The read scope is required (FR-6.2): a key without it is 403. The shared
// testResolver has no read-less key, so this test wires its own write-only key.
func TestCollectionsListScopeRequired(t *testing.T) {
	resolver, _, err := tenant.NewResolver([]tenant.Spec{
		{ID: "w", Keys: []tenant.KeySpec{{Plaintext: "key-w", Scopes: []string{"write"}, Collections: []string{"*"}}}},
	})
	if err != nil {
		t.Fatalf("NewResolver: %v", err)
	}
	h := newRouter(health.NewRegistry(), resolver, nil, nil, nil, nil, &fakeCatalog{summaries: sampleSummaries()}, nil, nil, nil, nil)
	rec := do(t, h, http.MethodGet, collectionsPath, "Bearer key-w")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestCollectionsListInternalError(t *testing.T) {
	h := catalogRouter(t, &fakeCatalog{err: errCatalog})
	rec := do(t, h, http.MethodGet, collectionsPath, "Bearer key-a")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
}
