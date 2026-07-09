package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// fakeStats implements domain.StatsRepository with canned names and per-name
// stats, so the catalog's orchestration is tested without a database.
type fakeStats struct {
	names    []string
	stats    map[string]domain.CollectionStats
	statsErr error
	listErr  error
}

func (f *fakeStats) ListCollections(context.Context) ([]string, error) {
	return f.names, f.listErr
}

func (f *fakeStats) Stats(_ context.Context, c string) (domain.CollectionStats, error) {
	if f.statsErr != nil {
		return domain.CollectionStats{}, f.statsErr
	}
	return f.stats[c], nil
}

// fakeConfigStore implements domain.CollectionRepository's GetConfig; the other
// methods are unused by the catalog and panic if called, which keeps the fake
// honest about what the use-case actually depends on.
type fakeConfigStore struct {
	configs map[string]*domain.CollectionConfig
}

func (f *fakeConfigStore) GetConfig(_ context.Context, c string) (*domain.CollectionConfig, error) {
	cfg, ok := f.configs[c]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return cfg, nil
}

func (f *fakeConfigStore) SaveConfig(context.Context, *domain.CollectionConfig) error {
	panic("unused")
}
func (f *fakeConfigStore) EnsurePhysical(context.Context, string) error { panic("unused") }

func TestCatalogListSortsAndPairsConfig(t *testing.T) {
	last := time.Unix(1700, 0).UTC()
	stats := &fakeStats{
		names: []string{"crm.deals", "docs.invoices"},
		stats: map[string]domain.CollectionStats{
			"crm.deals":     {Entities: 3, Events: 12, LastEventAt: last},
			"docs.invoices": {Entities: 0, Events: 0},
		},
	}
	cfgs := &fakeConfigStore{configs: map[string]*domain.CollectionConfig{
		"crm.deals": {Name: "crm.deals", ReliabilityMode: domain.ReliabilityStrict},
	}}

	got, err := NewCatalog(stats, cfgs).ListCollections(context.Background())
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	// Stable, name-sorted output regardless of the source order.
	if got[0].Name != "crm.deals" || got[1].Name != "docs.invoices" {
		t.Fatalf("order = %q,%q", got[0].Name, got[1].Name)
	}
	if got[0].Stats != stats.stats["crm.deals"] {
		t.Fatalf("stats = %+v", got[0].Stats)
	}
	// Explicit config is preserved; its unset fields are defaulted.
	if got[0].Config.ReliabilityMode != domain.ReliabilityStrict {
		t.Fatalf("reliability = %q, want strict", got[0].Config.ReliabilityMode)
	}
	if got[0].Config.SnapshotInterval != domain.DefaultSnapshotInterval {
		t.Fatalf("snapshot_interval = %d, want default", got[0].Config.SnapshotInterval)
	}
}

// An auto-created collection has a physical ev_* but no _collections config; it
// must still list, with the default config applied (FR-3.6, FR-6.1).
func TestCatalogAutoCreatedGetsDefaults(t *testing.T) {
	stats := &fakeStats{
		names: []string{"crm.deals"},
		stats: map[string]domain.CollectionStats{"crm.deals": {Entities: 1, Events: 1}},
	}
	cfgs := &fakeConfigStore{configs: map[string]*domain.CollectionConfig{}}

	got, err := NewCatalog(stats, cfgs).ListCollections(context.Background())
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d, want 1", len(got))
	}
	if got[0].Config.ReliabilityMode != domain.ReliabilityDurable {
		t.Fatalf("reliability = %q, want default durable", got[0].Config.ReliabilityMode)
	}
	if got[0].Config.Name != "crm.deals" {
		t.Fatalf("config name = %q", got[0].Config.Name)
	}
}

func TestCatalogEmptyList(t *testing.T) {
	got, err := NewCatalog(&fakeStats{}, &fakeConfigStore{}).ListCollections(context.Background())
	if err != nil {
		t.Fatalf("ListCollections: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}

func TestCatalogStatsErrorPropagates(t *testing.T) {
	stats := &fakeStats{names: []string{"crm.deals"}, statsErr: errors.New("boom")}
	_, err := NewCatalog(stats, &fakeConfigStore{}).ListCollections(context.Background())
	if err == nil {
		t.Fatal("want error from Stats, got nil")
	}
}
