package service

import (
	"context"
	"errors"
	"sort"

	"golang.org/x/sync/errgroup"

	"github.com/max-trifonov/letopis/internal/domain"
)

// catalogFanOut bounds the concurrent per-collection stat lookups. A small limit
// keeps the listing responsive without flooding the database.
const catalogFanOut = 8

// Catalog lists a tenant's collections with basic statistics. It fans out the
// cheap per-collection counters and pairs each with its effective config. No
// transport or storage knowledge — the collection mask is applied in transport,
// counters are computed in storage.
type Catalog struct {
	stats  domain.StatsRepository
	config domain.CollectionRepository
}

func NewCatalog(stats domain.StatsRepository, config domain.CollectionRepository) *Catalog {
	return &Catalog{stats: stats, config: config}
}

// ListCollections returns every collection of the tenant in name order. Summaries
// are gathered concurrently (bounded by catalogFanOut), then sorted so the result
// is deterministic regardless of fan-out completion order.
func (c *Catalog) ListCollections(ctx context.Context) ([]domain.CollectionSummary, error) {
	names, err := c.stats.ListCollections(ctx)
	if err != nil {
		return nil, err
	}

	summaries := make([]domain.CollectionSummary, len(names))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(catalogFanOut)
	for i, name := range names {
		// Each goroutine writes a distinct index — no synchronization needed
		// around summaries; the first error cancels the rest via gctx.
		g.Go(func() error {
			s, err := c.summary(gctx, name)
			if err != nil {
				return err
			}
			summaries[i] = s
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(summaries, func(i, j int) bool { return summaries[i].Name < summaries[j].Name })
	return summaries, nil
}

// summary builds one collection's entry: counters plus the effective config.
// A collection auto-created on first write has a physical ev_* but no stored
// config; GetConfig reports ErrNotFound and we present defaults, so the
// collection still lists with a meaningful config.
func (c *Catalog) summary(ctx context.Context, name string) (domain.CollectionSummary, error) {
	stats, err := c.stats.Stats(ctx, name)
	if err != nil {
		return domain.CollectionSummary{}, err
	}

	cfg, err := c.config.GetConfig(ctx, name)
	if err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			return domain.CollectionSummary{}, err
		}
		cfg = &domain.CollectionConfig{Name: name}
	}

	return domain.CollectionSummary{
		Name:   name,
		Stats:  stats,
		Config: domain.WithDefaults(*cfg),
	}, nil
}
