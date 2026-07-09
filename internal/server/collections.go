package server

import (
	"context"
	"net/http"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

type CatalogService interface {
	ListCollections(ctx context.Context) ([]domain.CollectionSummary, error)
}

type catalogHandlers struct {
	svc CatalogService
}

func (h catalogHandlers) list(w http.ResponseWriter, r *http.Request) {
	p, ok := tenant.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return
	}

	summaries, err := h.svc.ListCollections(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		if !p.CanAccess(s.Name) {
			continue // narrow to the key's collection mask
		}
		out = append(out, collectionSummaryDTO(s))
	}
	writeJSON(w, http.StatusOK, map[string]any{"collections": out})
}

// collectionSummaryDTO renders a domain summary to the wire shape. last_event_at
// is null when the collection has never been written to.
func collectionSummaryDTO(s domain.CollectionSummary) map[string]any {
	var lastAt any
	if !s.Stats.LastEventAt.IsZero() {
		lastAt = s.Stats.LastEventAt
	}
	return map[string]any{
		"name":          s.Name,
		"entities":      s.Stats.Entities,
		"events":        s.Stats.Events,
		"last_event_at": lastAt,
		"config":        collectionConfigDTO(s.Config),
	}
}

// collectionConfigDTO renders the effective per-collection config. Optional maps
// are omitted when empty.
func collectionConfigDTO(cfg domain.CollectionConfig) map[string]any {
	retention := map[string]any{"type": string(cfg.Retention.Type)}
	switch cfg.Retention.Type {
	case domain.RetentionDays:
		retention["days"] = cfg.Retention.Days
	case domain.RetentionVersions:
		retention["keep"] = cfg.Retention.Keep
	}

	ordering := map[string]any{"mode": string(cfg.Ordering.Mode)}
	if cfg.Ordering.ReorderWindowMS > 0 {
		ordering["reorder_window_ms"] = cfg.Ordering.ReorderWindowMS
	}

	out := map[string]any{
		"reliability_mode":     string(cfg.ReliabilityMode),
		"snapshot_interval":    cfg.SnapshotInterval,
		"retention":            retention,
		"max_event_size_bytes": cfg.MaxEventSizeBytes,
		"first_event_op":       string(cfg.FirstEventOp),
		"ordering":             ordering,
	}
	if len(cfg.ArrayKeys) > 0 {
		out["array_keys"] = cfg.ArrayKeys
	}
	if len(cfg.Plugins) > 0 {
		plugins := make(map[string]any, len(cfg.Plugins))
		for name, p := range cfg.Plugins {
			pm := map[string]any{"enabled": p.Enabled}
			if p.FailMode != "" {
				pm["fail_mode"] = string(p.FailMode)
			}
			if len(p.Params) > 0 {
				pm["params"] = p.Params
			}
			plugins[name] = pm
		}
		out["plugins"] = plugins
	}
	return out
}
