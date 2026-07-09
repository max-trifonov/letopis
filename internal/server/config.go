package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

type ConfigService interface {
	GetStored(ctx context.Context, collection string) (*domain.CollectionConfig, error)
	Update(ctx context.Context, cfg *domain.CollectionConfig) (*domain.CollectionConfig, error)
}

type configHandlers struct {
	svc ConfigService
}

func (h configHandlers) get(w http.ResponseWriter, r *http.Request) {
	p, ok := tenant.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return
	}
	collection := chi.URLParam(r, "collection")
	if !p.CanAccess(collection) {
		writeError(w, http.StatusForbidden, "collection not allowed")
		return
	}

	stored, err := h.svc.GetStored(r.Context(), collection)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "collection not configured")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	effective := domain.WithDefaults(*stored)
	writeJSON(w, http.StatusOK, map[string]any{
		"config":   collectionConfigDTO(effective),
		"defaults": defaultedFields(*stored),
	})
}

func (h configHandlers) put(w http.ResponseWriter, r *http.Request) {
	p, ok := tenant.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return
	}
	collection := chi.URLParam(r, "collection")
	if !p.CanAccess(collection) {
		writeError(w, http.StatusForbidden, "collection not allowed")
		return
	}

	cfg, err := decodeConfigRequest(r, collection)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	effective, err := h.svc.Update(r.Context(), cfg)
	if err != nil {
		var ce *domain.ConfigError
		if errors.As(err, &ce) {
			writeError(w, http.StatusBadRequest, ce.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"config": collectionConfigDTO(*effective)})
}

// collectionConfigRequest is the PUT /config body. Scalar fields are pointers so
// an omitted field is distinguishable from an explicit value: only the latter is
// range-checked, so a client can clear a field back to the default by omitting it.
type collectionConfigRequest struct {
	ReliabilityMode   *string                  `json:"reliability_mode"`
	SnapshotInterval  *int                     `json:"snapshot_interval"`
	Retention         *retentionRequest        `json:"retention"`
	MaxEventSizeBytes *int64                   `json:"max_event_size_bytes"`
	FirstEventOp      *string                  `json:"first_event_op"`
	Ordering          *orderingRequest         `json:"ordering"`
	ArrayKeys         map[string]string        `json:"array_keys"`
	Plugins           map[string]pluginRequest `json:"plugins"`
}

type retentionRequest struct {
	Type string `json:"type"`
	Days int    `json:"days"`
	Keep int    `json:"keep"`
}

type orderingRequest struct {
	Mode            string `json:"mode"`
	ReorderWindowMS int    `json:"reorder_window_ms"`
}

type pluginRequest struct {
	Enabled  bool           `json:"enabled"`
	FailMode string         `json:"fail_mode"`
	Params   map[string]any `json:"params"`
}

// decodeConfigRequest parses and range-checks the body, then maps it onto a
// domain config. It enforces the positive-value rule for explicitly supplied
// numbers (the domain validator only sees the mapped config, where an omitted
// field and an explicit zero look the same); enum validity is delegated to the
// domain validator, so the rules live in one place.
func decodeConfigRequest(r *http.Request, collection string) (*domain.CollectionConfig, error) {
	var body collectionConfigRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return nil, errors.New("invalid JSON body")
	}

	cfg := domain.CollectionConfig{Name: collection}
	if body.ReliabilityMode != nil {
		cfg.ReliabilityMode = domain.ReliabilityMode(*body.ReliabilityMode)
	}
	if body.SnapshotInterval != nil {
		if *body.SnapshotInterval <= 0 {
			return nil, errors.New("snapshot_interval must be positive")
		}
		cfg.SnapshotInterval = *body.SnapshotInterval
	}
	if body.MaxEventSizeBytes != nil {
		if *body.MaxEventSizeBytes <= 0 {
			return nil, errors.New("max_event_size_bytes must be positive")
		}
		cfg.MaxEventSizeBytes = *body.MaxEventSizeBytes
	}
	if body.FirstEventOp != nil {
		cfg.FirstEventOp = domain.FirstEventOp(*body.FirstEventOp)
	}
	if body.Retention != nil {
		cfg.Retention = domain.Retention{
			Type: domain.RetentionType(body.Retention.Type),
			Days: body.Retention.Days,
			Keep: body.Retention.Keep,
		}
	}
	if body.Ordering != nil {
		cfg.Ordering = domain.Ordering{
			Mode:            domain.OrderingMode(body.Ordering.Mode),
			ReorderWindowMS: body.Ordering.ReorderWindowMS,
		}
	}
	cfg.ArrayKeys = body.ArrayKeys
	if len(body.Plugins) > 0 {
		cfg.Plugins = make(map[string]domain.PluginConfig, len(body.Plugins))
		for name, p := range body.Plugins {
			cfg.Plugins[name] = domain.PluginConfig{
				Enabled:  p.Enabled,
				FailMode: domain.FailMode(p.FailMode),
				Params:   p.Params,
			}
		}
	}
	return &cfg, nil
}

// defaultedFields names the config fields the stored record left unset (which
// WithDefaults therefore filled). It mirrors the WithDefaults logic field for
// field; the duplication is cheaper than a reflective comparison.
func defaultedFields(stored domain.CollectionConfig) []string {
	out := []string{}
	if stored.ReliabilityMode == "" {
		out = append(out, "reliability_mode")
	}
	if stored.SnapshotInterval == 0 {
		out = append(out, "snapshot_interval")
	}
	if stored.Retention.Type == "" {
		out = append(out, "retention")
	}
	if stored.MaxEventSizeBytes == 0 {
		out = append(out, "max_event_size_bytes")
	}
	if stored.FirstEventOp == "" {
		out = append(out, "first_event_op")
	}
	if stored.Ordering.Mode == "" {
		out = append(out, "ordering")
	}
	return out
}
