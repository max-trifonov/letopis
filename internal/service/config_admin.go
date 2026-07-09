package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// configInvalidator is the slice of the config resolver the admin service needs:
// drop a collection's cached config after a write so the new settings take effect
// immediately rather than after the TTL. *ConfigResolver satisfies it.
type configInvalidator interface {
	Invalidate(ctx context.Context, collection string)
}

// CollectionConfigService backs the admin config API. Validates and persists a
// per-collection config, provisions the physical collections, invalidates the
// resolver cache so the change is seen at once, and records the change in the
// tenant's audit log. Reads return the stored config without defaults applied so
// transport can mark which values are defaults.
type CollectionConfigService struct {
	repo  domain.CollectionRepository
	cache configInvalidator
	audit domain.AuditStore // optional; nil disables the audit trail
	log   *slog.Logger
}

func NewCollectionConfigService(repo domain.CollectionRepository, cache configInvalidator, audit domain.AuditStore, log *slog.Logger) *CollectionConfigService {
	if log == nil {
		log = slog.Default()
	}
	return &CollectionConfigService{repo: repo, cache: cache, audit: audit, log: log}
}

// GetStored returns the collection's stored config as written, or
// domain.ErrNotFound when the collection has no config. Defaults are not applied
// here: the caller needs the raw record to tell a defaulted field from an
// explicitly set one.
func (s *CollectionConfigService) GetStored(ctx context.Context, collection string) (*domain.CollectionConfig, error) {
	return s.repo.GetConfig(ctx, collection)
}

// Update validates cfg, persists it, provisions the physical ev_*/cur_*/sn_*
// collections (idempotent), invalidates the resolver cache, and records the
// change in ev__system. Returns the effective config (with defaults applied).
// Validation failures surface as *domain.ConfigError → 400. The audit write is
// best-effort — a failed audit must not fail an applied config change.
func (s *CollectionConfigService) Update(ctx context.Context, cfg *domain.CollectionConfig) (*domain.CollectionConfig, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	// Persist the config as supplied — only what the admin set. Defaults are
	// applied on read (the resolver and GET both call WithDefaults), so storing
	// the raw record is what lets GET mark which values are defaults vs explicit.
	if err := s.repo.SaveConfig(ctx, cfg); err != nil {
		return nil, err
	}
	if err := s.repo.EnsurePhysical(ctx, cfg.Name); err != nil {
		return nil, err
	}
	if s.cache != nil {
		s.cache.Invalidate(ctx, cfg.Name)
	}
	effective := domain.WithDefaults(*cfg)
	s.recordAudit(ctx, effective)
	return &effective, nil
}

// recordAudit appends a "collection.config.updated" entry to the tenant's
// ev__system log. Best-effort: a failure is logged and swallowed, since the
// config change has already been applied.
func (s *CollectionConfigService) recordAudit(ctx context.Context, cfg domain.CollectionConfig) {
	if s.audit == nil {
		return
	}
	actor := ""
	if p, ok := tenant.FromContext(ctx); ok {
		actor = p.Tenant.ID
	}
	ev := domain.AuditEvent{
		ID:         domain.NewAuditID(),
		Action:     "collection.config.updated",
		Collection: cfg.Name,
		Actor:      actor,
		TS:         time.Now().UTC(),
		Details:    auditDetails(cfg),
	}
	if err := s.audit.Record(ctx, ev); err != nil {
		s.log.Warn("audit record failed", "action", ev.Action, "collection", cfg.Name, "err", err)
	}
}

// auditDetails captures the effective config so the audit log answers "what was
// it set to" without a second lookup.
func auditDetails(cfg domain.CollectionConfig) map[string]any {
	return map[string]any{
		"reliability_mode":     string(cfg.ReliabilityMode),
		"snapshot_interval":    cfg.SnapshotInterval,
		"retention_type":       string(cfg.Retention.Type),
		"max_event_size_bytes": cfg.MaxEventSizeBytes,
		"first_event_op":       string(cfg.FirstEventOp),
		"ordering_mode":        string(cfg.Ordering.Mode),
	}
}
