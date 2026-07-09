package service

import (
	"context"
	"log/slog"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/rules"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// RuleCacheInvalidator drops a collection's compiled-rule cache entry so a change
// takes effect immediately rather than after the TTL.
type RuleCacheInvalidator interface {
	InvalidateRules(ctx context.Context, collection string)
}

// RuleService backs the admin rules API: validate by compiling the condition,
// persist, audit, invalidate the cache. Same pattern as CollectionConfigService.
type RuleService struct {
	repo  domain.RuleRepository
	cache RuleCacheInvalidator // optional; nil skips invalidation
	audit domain.AuditStore    // optional; nil disables the audit trail
	log   *slog.Logger
}

func NewRuleService(repo domain.RuleRepository, cache RuleCacheInvalidator, audit domain.AuditStore, log *slog.Logger) *RuleService {
	if log == nil {
		log = slog.Default()
	}
	return &RuleService{repo: repo, cache: cache, audit: audit, log: log}
}

// Create validates and stores a new rule, then audits and invalidates. A bad
// condition or action returns *rules.RuleError → 400; a name clash returns
// domain.ErrRuleNameConflict → 409.
func (s *RuleService) Create(ctx context.Context, collection string, r *rules.Rule) (*rules.Rule, error) {
	if err := rules.ValidateRule(*r); err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, collection, r); err != nil {
		return nil, err
	}
	s.audited(ctx, domain.ActionRuleCreated, collection, r)
	s.invalidate(ctx, collection)
	return r, nil
}

// Update validates the rule, forces its id from the path, and replaces the
// stored body — Version is bumped by the repository. Unknown id → 404; name
// clash → 409.
func (s *RuleService) Update(ctx context.Context, collection, id string, r *rules.Rule) (*rules.Rule, error) {
	r.ID = id // id from the path — cannot be spoofed via the body
	if err := rules.ValidateRule(*r); err != nil {
		return nil, err
	}
	if err := s.repo.Update(ctx, collection, r); err != nil {
		return nil, err
	}
	s.audited(ctx, domain.ActionRuleUpdated, collection, r)
	s.invalidate(ctx, collection)
	return r, nil
}

func (s *RuleService) Get(ctx context.Context, collection, id string) (*rules.Rule, error) {
	return s.repo.Get(ctx, collection, id)
}

func (s *RuleService) List(ctx context.Context, collection string) ([]rules.Rule, error) {
	return s.repo.List(ctx, collection)
}

// Delete removes a rule, auditing and invalidating on success. It reads the rule
// first so the audit entry carries its name/version; an unknown id is 404 from
// either the read or the delete.
func (s *RuleService) Delete(ctx context.Context, collection, id string) error {
	r, err := s.repo.Get(ctx, collection, id)
	if err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, collection, id); err != nil {
		return err
	}
	s.audited(ctx, domain.ActionRuleDeleted, collection, r)
	s.invalidate(ctx, collection)
	return nil
}

func (s *RuleService) invalidate(ctx context.Context, collection string) {
	if s.cache != nil {
		s.cache.InvalidateRules(ctx, collection)
	}
}

// audited records a rule change in ev__system, best-effort: the rule write has
// already committed, so a failed audit is logged and swallowed.
func (s *RuleService) audited(ctx context.Context, action, collection string, r *rules.Rule) {
	if s.audit == nil {
		return
	}
	actor := ""
	if p, ok := tenant.FromContext(ctx); ok {
		actor = p.Tenant.ID
	}
	ev := domain.AuditEvent{
		ID:         domain.NewAuditID(),
		Action:     action,
		Collection: collection,
		Actor:      actor,
		TS:         time.Now().UTC(),
		Details:    map[string]any{"rule_id": r.ID, "name": r.Name, "version": r.Version},
	}
	if err := s.audit.Record(ctx, ev); err != nil {
		s.log.Warn("audit record failed", "action", action, "collection", collection, "rule_id", r.ID, "err", err)
	}
}
