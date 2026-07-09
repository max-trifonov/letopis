package domain

import (
	"context"
	"errors"

	"github.com/max-trifonov/letopis/internal/rules"
)

// ErrRuleNameConflict means a rule create/update would collide with an existing
// rule's name within the same collection. Transport maps it to 409.
var ErrRuleNameConflict = errors.New("domain: rule name already exists in collection")

// Audit action names for rule changes — fixed here so storage and the CRUD service agree.
const (
	ActionRuleCreated = "rule.created"
	ActionRuleUpdated = "rule.updated"
	ActionRuleDeleted = "rule.deleted"
)

// RuleRepository persists per-collection rules in the tenant's _rules store.
// The rule model is rules.Rule — the engine's own type — so the condition tree
// and actions round-trip without a domain-side copy; storage owns the BSON mapping.
// The raw rule is stored as written; compilation happens on read into the worker cache,
// mirroring how _collections stores a raw config and applies defaults on read.
type RuleRepository interface {
	// Create stores a new rule, assigning a "rule_"+ULID id when empty, Version 1
	// and UpdatedAt. Returns ErrRuleNameConflict if the name is already used.
	Create(ctx context.Context, collection string, r *rules.Rule) error

	// Get returns the rule by id, or ErrNotFound.
	Get(ctx context.Context, collection, id string) (*rules.Rule, error)

	// List returns every rule of the collection.
	List(ctx context.Context, collection string) ([]rules.Rule, error)

	// Update replaces the rule's body, bumping Version and UpdatedAt.
	// Returns ErrNotFound or ErrRuleNameConflict as appropriate.
	Update(ctx context.Context, collection string, r *rules.Rule) error

	// Delete removes the rule, or ErrNotFound.
	Delete(ctx context.Context, collection, id string) error

	// ListAll returns every rule of the tenant grouped by collection — used for cache warm start.
	ListAll(ctx context.Context) (map[string][]rules.Rule, error)
}
