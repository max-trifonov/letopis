package domain

import (
	"context"
	"time"
)

// AuditEvent records an administrative action against a tenant, written to
// the tenant's ev__system collection. The config API is the first producer;
// purge/forgetting and key management add their own actions in later stages.
type AuditEvent struct {
	ID         string         // "aud_" + ULID; assigned by the service if empty
	Action     string         // e.g. "collection.config.updated"
	Collection string         // logical collection the action targeted
	Actor      string         // identity that performed it (tenant id today)
	TS         time.Time      // when it happened (UTC); set by the store if zero
	Details    map[string]any // action-specific payload
}

// AuditStore appends administrative audit events to the tenant's system log.
// Recording is a side effect of an admin action, not its purpose, so callers
// treat a failure as loggable rather than fatal — the primary write has already succeeded.
type AuditStore interface {
	Record(ctx context.Context, e AuditEvent) error
}
