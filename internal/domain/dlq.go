package domain

import (
	"context"
	"time"
)

// DeadLetter is a webhook delivery that exhausted its retries and was parked for
// an operator. Carries everything needed to replay the delivery — the signed body,
// the target, and the secret ref — plus diagnostics to reason about why it failed.
type DeadLetter struct {
	ID         string    // "dlq_" + ULID; assigned by the store if empty
	RuleID     string    // the rule whose webhook action produced the delivery
	Collection string    // logical collection the triggering event belonged to
	DeliveryID string    // stable id carried in X-HM-Delivery across every attempt
	URL        string    // the webhook target
	SecretRef  string    // handle to the signing secret (resolved at delivery time)
	Body       []byte    // the exact bytes that were POSTed and signed
	Attempts   int       // how many delivery attempts were made before giving up
	LastError  string    // the final failure reason
	FailedAt   time.Time // when the delivery was parked (UTC); set by the store if zero
}

// DeadLetterSink is the write half of the DLQ. Declared separately so the
// dispatcher depends only on "park this", not on the full repository.
// A nil sink degrades to a logged drop.
type DeadLetterSink interface {
	Save(ctx context.Context, dl DeadLetter) error
}

// DLQCursor is a decoded DLQ pagination position. The (failed_at desc, id desc)
// pair is a total order, so paging is stable when two entries share a timestamp.
type DLQCursor struct {
	FailedAt time.Time
	ID       string
}

// DLQRepository is the per-tenant dead-letter store (_dlq). Tenant database
// comes from ctx.
type DLQRepository interface {
	DeadLetterSink

	// List returns a rule's dead letters newest-first, at most limit, after the cursor.
	List(ctx context.Context, ruleID string, limit int, after *DLQCursor) ([]DeadLetter, error)

	// Get returns a dead letter by id, or ErrNotFound.
	Get(ctx context.Context, id string) (*DeadLetter, error)

	// Delete removes a dead letter (after a successful redeliver, or a manual discard).
	Delete(ctx context.Context, id string) error

	// Count returns how many dead letters a rule has. An empty ruleID counts the whole tenant.
	Count(ctx context.Context, ruleID string) (int64, error)
}
