package domain

import "context"

// IdempotencyRecord is what the receipt-dedup barrier caches under an idempotency key:
// just enough to replay the original accept on a repeat. StatusCode pins the code the
// first delivery answered; TicketID lets the repeat point at the same async ticket.
type IdempotencyRecord struct {
	TicketID   string
	StatusCode int
}

// IdempotencyStore is the receipt-dedup barrier — the first of two idempotency rubicons
// (the second is the storage {event_id} index). Tenant namespace is taken from ctx,
// so one tenant can never dedup against another's keys.
type IdempotencyStore interface {
	// Reserve atomically claims key for rec with the configured TTL (SET NX EX).
	// When the key was free this is the first delivery: reserved=true, proceed with the write.
	// When already held this is a repeat: reserved=false, existing carries the record the
	// original delivery saved — replay that response instead of enqueuing a duplicate.
	Reserve(ctx context.Context, key string, rec IdempotencyRecord) (reserved bool, existing IdempotencyRecord, err error)

	// Release drops a reservation. Call when the write could not be enqueued after the claim,
	// so a client retry isn't forever pinned to a ticket that was never honoured.
	Release(ctx context.Context, key string) error
}
