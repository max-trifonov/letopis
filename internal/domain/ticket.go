package domain

import (
	"context"
	"errors"
	"time"
)

// TicketStatus is the lifecycle of an asynchronously accepted write.
type TicketStatus string

const (
	TicketAccepted   TicketStatus = "accepted"
	TicketProcessing TicketStatus = "processing"
	TicketStored     TicketStatus = "stored"
	TicketFailed     TicketStatus = "failed"
	// TicketPartial is the terminal status of a batch receipt where some items were
	// rejected synchronously. The worker never advances it.
	TicketPartial TicketStatus = "partial"
)

// Terminal reports whether the status is an end state. The reclaim path relies
// on this: a reprocessed delivery must not drag a stored ticket back to processing.
func (s TicketStatus) Terminal() bool {
	return s == TicketStored || s == TicketFailed || s == TicketPartial
}

// CanTransition encodes the legal status moves. Pure function so the rule lives
// in one place and is unit-tested in isolation; the ticket service consults it
// before persisting an update. Re-entering the same status is rejected here —
// callers short-circuit on equality.
func CanTransition(from, to TicketStatus) bool {
	switch from {
	case TicketAccepted:
		return to == TicketProcessing || to == TicketStored || to == TicketFailed
	case TicketProcessing:
		return to == TicketStored || to == TicketFailed
	default:
		// stored / failed / partial are terminal
		return false
	}
}

// ErrTicketNotFound is returned when the id is unknown or its TTL has lapsed.
// Transport maps it to 404; an expired ticket is indistinguishable from one
// that never existed, so callers can't confirm key validity.
var ErrTicketNotFound = errors.New("domain: ticket not found")

// Ticket tracks one asynchronously accepted write. The tenant is implicit —
// the store namespaces tickets by the request tenant, so a ticket id is only
// resolvable within the tenant that created it.
type Ticket struct {
	ID               string
	Status           TicketStatus
	EntityCollection string
	EntityID         string
	Error            string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// TicketStore persists tickets with a TTL. Tenant database/namespace is taken from ctx.
type TicketStore interface {
	// Create records a new ticket. The ticket's id and timestamps are assigned by the caller.
	Create(ctx context.Context, t *Ticket) error

	// Get returns the ticket by id, or ErrTicketNotFound when it is unknown or expired.
	Get(ctx context.Context, id string) (*Ticket, error)

	// Save overwrites an existing ticket (a status transition). Returns ErrTicketNotFound
	// if the ticket has expired out from under the update.
	Save(ctx context.Context, t *Ticket) error
}
