package service

import (
	"context"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// TicketService owns the async-write ticket lifecycle on top of the
// domain.TicketStore port. The single place status transitions are enforced —
// neither producer (AsyncIngester) nor consumer (worker) can write an illegal
// move; the store stays a dumb key/value with a TTL.
type TicketService struct {
	store domain.TicketStore
	now   func() time.Time
}

func NewTicketService(store domain.TicketStore) *TicketService {
	return &TicketService{store: store, now: time.Now}
}

// Open records a freshly accepted ticket for an async write. The tenant scope is
// taken from ctx by the store.
func (s *TicketService) Open(ctx context.Context, id, collection, entityID string) error {
	now := s.now().UTC()
	return s.store.Create(ctx, &domain.Ticket{
		ID:               id,
		Status:           domain.TicketAccepted,
		EntityCollection: collection,
		EntityID:         entityID,
		CreatedAt:        now,
		UpdatedAt:        now,
	})
}

// OpenBatch records the umbrella receipt for a batch accept. Unlike a single
// async write, a batch has no single entity. The receipt's status reflects
// synchronous acceptance only: accepted if every item was, partial when some
// were rejected up front. The worker never advances it, so this is the final
// state.
func (s *TicketService) OpenBatch(ctx context.Context, id string, status domain.TicketStatus) error {
	now := s.now().UTC()
	return s.store.Create(ctx, &domain.Ticket{
		ID:        id,
		Status:    status,
		CreatedAt: now,
		UpdatedAt: now,
	})
}

func (s *TicketService) Get(ctx context.Context, id string) (*domain.Ticket, error) {
	return s.store.Get(ctx, id)
}

// Mark advances a ticket to status (with an optional failure reason). Idempotent
// and reclaim-safe: re-entering the same status or attempting an illegal move from
// a terminal state is a silent no-op, so a reprocessed delivery (XAUTOCLAIM)
// never drags a stored ticket back to processing. An expired ticket is likewise
// ignored — the write still proceeds; only its tracking lapsed.
func (s *TicketService) Mark(ctx context.Context, id string, status domain.TicketStatus, reason string) error {
	if id == "" {
		return nil
	}
	t, err := s.store.Get(ctx, id)
	if err != nil {
		if err == domain.ErrTicketNotFound {
			return nil
		}
		return err
	}
	if t.Status == status || !domain.CanTransition(t.Status, status) {
		return nil
	}
	t.Status = status
	t.Error = reason
	t.UpdatedAt = s.now().UTC()
	return s.store.Save(ctx, t)
}
