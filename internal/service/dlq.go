package service

import (
	"context"
	"errors"
	"io"
	"log/slog"

	"github.com/max-trifonov/letopis/internal/domain"
)

// DeliveryRequeuer re-enqueues a parked dead letter for another delivery attempt.
// *WebhookPublisher satisfies it via Requeue. The narrow port keeps the DLQ
// service independent of the delivery infrastructure.
type DeliveryRequeuer interface {
	Requeue(ctx context.Context, dl domain.DeadLetter) error
}

// redeliverAllPageSize is the per-page fetch size when draining a whole rule's DLQ.
const redeliverAllPageSize = 200

// DLQService backs the dead-letter API: list and redeliver. A failed re-enqueue
// leaves the entry parked rather than lost.
type DLQService struct {
	repo    domain.DLQRepository
	requeue DeliveryRequeuer
	log     *slog.Logger
}

func NewDLQService(repo domain.DLQRepository, requeue DeliveryRequeuer, log *slog.Logger) *DLQService {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &DLQService{repo: repo, requeue: requeue, log: log.With("component", "dlq")}
}

func (s *DLQService) List(ctx context.Context, ruleID string, limit int, after *domain.DLQCursor) ([]domain.DeadLetter, error) {
	return s.repo.List(ctx, ruleID, limit, after)
}

// Redeliver re-enqueues dead letters for a rule. Empty ids drains the whole
// rule's DLQ; returns how many were requeued.
func (s *DLQService) Redeliver(ctx context.Context, ruleID string, ids []string) (int, error) {
	if len(ids) > 0 {
		return s.redeliverIDs(ctx, ruleID, ids)
	}
	return s.redeliverAll(ctx, ruleID)
}

func (s *DLQService) redeliverIDs(ctx context.Context, ruleID string, ids []string) (int, error) {
	requeued := 0
	for _, id := range ids {
		dl, err := s.repo.Get(ctx, id)
		if err != nil {
			return requeued, err // ErrNotFound → 404
		}
		// An id that belongs to another rule must not be redeliverable here; treat
		// it as not found to avoid crossing rule boundaries.
		if dl.RuleID != ruleID {
			return requeued, domain.ErrNotFound
		}
		if err := s.redeliverOne(ctx, *dl); err != nil {
			return requeued, err
		}
		requeued++
	}
	return requeued, nil
}

func (s *DLQService) redeliverAll(ctx context.Context, ruleID string) (int, error) {
	requeued := 0
	for {
		// Always read the first page: each redelivered entry is deleted, so the
		// window slides forward until the DLQ is empty.
		page, err := s.repo.List(ctx, ruleID, redeliverAllPageSize, nil)
		if err != nil {
			return requeued, err
		}
		if len(page) == 0 {
			return requeued, nil
		}
		for _, dl := range page {
			if err := s.redeliverOne(ctx, dl); err != nil {
				return requeued, err
			}
			requeued++
		}
		if len(page) < redeliverAllPageSize {
			return requeued, nil
		}
	}
}

// redeliverOne re-enqueues a single entry and removes it on success. A delete
// failure after a successful re-enqueue is logged but not fatal: the redelivery
// is already on the queue, so the worst case is the entry lingers and is
// redelivered again (at-least-once semantics).
func (s *DLQService) redeliverOne(ctx context.Context, dl domain.DeadLetter) error {
	if err := s.requeue.Requeue(ctx, dl); err != nil {
		return err
	}
	if err := s.repo.Delete(ctx, dl.ID); err != nil && !errors.Is(err, domain.ErrNotFound) {
		s.log.Warn("dead letter requeued but not deleted; may redeliver again", "id", dl.ID, "err", err)
	}
	return nil
}
