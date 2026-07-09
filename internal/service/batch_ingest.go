package service

import (
	"context"
	"errors"
	"log/slog"
	"sort"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
)

// BatchEntry is one transport-validated batch element. Index is the element's
// original position in the request, echoed back on rejection.
type BatchEntry struct {
	Index    int
	Kind     Kind
	Command  IngestCommand
	BodySize int64 // serialized payload size, for the per-collection size limit
}

// BatchReject is one element the accept could not take, with its original index
// and a typed reason. The machine code lets a client branch without parsing the
// message; the message is human-readable detail.
type BatchReject struct {
	Index   int
	Code    string
	Message string
}

type BatchResult struct {
	Ticket   string
	Accepted int
	Rejected []BatchReject
}

// Reject codes returned to the client (BatchReject.Code).
const (
	batchRejectNotFound     = "collection_not_found"
	batchRejectTooLarge     = "too_large"
	batchRejectInvalidDiff  = "invalid_diff"
	batchRejectConflict     = "version_conflict"
	batchRejectUnavailable  = "unavailable"
	batchRejectInternalCode = "internal"
)

// BatchIngester accepts a batch over the same async pipeline as single writes;
// each item is indistinguishable from a single write once on the queue. The batch
// is not atomic: a failed item is rejected on its own, not its neighbours.
type BatchIngester struct {
	async   *AsyncIngester
	tickets *TicketService // optional; nil disables the receipt (the id is still returned)
	log     *slog.Logger
}

func NewBatchIngester(async *AsyncIngester, tickets *TicketService, log *slog.Logger) *BatchIngester {
	if log == nil {
		log = slog.Default()
	}
	return &BatchIngester{async: async, tickets: tickets, log: log.With("component", "batch-ingest")}
}

// Ingest accepts the validated entries, merges priorRejects, and returns the
// umbrella receipt. Backpressure is checked once for the whole batch.
func (b *BatchIngester) Ingest(ctx context.Context, entries []BatchEntry, priorRejects []BatchReject, modeOverride domain.ReliabilityMode) (BatchResult, error) {
	gateMode := modeOverride
	if gateMode == "" {
		gateMode = domain.ReliabilityDurable
	}
	if b.async.limiter != nil {
		if allowed, retryAfter := b.async.limiter.Allow(gateMode); !allowed {
			return BatchResult{}, &BackpressureError{RetryAfter: retryAfter}
		}
	}

	ticketID := domain.NewTicketID()
	res := BatchResult{Ticket: ticketID, Rejected: priorRejects}

	for _, e := range entries {
		if rej, ok := b.accept(ctx, ticketID, e); ok {
			res.Accepted++
		} else {
			res.Rejected = append(res.Rejected, rej)
		}
	}

	sort.Slice(res.Rejected, func(i, j int) bool { return res.Rejected[i].Index < res.Rejected[j].Index })

	status := domain.TicketAccepted
	if len(res.Rejected) > 0 {
		status = domain.TicketPartial
	}
	if b.tickets != nil {
		if err := b.tickets.OpenBatch(ctx, ticketID, status); err != nil {
			// Receipt open is best-effort: don't fail items that already shipped.
			// The client gets the id; GET may 404 if the open never landed.
			b.log.Warn("opening batch receipt", "ticket", ticketID, "err", err)
		}
	}
	return res, nil
}

// accept routes one entry: strict items are written synchronously, async items
// are enqueued under the umbrella receipt. Returns ok=true on success or dedup.
func (b *BatchIngester) accept(ctx context.Context, ticketID string, e BatchEntry) (BatchReject, bool) {
	cfg, err := b.async.sync.Config(ctx, e.Command.Collection)
	if err != nil {
		return b.mapErr(e.Index, err), false
	}
	max := cfg.MaxEventSizeBytes
	if max <= 0 {
		max = domain.DefaultMaxEventSizeBytes
	}
	if e.BodySize > max {
		return BatchReject{Index: e.Index, Code: batchRejectTooLarge, Message: "event exceeds max_event_size_bytes"}, false
	}

	mode := effectiveMode(e.Command.Mode, cfg)
	if mode == domain.ReliabilityStrict {
		if _, err := b.async.writeStrict(ctx, e.Kind, e.Command); err != nil {
			return b.mapErr(e.Index, err), false
		}
		return BatchReject{}, true
	}
	if _, err := b.async.acceptBatchItem(ctx, ticketID, e.Kind, mode, e.Command); err != nil {
		return b.mapErr(e.Index, err), false
	}
	return BatchReject{}, true
}

// mapErr turns a write/accept error into a typed reject.
func (b *BatchIngester) mapErr(index int, err error) BatchReject {
	switch {
	case errors.Is(err, ErrAutoCreateDisabled):
		return BatchReject{Index: index, Code: batchRejectNotFound, Message: "collection does not exist"}
	case errors.Is(err, ErrInvalidDiff):
		return BatchReject{Index: index, Code: batchRejectInvalidDiff, Message: "diff does not apply to current state"}
	case errors.Is(err, domain.ErrVersionConflict):
		return BatchReject{Index: index, Code: batchRejectConflict, Message: "version conflict"}
	case errors.Is(err, queue.ErrQueueUnavailable):
		return BatchReject{Index: index, Code: batchRejectUnavailable, Message: "queue unavailable, retry later"}
	default:
		b.log.Warn("batch item failed", "index", index, "err", err)
		return BatchReject{Index: index, Code: batchRejectInternalCode, Message: "internal error"}
	}
}
