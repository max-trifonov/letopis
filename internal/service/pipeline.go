package service

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// Message attribute keys carried alongside the payload in queue.Message.Attrs.
// AttrEventID rides out-of-band so the worker dedup barrier acts without parsing
// the body; AttrEnqueuedAt (unix nanos) feeds the 202→stored lag gauge.
const (
	AttrEventID    = "event_id"
	AttrEnqueuedAt = "enq"
)

// ProducerMetrics is the producer-side observability hook. Nil disables
// instrumentation. Declared here so the service layer doesn't import metrics.
type ProducerMetrics interface {
	IncPublishError(queue string)
	ObserveAccept(tenant, mode string)
}

// ShardKey builds the queue routing key for an entity: all events of one entity
// hash to the same shard so a single consumer applies them in order. Exported
// so producer and any test agree on the exact form.
func ShardKey(tenantID, collection, entityID string) string {
	return tenantID + "|" + collection + "|" + entityID
}

// Pipeline serializes an ingest task and publishes it to the queue selected by
// the reliability mode. Durable and fast use separate queues so their ack
// disciplines stay independent — durable is at-least-once, fast is best-effort.
type Pipeline struct {
	durable queue.Queue
	// fast is the in-memory queue for the fast path. It is nil when this process
	// cannot host one (a split api-only role has no in-process consumer), in
	// which case fast degrades to durable rather than publishing into a void.
	fast    queue.Queue
	metrics ProducerMetrics // optional; nil disables instrumentation
	now     func() time.Time
}

func NewPipeline(durable, fast queue.Queue, metrics ProducerMetrics) *Pipeline {
	return &Pipeline{durable: durable, fast: fast, metrics: metrics, now: time.Now}
}

// Publish routes the task to the queue for mode and returns the mode actually
// used. fast degrades to durable when no in-process fast queue is configured;
// the caller reflects the effective mode (the client still gets a 202 either
// way, only the guarantee differs).
func (p *Pipeline) Publish(ctx context.Context, mode domain.ReliabilityMode, task Task) (domain.ReliabilityMode, error) {
	payload, err := Encode(task)
	if err != nil {
		return "", fmt.Errorf("service: encode task: %w", err)
	}
	attrs := map[string]string{AttrEnqueuedAt: strconv.FormatInt(p.now().UnixNano(), 10)}
	if task.Command.EventID != "" {
		attrs[AttrEventID] = task.Command.EventID
	}
	msg := queue.Message{
		Key:     ShardKey(task.Tenant.ID, task.Command.Collection, task.Command.EntityID),
		Payload: payload,
		Attrs:   attrs,
	}
	q := p.durable
	used := domain.ReliabilityDurable
	if mode == domain.ReliabilityFast && p.fast != nil {
		q, used = p.fast, domain.ReliabilityFast
	}
	if err := q.Publish(ctx, msg); err != nil {
		if p.metrics != nil {
			p.metrics.IncPublishError(string(used))
		}
		return "", err
	}
	if p.metrics != nil {
		p.metrics.ObserveAccept(task.Tenant.ID, string(used))
	}
	return used, nil
}

// AsyncIngester is the mode-aware write facade the API handlers call. Strict
// writes run synchronously; durable/fast are enqueued. The worker drives the
// inner Ingester directly — it must never re-enqueue.
type AsyncIngester struct {
	sync    *Ingester
	pipe    *Pipeline
	tickets *TicketService          // optional; nil disables ticket tracking
	idemp   domain.IdempotencyStore // optional; nil disables receipt dedup
	limiter QueueLimiter            // optional; nil disables backpressure
	log     *slog.Logger
}

// AsyncOptions carries the optional collaborators of an AsyncIngester. All
// fields are nil-safe — absent, the guarantee lapses but the path still works.
type AsyncOptions struct {
	Idempotency domain.IdempotencyStore
	Limiter     QueueLimiter
	Logger      *slog.Logger
}

func NewAsyncIngester(sync *Ingester, pipe *Pipeline, tickets *TicketService, opts AsyncOptions) *AsyncIngester {
	log := opts.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &AsyncIngester{sync: sync, pipe: pipe, tickets: tickets, idemp: opts.Idempotency, limiter: opts.Limiter, log: log.With("component", "async-ingest")}
}

// Config delegates to the inner Ingester so the handler uses a single port for
// size-limit resolution and collection provisioning.
func (a *AsyncIngester) Config(ctx context.Context, collection string) (*domain.CollectionConfig, error) {
	return a.sync.Config(ctx, collection)
}

func (a *AsyncIngester) State(ctx context.Context, cmd IngestCommand) (IngestResult, error) {
	return a.dispatch(ctx, KindState, cmd, a.sync.State)
}

func (a *AsyncIngester) Diff(ctx context.Context, cmd IngestCommand) (IngestResult, error) {
	return a.dispatch(ctx, KindDiff, cmd, a.sync.Diff)
}

func (a *AsyncIngester) Delete(ctx context.Context, cmd IngestCommand) (IngestResult, error) {
	return a.dispatch(ctx, KindDelete, cmd, a.sync.Delete)
}

// dispatch resolves the effective mode and routes to the sync or async path.
func (a *AsyncIngester) dispatch(ctx context.Context, kind Kind, cmd IngestCommand, syncFn func(context.Context, IngestCommand) (IngestResult, error)) (IngestResult, error) {
	cfg, err := a.sync.Config(ctx, cmd.Collection)
	if err != nil {
		return IngestResult{}, err
	}
	if effectiveMode(cmd.Mode, cfg) == domain.ReliabilityStrict {
		return syncFn(ctx, cmd)
	}
	return a.enqueue(ctx, kind, effectiveMode(cmd.Mode, cfg), cmd)
}

func (a *AsyncIngester) enqueue(ctx context.Context, kind Kind, mode domain.ReliabilityMode, cmd IngestCommand) (IngestResult, error) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return IngestResult{}, tenant.ErrNoKey
	}
	// Refuse before opening a ticket or claiming a dedup key, so a 429 leaves no
	// trace to clean up. The limiter reads a sampled depth so a burst at the
	// threshold does not flap per request.
	if a.limiter != nil {
		if allowed, retryAfter := a.limiter.Allow(mode); !allowed {
			return IngestResult{}, &BackpressureError{RetryAfter: retryAfter}
		}
	}

	ticketID := domain.NewTicketID()
	key, dedup := a.idempKey(cmd)
	if dedup {
		reserved, existing, err := a.idemp.Reserve(ctx, key, domain.IdempotencyRecord{TicketID: ticketID, StatusCode: http.StatusAccepted})
		switch {
		case err != nil:
			// Redis unreachable: degrade rather than refuse the write — accept undeduped
			// and let the storage {event_id} barrier catch the repeat in the worker.
			a.log.Warn("idempotency unavailable; accepting without receipt dedup", "collection", cmd.Collection, "err", err)
			dedup = false
		case !reserved:
			// A concurrent or earlier delivery already claimed this event_id: replay
			// its accept verbatim, enqueuing nothing.
			return IngestResult{EntityID: cmd.EntityID, Async: true, Ticket: existing.TicketID, Deduplicated: true}, nil
		}
	}

	if err := a.openAndPublish(ctx, ticketID, kind, mode, p, cmd); err != nil {
		// Queue down: free the dedup reservation so a retry isn't pinned to a
		// ticket that never shipped.
		if dedup {
			if rerr := a.idemp.Release(ctx, key); rerr != nil {
				a.log.Warn("releasing idempotency key after failed enqueue", "err", rerr)
			}
		}
		return IngestResult{}, err
	}
	return IngestResult{EntityID: cmd.EntityID, Async: true, Ticket: ticketID}, nil
}

// openAndPublish opens the ticket before enqueuing so a concurrent GET after the
// 202 always finds it; the worker only transitions an existing ticket, never creates one.
func (a *AsyncIngester) openAndPublish(ctx context.Context, ticketID string, kind Kind, mode domain.ReliabilityMode, p tenant.Principal, cmd IngestCommand) error {
	if a.tickets != nil {
		if err := a.tickets.Open(ctx, ticketID, cmd.Collection, cmd.EntityID); err != nil {
			return err
		}
	}
	task := Task{
		Tenant:   TenantRef{ID: p.Tenant.ID, DBURI: p.Tenant.Database.URI, DBName: p.Tenant.Database.Name},
		Kind:     kind,
		Mode:     mode,
		TicketID: ticketID,
		Command:  cmd,
	}
	_, err := a.pipe.Publish(ctx, mode, task)
	return err
}

// acceptBatchItem publishes one batch item, reusing the receipt-dedup barrier
// but opening no per-entity ticket (the batch has one umbrella receipt). Returns
// deduped=true when the event_id was already accepted.
func (a *AsyncIngester) acceptBatchItem(ctx context.Context, receiptID string, kind Kind, mode domain.ReliabilityMode, cmd IngestCommand) (bool, error) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return false, tenant.ErrNoKey
	}
	key, dedup := a.idempKey(cmd)
	if dedup {
		reserved, _, err := a.idemp.Reserve(ctx, key, domain.IdempotencyRecord{TicketID: receiptID, StatusCode: http.StatusAccepted})
		switch {
		case err != nil:
			// Redis unreachable: degrade to undeduped accept; the storage {event_id}
			// barrier still catches the repeat in the worker.
			a.log.Warn("idempotency unavailable; accepting batch item without receipt dedup", "collection", cmd.Collection, "err", err)
			dedup = false
		case !reserved:
			return true, nil
		}
	}
	task := Task{
		Tenant:  TenantRef{ID: p.Tenant.ID, DBURI: p.Tenant.Database.URI, DBName: p.Tenant.Database.Name},
		Kind:    kind,
		Mode:    mode,
		Command: cmd,
	}
	if _, err := a.pipe.Publish(ctx, mode, task); err != nil {
		if dedup {
			if rerr := a.idemp.Release(ctx, key); rerr != nil {
				a.log.Warn("releasing idempotency key after failed batch enqueue", "err", rerr)
			}
		}
		return false, err
	}
	return false, nil
}

// writeStrict runs a synchronous write for the kind (batch strict path).
func (a *AsyncIngester) writeStrict(ctx context.Context, kind Kind, cmd IngestCommand) (IngestResult, error) {
	switch kind {
	case KindState:
		return a.sync.State(ctx, cmd)
	case KindDiff:
		return a.sync.Diff(ctx, cmd)
	case KindDelete:
		return a.sync.Delete(ctx, cmd)
	default:
		return IngestResult{}, fmt.Errorf("service: unknown ingest kind %q", kind)
	}
}

// idempKey builds the dedup key, or reports that dedup does not apply (no store
// wired, or no event_id supplied). The store adds the tenant namespace from ctx.
func (a *AsyncIngester) idempKey(cmd IngestCommand) (string, bool) {
	if a.idemp == nil || cmd.EventID == "" {
		return "", false
	}
	return cmd.Collection + "|" + cmd.EventID, true
}

// effectiveMode resolves the reliability mode: per-request override wins over
// per-collection default, which already carries the instance-wide fallback.
func effectiveMode(override domain.ReliabilityMode, cfg *domain.CollectionConfig) domain.ReliabilityMode {
	if override != "" {
		return override
	}
	if cfg != nil && cfg.ReliabilityMode != "" {
		return cfg.ReliabilityMode
	}
	return domain.ReliabilityDurable
}
