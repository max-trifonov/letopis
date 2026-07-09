package worker

import (
	"context"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/service"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// BatchIngester is the bulk write use-case the fast loop drives.
// *service.Ingester satisfies it.
type BatchIngester interface {
	IngestBatch(ctx context.Context, items []service.BatchItem) []error
}

// Default batcher tuning: flush at 500 accumulated events or 50ms, whichever
// comes first.
const (
	defaultBatchSize   = 500
	defaultBatchLinger = 50 * time.Millisecond
	// flushTimeout bounds the best-effort flush of whatever is buffered when the
	// loop is shutting down, since the loop context is already cancelled.
	flushTimeout = 5 * time.Second
)

type BatchOptions struct {
	Size    int
	Linger  time.Duration
	Tickets TicketUpdater
	// Metrics, when set, receives the per-flush processing lag, labelled
	// by QueueName ("fast").
	Metrics   Metrics
	QueueName string
}

func (o BatchOptions) withDefaults() BatchOptions {
	if o.Size <= 0 {
		o.Size = defaultBatchSize
	}
	if o.Linger <= 0 {
		o.Linger = defaultBatchLinger
	}
	if o.QueueName == "" {
		o.QueueName = "fast"
	}
	return o
}

// BatchWorker drains the in-memory queue, accumulates deliveries up to a size
// or a linger deadline, and flushes with a single insertMany. Unlike the durable
// Worker it does not reclaim: fast mode is at-most-once, lost on crash by design.
type BatchWorker struct {
	q       queue.Queue
	ing     BatchIngester
	tickets TicketUpdater
	metrics Metrics
	log     *slog.Logger
	opts    BatchOptions
}

func NewBatchWorker(q queue.Queue, ing BatchIngester, log *slog.Logger, opts BatchOptions) *BatchWorker {
	opts = opts.withDefaults()
	return &BatchWorker{q: q, ing: ing, tickets: opts.Tickets, metrics: opts.Metrics, log: log.With("component", "batch-worker"), opts: opts}
}

func (b *BatchWorker) Run(ctx context.Context) error {
	n := b.q.Shards()
	b.log.Info("batch worker started", "shards", n, "batch_size", b.opts.Size, "linger", b.opts.Linger)
	g, ctx := errgroup.WithContext(ctx)
	for s := range n {
		g.Go(func() error { return b.runShard(ctx, s) })
	}
	err := g.Wait()
	b.log.Info("batch worker stopped")
	return err
}

// runShard accumulates deliveries and flushes on a full batch or linger deadline.
// On shutdown it flushes the tail on a detached context so nothing already
// enqueued is lost.
func (b *BatchWorker) runShard(ctx context.Context, shard int) error {
	deliveries, err := b.q.Subscribe(ctx, shard)
	if err != nil {
		return err
	}
	timer := time.NewTimer(b.opts.Linger)
	timer.Stop()
	var buf []queue.Delivery

	flush := func(c context.Context) {
		if len(buf) == 0 {
			return
		}
		b.flush(c, buf)
		buf = nil
		timer.Stop()
		select {
		case <-timer.C:
		default:
		}
	}

	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), flushTimeout)
			flush(fctx)
			cancel()
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			if len(buf) == 0 {
				timer.Reset(b.opts.Linger)
			}
			buf = append(buf, d)
			if len(buf) >= b.opts.Size {
				flush(ctx)
			}
		case <-timer.C:
			flush(ctx)
		}
	}
}

// flush applies one accumulated batch grouped by tenant, each under its own
// tenant context. Every delivery is acked regardless of outcome — fast mode is
// best-effort and never reclaims.
func (b *BatchWorker) flush(ctx context.Context, batch []queue.Delivery) {
	groups, order := b.group(batch)
	for _, key := range order {
		g := groups[key]
		tctx := tenant.NewContext(ctx, taskPrincipal(service.Task{Tenant: g.ref}))
		errs := b.ing.IngestBatch(tctx, g.items)
		for i, item := range g.items {
			b.settle(tctx, g.tickets[i], errs[i], item)
		}
	}
	for _, d := range batch {
		if err := b.q.Ack(ctx, d); err != nil {
			b.log.Warn("ack failed (fast path, message dropped)", "id", d.ID, "shard", d.Shard, "err", err)
		}
	}
	b.recordLag(batch)
}

// recordLag reports the processing lag of the batch's last delivery: one gauge
// update per flush, not per item.
func (b *BatchWorker) recordLag(batch []queue.Delivery) {
	if b.metrics == nil || len(batch) == 0 {
		return
	}
	last := batch[len(batch)-1]
	if secs, ok := lagSeconds(last.Attrs[service.AttrEnqueuedAt], time.Now()); ok {
		b.metrics.SetConsumerLag(b.opts.QueueName, secs)
	}
}

// settle records the per-item ticket outcome, transitioning directly to a
// terminal state (accepted→stored is legal; the processing step is skipped).
func (b *BatchWorker) settle(ctx context.Context, ticketID string, err error, item service.BatchItem) {
	if b.tickets == nil || ticketID == "" {
		return
	}
	status, reason := domain.TicketStored, ""
	if err != nil {
		status, reason = domain.TicketFailed, err.Error()
		b.log.Warn("fast write failed", "collection", item.Command.Collection, "entity", item.Command.EntityID, "err", err)
	}
	if merr := b.tickets.Mark(ctx, ticketID, status, reason); merr != nil {
		b.log.Warn("ticket update failed", "id", ticketID, "status", status, "err", merr)
	}
}

// tenantGroup is one tenant's slice of a flushed batch. Items and ticket ids
// are in arrival order so IngestBatch's aligned results map to the right tickets.
type tenantGroup struct {
	ref     service.TenantRef
	items   []service.BatchItem
	tickets []string
}

// group splits a batch by tenant, preserving arrival order. A malformed payload
// is logged and skipped; it has no recoverable ticket.
func (b *BatchWorker) group(batch []queue.Delivery) (map[string]*tenantGroup, []string) {
	groups := map[string]*tenantGroup{}
	order := []string{}
	for _, d := range batch {
		task, err := service.Decode(d.Payload)
		if err != nil {
			b.log.Error("dropping malformed fast payload", "id", d.ID, "err", err)
			continue
		}
		key := task.Tenant.ID + "|" + task.Tenant.DBName + "|" + task.Tenant.DBURI
		g, ok := groups[key]
		if !ok {
			g = &tenantGroup{ref: task.Tenant}
			groups[key] = g
			order = append(order, key)
		}
		g.items = append(g.items, service.BatchItem{Kind: task.Kind, Command: task.Command})
		g.tickets = append(g.tickets, task.TicketID)
	}
	return groups, order
}
