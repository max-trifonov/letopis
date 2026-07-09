// Package worker is the consuming side of the async pipeline: reads ingest
// tasks from the queue, applies them through the service Ingester, and acks
// only after the write is committed (at-least-once). One goroutine per shard
// keeps per-entity order; shards run in parallel. Stuck, unacked work left by
// a crashed consumer is picked up by a periodic Reclaim.
package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/service"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// Ingester is the write use-case the loop drives. *service.Ingester satisfies it.
type Ingester interface {
	State(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error)
	Diff(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error)
	Delete(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error)
}

// TicketUpdater advances an async-write ticket. Nil disables ticket tracking.
type TicketUpdater interface {
	Mark(ctx context.Context, id string, status domain.TicketStatus, reason string) error
}

// Metrics receives the processing lag of each handled message. Nil disables
// instrumentation. Declared here to keep the worker free of the metrics import.
type Metrics interface {
	SetConsumerLag(queue string, seconds float64)
}

// Options tunes reclamation and ticket tracking. Reclaim defaults are
// conservative so a slow (not dead) worker is not raced for its in-flight work.
type Options struct {
	ReclaimInterval time.Duration
	ReclaimMinIdle  time.Duration
	// Tickets, when set, receives the ticket lifecycle transitions for each task.
	Tickets TicketUpdater
	// Metrics, when set, receives the per-message processing lag. QueueName
	// labels it ("durable" for this loop).
	Metrics   Metrics
	QueueName string
}

func (o Options) withDefaults() Options {
	if o.ReclaimInterval <= 0 {
		o.ReclaimInterval = 5 * time.Second
	}
	if o.ReclaimMinIdle <= 0 {
		o.ReclaimMinIdle = 30 * time.Second
	}
	if o.QueueName == "" {
		o.QueueName = "durable"
	}
	return o
}

type Worker struct {
	q       queue.Queue
	ing     Ingester
	tickets TicketUpdater
	metrics Metrics
	log     *slog.Logger
	opts    Options
}

func New(q queue.Queue, ing Ingester, log *slog.Logger, opts Options) *Worker {
	opts = opts.withDefaults()
	return &Worker{q: q, ing: ing, tickets: opts.Tickets, metrics: opts.Metrics, log: log.With("component", "worker"), opts: opts}
}

func (w *Worker) Run(ctx context.Context) error {
	n := w.q.Shards()
	w.log.Info("worker started", "shards", n)

	g, ctx := errgroup.WithContext(ctx)
	for s := range n {
		g.Go(func() error { return w.runShard(ctx, s) })
	}
	err := g.Wait()
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	w.log.Info("worker stopped")
	return err
}

// runShard reads deliveries and periodically reclaims stuck ones. New and
// reclaimed messages share the same handler.
func (w *Worker) runShard(ctx context.Context, shard int) error {
	deliveries, err := w.q.Subscribe(ctx, shard)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(w.opts.ReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return nil
			}
			w.handle(ctx, d)
		case <-ticker.C:
			w.reclaim(ctx, shard)
		}
	}
}

// handle applies one delivery and acks on success. A retryable failure is left
// unacked for Reclaim; a poison message is acked away with a loud log.
func (w *Worker) handle(ctx context.Context, d queue.Delivery) {
	err := w.process(ctx, d)
	switch {
	case err == nil:
		w.recordLag(d)
		if ackErr := w.q.Ack(ctx, d); ackErr != nil {
			w.log.Error("ack failed; message will be redelivered", "id", d.ID, "shard", d.Shard, "err", ackErr)
		}
	case isPoison(err):
		w.log.Error("dropping poison message (no DLQ yet)", "id", d.ID, "shard", d.Shard, "err", err)
		if ackErr := w.q.Ack(ctx, d); ackErr != nil {
			w.log.Error("ack of poison message failed", "id", d.ID, "err", ackErr)
		}
	default:
		// Retryable: leave unacked. Re-applying a full state is safe; the unique
		// {eid,v} index guards against duplicate versions.
		w.log.Warn("processing failed; leaving unacked for reclaim", "id", d.ID, "shard", d.Shard, "deliveries", d.Deliveries, "err", err)
	}
}

// process decodes the envelope, restores the tenant context, and dispatches to
// the matching Ingester entry point. A permanent failure settles the ticket as
// failed and wraps the error as poison; retryable errors leave the ticket in
// processing for a reclaim retry.
func (w *Worker) process(ctx context.Context, d queue.Delivery) error {
	task, err := service.Decode(d.Payload)
	if err != nil {
		return poison{err} // no ticket id is recoverable from a malformed payload
	}
	tctx := tenant.NewContext(ctx, taskPrincipal(task))
	w.markTicket(tctx, task.TicketID, domain.TicketProcessing, "")

	switch task.Kind {
	case service.KindState:
		_, err = w.ing.State(tctx, task.Command)
	case service.KindDiff:
		_, err = w.ing.Diff(tctx, task.Command)
	case service.KindDelete:
		_, err = w.ing.Delete(tctx, task.Command)
	default:
		err = poison{errUnknownKind(task.Kind)}
	}

	switch {
	case err == nil:
		w.markTicket(tctx, task.TicketID, domain.TicketStored, "")
		return nil
	case isPermanent(err):
		w.markTicket(tctx, task.TicketID, domain.TicketFailed, err.Error())
		return poison{err}
	default:
		return err
	}
}

// markTicket records a ticket transition, best-effort: a hiccup must not stall
// the pipeline.
func (w *Worker) markTicket(ctx context.Context, id string, status domain.TicketStatus, reason string) {
	if w.tickets == nil || id == "" {
		return
	}
	if err := w.tickets.Mark(ctx, id, status, reason); err != nil {
		w.log.Warn("ticket update failed", "id", id, "status", status, "err", err)
	}
}

// taskPrincipal rebuilds the storage principal from the envelope; only the
// database selectors travel, never the API key.
func taskPrincipal(task service.Task) tenant.Principal {
	return tenant.Principal{Tenant: tenant.Tenant{
		ID:       task.Tenant.ID,
		Database: tenant.Database{URI: task.Tenant.DBURI, Name: task.Tenant.DBName},
	}}
}

// isPermanent reports whether an error can never succeed on retry (poison
// envelope, unapplyable diff, or a fail-closed plugin rejection); transient
// store/queue errors are retryable.
func isPermanent(err error) bool {
	var fc *plugin.FailClosedError
	return isPoison(err) || errors.Is(err, service.ErrInvalidDiff) || errors.As(err, &fc)
}

func (w *Worker) reclaim(ctx context.Context, shard int) {
	ds, err := w.q.Reclaim(ctx, shard, w.opts.ReclaimMinIdle)
	if err != nil {
		w.log.Error("reclaim failed", "shard", shard, "err", err)
		return
	}
	for _, d := range ds {
		w.handle(ctx, d)
	}
}

// poison marks an unrecoverable error (malformed envelope, unknown kind) so the
// loop acks it away rather than looping forever.
type poison struct{ err error }

func (p poison) Error() string { return p.err.Error() }
func (p poison) Unwrap() error { return p.err }

func isPoison(err error) bool {
	var p poison
	return errors.As(err, &p)
}

func errUnknownKind(k service.Kind) error {
	return errors.New("worker: unknown task kind " + string(k))
}
