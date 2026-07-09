package delivery

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// reasonUnknownSecret is the DLQ reason for a delivery whose secret_ref does not
// resolve. It is a configuration error, not a transient one, so the delivery is
// parked immediately without retries.
const reasonUnknownSecret = "unknown_secret"

// Config tunes the dispatcher. Zero values fall back to the package defaults, so
// a caller can pass an empty Config and get sane behaviour. MaxAttempts and the
// timeout here are the instance defaults; a task (from a rule) may carry its own.
type Config struct {
	DefaultTimeout  time.Duration
	MaxAttempts     int
	Backoff         BackoffConfig
	ReclaimInterval time.Duration
	ReclaimMinIdle  time.Duration
}

func (c Config) withDefaults() Config {
	if c.DefaultTimeout <= 0 {
		c.DefaultTimeout = defaultTimeout
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.Backoff.Base <= 0 {
		c.Backoff.Base = defaultBackoffBase
	}
	if c.Backoff.Max <= 0 {
		c.Backoff.Max = defaultBackoffMax
	}
	if c.ReclaimInterval <= 0 {
		c.ReclaimInterval = 5 * time.Second
	}
	if c.ReclaimMinIdle <= 0 {
		c.ReclaimMinIdle = 30 * time.Second
	}
	return c
}

// DeliveryMetrics is the narrow observability port the dispatcher uses. A nil
// implementation is always safe — all methods are no-ops.
type DeliveryMetrics interface {
	ObserveDelivery(result string, duration time.Duration)
	IncDeliveryRetry()
}

// Options carries the dispatcher's collaborators. Transport is the SSRF
// injection point: nil uses the default transport, which performs no address
// filtering — a deployment exposed to untrusted rule URLs must inject a guarded
// transport. Sink is the DLQ; nil logs an exhausted delivery rather than
// persisting it. Jitter is injected for deterministic tests; nil uses
// crypto-free full jitter.
type Options struct {
	Transport http.RoundTripper
	Sink      domain.DeadLetterSink
	Jitter    func(d time.Duration) time.Duration
	Metrics   DeliveryMetrics
}

// Dispatcher consumes the delivery queue and delivers each webhook. One goroutine
// per shard mirrors worker.Worker; deliveries are not ordered between themselves,
// so the per-shard discipline is only for parallelism, not ordering.
type Dispatcher struct {
	q       queue.Queue
	client  *http.Client
	secrets map[string]string
	sink    domain.DeadLetterSink
	cfg     Config
	jitter  func(d time.Duration) time.Duration
	met     DeliveryMetrics
	log     *slog.Logger

	retries sync.WaitGroup // tracks in-flight scheduled re-publishes
}

// New builds a dispatcher. The HTTP client wraps Options.Transport so the SSRF
// guard is a drop-in; secrets maps a task's secret_ref to its signing secret.
func New(q queue.Queue, secrets map[string]string, cfg Config, opts Options, log *slog.Logger) *Dispatcher {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	jitter := opts.Jitter
	if jitter == nil {
		jitter = fullJitter
	}
	return &Dispatcher{
		q:       q,
		client:  &http.Client{Transport: opts.Transport},
		secrets: secrets,
		sink:    opts.Sink,
		cfg:     cfg.withDefaults(),
		jitter:  jitter,
		met:     opts.Metrics,
		log:     log.With("component", "webhook-dispatcher"),
	}
}

// Run consumes every shard until the context is cancelled, then waits for any
// scheduled retries to settle. A clean shutdown returns nil so the errgroup
// above stays clean (mirrors worker.Worker.Run).
func (d *Dispatcher) Run(ctx context.Context) error {
	n := d.q.Shards()
	d.log.Info("webhook dispatcher started", "shards", n)

	g, gctx := errgroup.WithContext(ctx)
	for s := range n {
		g.Go(func() error { return d.runShard(gctx, s) })
	}
	err := g.Wait()
	d.retries.Wait() // let pending re-publishes flush before returning
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	d.log.Info("webhook dispatcher stopped")
	return err
}

func (d *Dispatcher) runShard(ctx context.Context, shard int) error {
	deliveries, err := d.q.Subscribe(ctx, shard)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(d.cfg.ReclaimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case del, ok := <-deliveries:
			if !ok {
				return nil
			}
			d.handle(ctx, del)
		case <-ticker.C:
			d.reclaim(ctx, shard)
		}
	}
}

func (d *Dispatcher) reclaim(ctx context.Context, shard int) {
	ds, err := d.q.Reclaim(ctx, shard, d.cfg.ReclaimMinIdle)
	if err != nil {
		d.log.Error("delivery reclaim failed", "shard", shard, "err", err)
		return
	}
	for _, del := range ds {
		d.handle(ctx, del)
	}
}

// handle delivers one task and always acks it: success removes it, a permanent
// failure parks it in the DLQ, and a retryable failure re-publishes a delayed
// copy (with attempt+1) before acking the current one. The current message is
// always removed so a shard slot is never held during a backoff wait.
func (d *Dispatcher) handle(ctx context.Context, del queue.Delivery) {
	task, err := DecodeTask(del.Payload)
	if err != nil {
		// A malformed payload can never succeed: log loudly and ack it away.
		d.log.Error("dropping malformed delivery task", "id", del.ID, "shard", del.Shard, "err", err)
		d.ack(ctx, del)
		return
	}

	d.process(ctx, task)
	d.ack(ctx, del)
}

// process performs one delivery attempt and decides what happens next. An
// unknown secret_ref is a configuration error → straight to the DLQ, no retries.
// A 2xx is success. Anything else (non-2xx, timeout, transport error) is
// retryable: schedule the next attempt, or park in the DLQ once attempts are
// exhausted.
func (d *Dispatcher) process(ctx context.Context, task Task) {
	secret, ok := d.secrets[task.SecretRef]
	if !ok {
		d.log.Error("unknown secret_ref; parking delivery without retry", "rule_id", task.RuleID, "secret_ref", task.SecretRef, "delivery_id", task.DeliveryID)
		d.toDLQ(ctx, task, reasonUnknownSecret)
		d.obsDelivery("dropped", 0)
		return
	}

	start := time.Now()
	err := d.deliver(ctx, task, secret)
	elapsed := time.Since(start)

	if err == nil {
		d.obsDelivery("delivered", elapsed)
		return // 2xx: delivered
	}

	// SSRF-blocked: not retryable, park immediately.
	if IsSSRFBlocked(err) {
		d.log.Warn("delivery blocked by SSRF guard; parking in DLQ", "rule_id", task.RuleID, "delivery_id", task.DeliveryID, "err", err)
		d.toDLQ(ctx, task, err.Error())
		d.obsDelivery("blocked", elapsed)
		return
	}

	d.obsDelivery("failed", elapsed)

	maxAttempts := task.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = d.cfg.MaxAttempts
	}
	// task.Attempt is the number of attempts already made before this one; this
	// call is attempt number task.Attempt+1. Exhausted when that reaches the cap.
	attemptsMade := task.Attempt + 1
	if attemptsMade >= maxAttempts {
		d.log.Warn("delivery exhausted retries; parking in DLQ", "rule_id", task.RuleID, "delivery_id", task.DeliveryID, "attempts", attemptsMade, "err", err)
		task.Attempt = attemptsMade
		d.toDLQ(ctx, task, err.Error())
		return
	}

	delay := nextDelay(task.Attempt, d.cfg.Backoff, d.jitter)
	d.log.Warn("delivery failed; scheduling retry", "rule_id", task.RuleID, "delivery_id", task.DeliveryID, "attempt", attemptsMade, "retry_in", delay, "err", err)
	next := task
	next.Attempt = attemptsMade
	if d.met != nil {
		d.met.IncDeliveryRetry()
	}
	d.scheduleRetry(ctx, next, delay)
}

func (d *Dispatcher) obsDelivery(result string, dur time.Duration) {
	if d.met != nil {
		d.met.ObserveDelivery(result, dur)
	}
}

// deliver makes one HTTP POST, signed and with the delivery headers. A 2xx is
// success; any other status, a timeout, or a transport error is a retryable
// failure returned as an error.
func (d *Dispatcher) deliver(ctx context.Context, task Task, secret string) error {
	timeout := d.cfg.DefaultTimeout
	if task.TimeoutMS > 0 {
		timeout = time.Duration(task.TimeoutMS) * time.Millisecond
	}
	rctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(rctx, http.MethodPost, task.URL, bytes.NewReader(task.Body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderSignature, Sign(secret, task.Body))
	req.Header.Set(HeaderDelivery, task.DeliveryID)
	req.Header.Set(HeaderRule, task.RuleID)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	// Drain and close so the connection can be reused (keep-alive).
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	return &httpStatusError{status: resp.StatusCode}
}

// scheduleRetry re-publishes a delayed copy of the task, off the consumer
// goroutine so the shard slot is freed during the backoff wait. The retry is
// tracked so graceful shutdown waits for it; context cancellation mid-wait
// publishes immediately rather than losing the retry.
func (d *Dispatcher) scheduleRetry(ctx context.Context, task Task, delay time.Duration) {
	d.retries.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			// Shutting down: publish now rather than lose the retry. Use a detached
			// context so the publish is not cancelled by the same shutdown.
			d.publish(context.WithoutCancel(ctx), task)
			return
		}
		d.publish(ctx, task)
	})
}

// publish puts a (retry) task on the delivery queue, sharded by rule so a rule's
// deliveries spread evenly; on a publish failure the delivery would be lost, so
// it is sent to the DLQ as a last resort.
func (d *Dispatcher) publish(ctx context.Context, task Task) {
	payload, err := EncodeTask(task)
	if err != nil {
		d.log.Error("encoding retry task failed; parking in DLQ", "delivery_id", task.DeliveryID, "err", err)
		d.toDLQ(ctx, task, "encode_retry: "+err.Error())
		return
	}
	msg := queue.Message{Key: task.RuleID, Payload: payload}
	if err := d.q.Publish(ctx, msg); err != nil {
		d.log.Error("re-publishing delivery failed; parking in DLQ", "delivery_id", task.DeliveryID, "err", err)
		d.toDLQ(ctx, task, "republish_failed: "+err.Error())
	}
}

// toDLQ parks an undeliverable webhook in the tenant's DLQ. The tenant is
// rebuilt from the task envelope (the delivery queue carries no context). A nil
// sink degrades to a loud log so the delivery is at least visible. A sink error
// is logged: there is nowhere else to put it.
func (d *Dispatcher) toDLQ(ctx context.Context, task Task, reason string) {
	dl := domain.DeadLetter{
		ID:         domain.NewDeadLetterID(),
		RuleID:     task.RuleID,
		Collection: task.Collection,
		DeliveryID: task.DeliveryID,
		URL:        task.URL,
		SecretRef:  task.SecretRef,
		Body:       task.Body,
		Attempts:   task.Attempt,
		LastError:  reason,
		FailedAt:   time.Now().UTC(),
	}
	if d.sink == nil {
		d.log.Error("no DLQ sink wired; dropping exhausted delivery", "delivery_id", task.DeliveryID, "rule_id", task.RuleID, "reason", reason)
		return
	}
	tctx := tenant.NewContext(ctx, taskPrincipal(task))
	if err := d.sink.Save(tctx, dl); err != nil {
		d.log.Error("saving dead letter failed; dropping", "delivery_id", task.DeliveryID, "err", err)
	}
}

func (d *Dispatcher) ack(ctx context.Context, del queue.Delivery) {
	if err := d.q.Ack(ctx, del); err != nil {
		d.log.Error("ack failed; delivery may be redelivered", "id", del.ID, "shard", del.Shard, "err", err)
	}
}

// taskPrincipal rebuilds the storage principal from the task envelope — only
// the database selectors travel, never the API key.
func taskPrincipal(task Task) tenant.Principal {
	return tenant.Principal{Tenant: tenant.Tenant{
		ID:       task.Tenant.ID,
		Database: tenant.Database{URI: task.Tenant.DBURI, Name: task.Tenant.DBName},
	}}
}

// httpStatusError marks a non-2xx response as a retryable failure carrying the
// status, so the retry log and the DLQ reason record what the receiver returned.
type httpStatusError struct{ status int }

func (e *httpStatusError) Error() string {
	return "webhook returned status " + http.StatusText(e.status)
}
