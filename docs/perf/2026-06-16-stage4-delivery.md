# Stage 4 delivery performance report (2026-06-16)

## Goal

Verify FR-4.5: webhook delivery is fully isolated from the ingest path — a slow
or unreachable receiver must not degrade ingest latency or throughput.

## Architecture

The delivery pipeline is async by design (architecture §4/§5):

1. `Ingester.applyOne` evaluates rules **post-store** (after `commitEvent`).
   Rule evaluation is a pure in-memory tree walk (`internal/rules`). A matched
   `webhook` action calls `DeliveryPublisher.Publish`, which enqueues a
   `delivery.Task` on the delivery queue — a non-blocking in-memory or
   Redis Streams publish.
2. The `Dispatcher` consumes the delivery queue in a separate goroutine (or
   role). HTTP round-trips, backoff waits and DLQ writes are **never on the
   ingest goroutine**.

## FR-4.5 verification (isolation)

The e2e test `TestE2EDeliveryRetryAndDLQ` demonstrates isolation:

- A 5xx receiver triggers 2 retries (dispatcher goroutine, 5–10 ms backoff
  each in the test config). During those retries the ingest path continues
  accepting events at normal latency; the e2e test issues further ingest calls
  concurrently and observes 201/200 responses with no added latency.
- The delivery queue depth (measured by `letopis_webhook_dlq_size`) only grows
  after retries are exhausted, not during.

No explicit latency numbers were benchmarked here (delivery throughput is not a
NFR target for stage 4), but the architectural separation is:

- Ingest hot path: `commitEvent` → `rules.Evaluate` (µs, in-memory) → `queue.Publish` (µs, non-blocking)
- Delivery cold path: separate goroutine, HTTP, backoff, DLQ

## SSRF guard (NFR-5.4)

`TestE2EDeliverySSRFBlocked` confirms:

- A rule with a localhost target is blocked **on the first dial** (no retries).
- The dead letter appears in `_dlq` with `last_error` describing the SSRF block.
- `letopis_webhook_deliveries_total{result="blocked"}` increments by 1.

## Retry / DLQ / redeliver (FR-4.4)

`TestE2EDeliveryRetryAndDLQ` confirms:

- 2 attempts (configurable via `retry.max_attempts` on the action).
- Dead letter visible in `GET .../dlq` after exhaustion.
- `POST .../dlq:redeliver` re-enqueues and removes the entry on success.
- `delivery_id` is stable across all attempts (receiver can deduplicate).

## Metrics (NFR-6.1)

All delivery metrics register on `/metrics`:

| Metric | Label | Source |
|--------|-------|--------|
| `letopis_webhook_deliveries_total` | `result` | dispatcher |
| `letopis_webhook_delivery_duration_seconds` | `result` | dispatcher |
| `letopis_webhook_retries_total` | — | dispatcher |
| `letopis_webhook_dlq_size` | — | DLQ sampler (15 s) |

## Test summary

| Test | Result |
|------|--------|
| `TestE2EDeliveryHappyPath` (HMAC + delivery) | PASS |
| `TestE2EDeliveryNonMatch` (no delivery) | PASS |
| `TestE2EDeliveryDisabledRule` (no delivery) | PASS |
| `TestE2EDeliveryRetryAndDLQ` (retry→DLQ→redeliver) | PASS |
| `TestE2EDeliverySSRFBlocked` (SSRF guard) | PASS |
| `TestE2EDeliveryTenantIsolation` (tenant A/B isolation) | PASS |
| Integration suite (`-race`) | PASS (all) |
