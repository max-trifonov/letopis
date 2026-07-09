package service

import (
	"fmt"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// QueueLimiter gates async accepts on queue depth. The service asks before
// enqueuing; the concrete limiter compares a periodically sampled depth against
// the configured threshold, so the decision is stable under a burst rather than
// flapping per request. A nil limiter disables backpressure entirely.
type QueueLimiter interface {
	// Allow reports whether a write in the given mode may be enqueued. When it
	// may not, retryAfter is how long the client should wait before retrying,
	// surfaced as the Retry-After header on the 429.
	Allow(mode domain.ReliabilityMode) (allowed bool, retryAfter time.Duration)
}

// BackpressureError signals that an async accept was refused because the queue
// is at capacity. Transport maps it to 429 and echoes RetryAfter as the
// Retry-After header. It is a typed error (not a sentinel) so it can carry the
// per-decision retry hint.
type BackpressureError struct {
	RetryAfter time.Duration
}

func (e *BackpressureError) Error() string {
	return fmt.Sprintf("service: queue at capacity, retry after %s", e.RetryAfter)
}
