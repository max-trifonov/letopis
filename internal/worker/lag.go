package worker

import (
	"strconv"
	"time"

	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/service"
)

// recordLag reports the wall-clock lag from enqueue to write commit. Missing
// stamp (test message or old format) is skipped.
func (w *Worker) recordLag(d queue.Delivery) {
	if w.metrics == nil {
		return
	}
	secs, ok := lagSeconds(d.Attrs[service.AttrEnqueuedAt], time.Now())
	if !ok {
		return
	}
	w.metrics.SetConsumerLag(w.opts.QueueName, secs)
}

// lagSeconds converts an enqueue stamp (unix nanos string) to a non-negative
// lag in seconds. Negative lags (clock skew) are clamped to 0.
func lagSeconds(enqStamp string, now time.Time) (float64, bool) {
	if enqStamp == "" {
		return 0, false
	}
	nanos, err := strconv.ParseInt(enqStamp, 10, 64)
	if err != nil {
		return 0, false
	}
	d := max(now.Sub(time.Unix(0, nanos)), 0)
	return d.Seconds(), true
}
