// Package metrics holds the Prometheus instrumentation: queue depth, consumer
// lag, publish errors, accepted-event and backpressure counters. An
// observability adapter — instrumented at boundaries (transport, queue, worker)
// via small interfaces those packages define, so the domain never knows about it.
// Collectors register against an injected Registerer so tests can scrape an
// isolated registry without global state.
package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "letopis"

// Metrics is the set of collectors. The per-event counter carries tenant but
// never collection or entity (cardinality); queue gauges/counters carry a
// coarse queue label only.
type Metrics struct {
	queueDepth       *prometheus.GaugeVec
	consumerLag      *prometheus.GaugeVec
	publishErrors    *prometheus.CounterVec
	backpressure     *prometheus.CounterVec
	events           *prometheus.CounterVec
	ruleMatches      *prometheus.CounterVec
	pluginErrors     *prometheus.CounterVec
	deliveries       *prometheus.CounterVec
	deliveryDuration *prometheus.HistogramVec
	deliveryRetries  prometheus.Counter
	dlqSize          prometheus.Gauge
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		queueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "queue_depth",
			Help: "Messages accepted but not yet processed, per queue.",
		}, []string{"queue"}),
		consumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "consumer_lag_seconds",
			Help: "Seconds from async accept (202) to write of the last processed message, per queue.",
		}, []string{"queue"}),
		publishErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "queue_publish_errors_total",
			Help: "Failed enqueue attempts, per queue.",
		}, []string{"queue"}),
		backpressure: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "ingest_backpressure_total",
			Help: "Async accepts refused with 429 because a queue was at capacity, per queue.",
		}, []string{"queue"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "ingest_events_total",
			Help: "Accepted async ingest events by tenant and reliability mode (no collection/entity labels — cardinality).",
		}, []string{"tenant", "mode"}),
		ruleMatches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "rule_matches_total",
			Help: "Rule firings by tenant and action kind (no collection/entity labels — cardinality).",
		}, []string{"tenant", "action"}),
		pluginErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "plugin_errors_total",
			Help: "Fail-open plugin hook errors by plugin and hook (pre_store/post_store).",
		}, []string{"plugin", "hook"}),
		deliveries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "webhook_deliveries_total",
			Help: "Webhook delivery attempts by result: delivered, failed, blocked, or dropped.",
		}, []string{"result"}),
		deliveryDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "webhook_delivery_duration_seconds",
			Help:    "Latency of individual webhook delivery attempts.",
			Buckets: prometheus.DefBuckets,
		}, []string{"result"}),
		deliveryRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "webhook_retries_total",
			Help: "Number of webhook delivery retries scheduled.",
		}),
		dlqSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace, Name: "webhook_dlq_size",
			Help: "Total dead-lettered webhook deliveries across the instance.",
		}),
	}
	reg.MustRegister(
		m.queueDepth, m.consumerLag, m.publishErrors, m.backpressure, m.events, m.ruleMatches,
		m.pluginErrors, m.deliveries, m.deliveryDuration, m.deliveryRetries, m.dlqSize,
	)
	return m
}

func (m *Metrics) SetQueueDepth(queue string, depth float64) {
	m.queueDepth.WithLabelValues(queue).Set(depth)
}

func (m *Metrics) SetConsumerLag(queue string, seconds float64) {
	m.consumerLag.WithLabelValues(queue).Set(seconds)
}

func (m *Metrics) IncPublishError(queue string) {
	m.publishErrors.WithLabelValues(queue).Inc()
}

func (m *Metrics) IncBackpressure(queue string) {
	m.backpressure.WithLabelValues(queue).Inc()
}

func (m *Metrics) ObserveAccept(tenant, mode string) {
	m.events.WithLabelValues(tenant, mode).Inc()
}

func (m *Metrics) IncRuleMatch(tenant, action string) {
	m.ruleMatches.WithLabelValues(tenant, action).Inc()
}

func (m *Metrics) IncPluginError(plugin, hook string) {
	m.pluginErrors.WithLabelValues(plugin, hook).Inc()
}

func (m *Metrics) ObserveDelivery(result string, duration time.Duration) {
	m.deliveries.WithLabelValues(result).Inc()
	m.deliveryDuration.WithLabelValues(result).Observe(duration.Seconds())
}

func (m *Metrics) IncDeliveryRetry() {
	m.deliveryRetries.Inc()
}

func (m *Metrics) SetDLQSize(size float64) {
	m.dlqSize.Set(size)
}
