package metrics

import (
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// The per-event counter must carry tenant but never collection or entity, the
// cardinality rule from NFR-6.1.
func TestEventsCounterLabelCardinality(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.ObserveAccept("acme", "durable")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var labels []string
	for _, mf := range mfs {
		if mf.GetName() != "letopis_ingest_events_total" {
			continue
		}
		for _, l := range mf.GetMetric()[0].GetLabel() {
			labels = append(labels, l.GetName())
		}
	}
	slices.Sort(labels)
	if want := []string{"mode", "tenant"}; !slices.Equal(labels, want) {
		t.Fatalf("event counter labels = %v, want %v", labels, want)
	}
	for _, banned := range []string{"collection", "entity"} {
		if slices.Contains(labels, banned) {
			t.Fatalf("forbidden high-cardinality label %q present", banned)
		}
	}
}

// All collectors register cleanly and expose the documented metric names.
func TestMetricsRegisterAndExpose(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := New(reg)
	m.SetQueueDepth("durable", 3)
	m.SetConsumerLag("durable", 0.5)
	m.IncPublishError("durable")
	m.IncBackpressure("durable")
	m.ObserveAccept("acme", "fast")

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	got := map[string]bool{}
	for _, mf := range mfs {
		got[mf.GetName()] = true
	}
	for _, name := range []string{
		"letopis_queue_depth",
		"letopis_consumer_lag_seconds",
		"letopis_queue_publish_errors_total",
		"letopis_ingest_backpressure_total",
		"letopis_ingest_events_total",
	} {
		if !got[name] {
			t.Errorf("metric %q not exposed", name)
		}
	}
}
