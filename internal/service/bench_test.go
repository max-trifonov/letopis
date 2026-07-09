package service

import (
	"testing"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

// These benchmarks isolate the CPU cost of the async accept and worker-apply
// paths from storage I/O, the split S2-07 leans on to reason about NFR-1.1
// (202 latency) and NFR-1.3 (throughput): the queue payload codec and the
// per-event planning (diff apply) are the work that runs under the request and
// the worker goroutine, while Mongo/Redis round-trips are measured end-to-end
// by the k6 scenarios in test/load. A ~2KB diff (architecture §2, NFR-1.3)
// keeps the payload representative of a real CRM record edit.

// benchTask builds a representative durable-mode task: a ready ~2KB diff for an
// existing entity, the shape the producer serializes on every async accept.
func benchTask() Task {
	return Task{
		Tenant:   TenantRef{ID: "acme"},
		Kind:     KindDiff,
		Mode:     domain.ReliabilityDurable,
		TicketID: domain.NewTicketID(),
		Command: IngestCommand{
			Collection: "crm.deals",
			EntityID:   "deal-4821",
			Op:         domain.OpUpdate,
			Author:     "svc-sync",
			Source:     "crm",
			EventID:    domain.NewTicketID(),
			Changes:    benchChanges(),
		},
	}
}

// benchChanges is a ~2KB diff: a dozen field edits with realistic string and
// nested values, the size NFR-1.3 fixes for its throughput target.
func benchChanges() []diff.Change {
	const note = "reconciled against upstream CRM export; tier promotion approved by region lead"
	changes := []diff.Change{
		{Path: "status", Op: diff.OpChange, Old: "processing", New: "shipped"},
		{Path: "customer.tier", Op: diff.OpChange, Old: "gold", New: "platinum"},
		{Path: "customer.account_manager", Op: diff.OpChange, Old: "amgr-12", New: "amgr-30"},
		{Path: "total", Op: diff.OpChange, Old: 1299.5, New: 1450.0},
		{Path: "currency", Op: diff.OpChange, Old: "USD", New: "EUR"},
		{Path: "shipping.carrier", Op: diff.OpChange, Old: "dhl", New: "fedex"},
		{Path: "shipping.tracking", Op: diff.OpAdd, New: "FX-99-2841-0033-7712"},
		{Path: "shipping.address.city", Op: diff.OpChange, Old: "Berlin", New: "Munich"},
		{Path: "shipping.address.postcode", Op: diff.OpChange, Old: "10115", New: "80331"},
		{Path: "items.1.price", Op: diff.OpChange, Old: 901.5, New: 950.0},
		{Path: "items.2.qty", Op: diff.OpChange, Old: 4.0, New: 6.0},
		{Path: "tags.2", Op: diff.OpAdd, New: "expedited"},
		{Path: "notes", Op: diff.OpChange, Old: "", New: note},
		{Path: "review.note", Op: diff.OpChange, Old: "", New: note},
		{Path: "review.reviewer", Op: diff.OpChange, Old: "ops-1", New: "ops-7"},
		{Path: "review.checklist", Op: diff.OpAdd, New: []any{"credit", "stock", "fraud", "export-licence"}},
		{Path: "billing.method", Op: diff.OpChange, Old: "invoice", New: "card"},
		{Path: "billing.terms", Op: diff.OpChange, Old: "net-30", New: "net-15"},
		{Path: "fulfilment.warehouse", Op: diff.OpChange, Old: "wh-eu-1", New: "wh-eu-3"},
		{Path: "fulfilment.priority", Op: diff.OpChange, Old: "standard", New: "high"},
	}
	return changes
}

func benchPlanCommand() IngestCommand {
	return IngestCommand{
		Collection: "crm.deals",
		EntityID:   "deal-4821",
		Op:         domain.OpUpdate,
		Changes:    benchChanges(),
	}
}

// baseDoc is the current state a ready diff is applied against in planEvent.
func benchBaseState() *domain.CurrentState {
	return &domain.CurrentState{
		EntityID: "deal-4821",
		Version:  7,
		State: map[string]any{
			"status":   "processing",
			"total":    1299.5,
			"currency": "USD",
			"customer": map[string]any{"id": "cust-77", "tier": "gold", "account_manager": "amgr-12"},
			"shipping": map[string]any{
				"carrier": "dhl",
				"address": map[string]any{"city": "Berlin", "postcode": "10115"},
			},
			"items": []any{
				map[string]any{"sku": "A-1", "qty": 2.0, "price": 199.0},
				map[string]any{"sku": "B-7", "qty": 1.0, "price": 901.5},
				map[string]any{"sku": "C-3", "qty": 4.0, "price": 49.75},
			},
			"tags":       []any{"priority", "export"},
			"notes":      "",
			"review":     map[string]any{"note": "", "reviewer": "ops-1"},
			"billing":    map[string]any{"method": "invoice", "terms": "net-30"},
			"fulfilment": map[string]any{"warehouse": "wh-eu-1", "priority": "standard"},
		},
	}
}

// BenchmarkEncodeTask measures the queue-payload serialization that runs under
// every async accept (NFR-1.1). The reported bytes/op double as the on-wire
// payload size the k6 ~2KB target is calibrated against.
func BenchmarkEncodeTask(b *testing.B) {
	task := benchTask()
	if p, err := Encode(task); err != nil {
		b.Fatalf("Encode: %v", err)
	} else {
		b.Logf("encoded task payload: %d bytes", len(p))
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Encode(task); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeTask measures the worker-side deserialization (NFR-1.6: part
// of the 202→stored budget).
func BenchmarkDecodeTask(b *testing.B) {
	payload, err := Encode(benchTask())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Decode(payload); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPlanEvent measures the pure per-event planning the worker runs for a
// ready diff (apply against current state, build the event), the CPU side of
// the async write that bounds single-shard throughput (NFR-1.3). It touches no
// storage, so it isolates the domain cost from Mongo.
func BenchmarkPlanEvent(b *testing.B) {
	repo := newFakeRepo()
	ing := NewIngester(NewConfigResolver(repo, Options{AutoCreate: true}), newFakeEvents(), newFakeCurrent())
	cfg := domain.WithDefaults(domain.CollectionConfig{Name: "crm.deals"})
	cmd := benchPlanCommand()
	cur := benchBaseState()
	b.ReportAllocs()
	for b.Loop() {
		if _, _, _, _, err := ing.planEvent(&cfg, KindDiff, cmd, cur); err != nil {
			b.Fatal(err)
		}
	}
}
