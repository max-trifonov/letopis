package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"
)

// stubQueue records published messages so a producer test can assert routing
// without a real backend. The consumer methods are unused here.
type stubQueue struct {
	mu  sync.Mutex
	got []queue.Message
}

func (s *stubQueue) Publish(_ context.Context, m queue.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, m)
	return nil
}

func (s *stubQueue) Subscribe(context.Context, int) (<-chan queue.Delivery, error) { return nil, nil }
func (s *stubQueue) Ack(context.Context, queue.Delivery) error                     { return nil }
func (s *stubQueue) Reclaim(context.Context, int, time.Duration) ([]queue.Delivery, error) {
	return nil, nil
}
func (s *stubQueue) Shards() int  { return 1 }
func (s *stubQueue) Close() error { return nil }

func (s *stubQueue) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func deals(mode domain.ReliabilityMode) *domain.CollectionConfig {
	return &domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: mode}
}

func TestEffectiveMode(t *testing.T) {
	cases := []struct {
		override domain.ReliabilityMode
		cfg      *domain.CollectionConfig
		want     domain.ReliabilityMode
	}{
		{"", deals(domain.ReliabilityStrict), domain.ReliabilityStrict},                     // collection default
		{"", deals(domain.ReliabilityFast), domain.ReliabilityFast},                         // collection default
		{domain.ReliabilityStrict, deals(domain.ReliabilityFast), domain.ReliabilityStrict}, // override wins
		{domain.ReliabilityFast, deals(domain.ReliabilityStrict), domain.ReliabilityFast},   // override wins
		{"", &domain.CollectionConfig{}, domain.ReliabilityDurable},                         // nothing set → durable
		{"", nil, domain.ReliabilityDurable},                                                // no config → durable
	}
	for _, c := range cases {
		if got := effectiveMode(c.override, c.cfg); got != c.want {
			t.Errorf("effectiveMode(%q, %+v) = %q, want %q", c.override, c.cfg, got, c.want)
		}
	}
}

func TestPipelineRoutesByMode(t *testing.T) {
	durable, fast := &stubQueue{}, &stubQueue{}
	p := NewPipeline(durable, fast, nil)
	task := Task{Tenant: TenantRef{ID: "acme"}, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1"}}

	used, err := p.Publish(context.Background(), domain.ReliabilityDurable, task)
	if err != nil || used != domain.ReliabilityDurable {
		t.Fatalf("durable publish: used=%s err=%v", used, err)
	}
	if durable.count() != 1 || fast.count() != 0 {
		t.Fatalf("durable not routed to durable queue: d=%d f=%d", durable.count(), fast.count())
	}

	used, err = p.Publish(context.Background(), domain.ReliabilityFast, task)
	if err != nil || used != domain.ReliabilityFast {
		t.Fatalf("fast publish: used=%s err=%v", used, err)
	}
	if fast.count() != 1 {
		t.Fatalf("fast not routed to fast queue: %d", fast.count())
	}
}

// With no in-process fast queue (split api role), fast degrades to durable
// rather than publishing into a void.
func TestPipelineFastDegradesToDurable(t *testing.T) {
	durable := &stubQueue{}
	p := NewPipeline(durable, nil, nil)
	task := Task{Tenant: TenantRef{ID: "acme"}, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1"}}

	used, err := p.Publish(context.Background(), domain.ReliabilityFast, task)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if used != domain.ReliabilityDurable || durable.count() != 1 {
		t.Fatalf("fast did not degrade to durable: used=%s count=%d", used, durable.count())
	}
}

func TestPipelineShardKey(t *testing.T) {
	durable := &stubQueue{}
	p := NewPipeline(durable, nil, nil)
	task := Task{Tenant: TenantRef{ID: "acme"}, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", EventID: "evt-9"}}
	if _, err := p.Publish(context.Background(), domain.ReliabilityDurable, task); err != nil {
		t.Fatal(err)
	}
	m := durable.got[0]
	if m.Key != "acme|crm.deals|d-1" {
		t.Errorf("shard key = %q", m.Key)
	}
	if m.Attrs["event_id"] != "evt-9" {
		t.Errorf("event_id attr = %q", m.Attrs["event_id"])
	}
}

// asyncFixture wires an AsyncIngester over the in-memory repos plus stub queues
// and a fake ticket store.
type asyncFixture struct {
	async   *AsyncIngester
	durable *stubQueue
	fast    *stubQueue
	tickets *fakeTickets
	events  *fakeEvents
}

func newAsyncFixture(t *testing.T) asyncFixture {
	t.Helper()
	repo := newFakeRepo()
	events := newFakeEvents()
	resolver := NewConfigResolver(repo, Options{AutoCreate: true})
	ing := NewIngester(resolver, events, newFakeCurrent())
	durable, fast := &stubQueue{}, &stubQueue{}
	tickets := newFakeTickets()
	return asyncFixture{
		async:   NewAsyncIngester(ing, NewPipeline(durable, fast, nil), NewTicketService(tickets), AsyncOptions{}),
		durable: durable, fast: fast, tickets: tickets, events: events,
	}
}

// A strict collection runs synchronously: a real version, no ticket, no enqueue.
func TestAsyncIngesterStrictIsSynchronous(t *testing.T) {
	fx := newAsyncFixture(t)
	fx.async.sync.cfg.repo.(*fakeRepo).configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals", ReliabilityMode: domain.ReliabilityStrict}

	res, err := fx.async.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if res.Async || res.Ticket != "" || res.Version != 1 {
		t.Fatalf("strict should be synchronous: %+v", res)
	}
	if fx.durable.count() != 0 || fx.fast.count() != 0 {
		t.Fatalf("strict enqueued: d=%d f=%d", fx.durable.count(), fx.fast.count())
	}
}

// A durable collection (the default) returns 202 semantics: async, a ticket,
// one durable enqueue, no event written synchronously.
func TestAsyncIngesterDurableEnqueues(t *testing.T) {
	fx := newAsyncFixture(t)
	res, err := fx.async.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	if err != nil {
		t.Fatalf("State: %v", err)
	}
	if !res.Async || res.Ticket == "" {
		t.Fatalf("durable should be async with a ticket: %+v", res)
	}
	if fx.durable.count() != 1 {
		t.Fatalf("durable enqueue count = %d, want 1", fx.durable.count())
	}
	if got := len(fx.events.byEntity["crm.deals|d-1"]); got != 0 {
		t.Fatalf("durable wrote synchronously: %d events", got)
	}
	tk, err := fx.tickets.Get(authedCtx(), res.Ticket)
	if err != nil || tk.Status != domain.TicketAccepted {
		t.Fatalf("ticket not opened accepted: %+v err=%v", tk, err)
	}
}

// A per-request fast override beats a durable collection default and routes to
// the fast queue.
func TestAsyncIngesterPerRequestOverride(t *testing.T) {
	fx := newAsyncFixture(t)
	res, err := fx.async.Diff(authedCtx(), IngestCommand{
		Collection: "crm.deals", EntityID: "d-1", Mode: domain.ReliabilityFast, Op: domain.OpCreate, State: deal(100),
	})
	if err != nil {
		t.Fatalf("Diff: %v", err)
	}
	if !res.Async || fx.fast.count() != 1 || fx.durable.count() != 0 {
		t.Fatalf("override did not route to fast: %+v d=%d f=%d", res, fx.durable.count(), fx.fast.count())
	}
}
