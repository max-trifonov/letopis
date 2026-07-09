package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/rules"
)

// fakeRuleRepo is a minimal in-memory RuleRepository for engine tests: it serves
// List from a per-collection slice and counts the reads so a test can assert the
// cache spares the repository on a hit. The other methods are unused by the
// engine and return zero values.
type fakeRuleRepo struct {
	mu        sync.Mutex
	byColl    map[string][]rules.Rule
	listCalls int
	listErr   error
}

func newFakeRuleRepo() *fakeRuleRepo {
	return &fakeRuleRepo{byColl: map[string][]rules.Rule{}}
}

func (r *fakeRuleRepo) List(_ context.Context, collection string) ([]rules.Rule, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.listCalls++
	if r.listErr != nil {
		return nil, r.listErr
	}
	return append([]rules.Rule(nil), r.byColl[collection]...), nil
}

func (r *fakeRuleRepo) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.listCalls
}

func (r *fakeRuleRepo) Create(context.Context, string, *rules.Rule) error { return nil }
func (r *fakeRuleRepo) Get(context.Context, string, string) (*rules.Rule, error) {
	return nil, domain.ErrNotFound
}
func (r *fakeRuleRepo) Update(context.Context, string, *rules.Rule) error { return nil }
func (r *fakeRuleRepo) Delete(context.Context, string, string) error      { return nil }
func (r *fakeRuleRepo) ListAll(context.Context) (map[string][]rules.Rule, error) {
	return nil, nil
}

// capturePublisher records every webhook delivery handed to it; failNext makes
// the next Publish return an error so a test can assert the engine swallows it.
type capturePublisher struct {
	mu       sync.Mutex
	reqs     []DeliveryRequest
	failWith error
}

func (p *capturePublisher) Publish(_ context.Context, req DeliveryRequest) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failWith != nil {
		return p.failWith
	}
	p.reqs = append(p.reqs, req)
	return nil
}

func (p *capturePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.reqs)
}

// countMetrics records rule-match increments by action.
type countMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCountMetrics() *countMetrics { return &countMetrics{counts: map[string]int{}} }

func (m *countMetrics) IncRuleMatch(_ /*tenant*/, action string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[action]++
}

func (m *countMetrics) get(action string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[action]
}

// logRule is an enabled rule that fires a log action when source equals the
// given value — the simplest matchable rule for these tests.
func logRule(name, source string) rules.Rule {
	return rules.Rule{
		ID:        "rule_" + name,
		Name:      name,
		Enabled:   true,
		Condition: rules.Condition{Field: rules.FieldSource, Op: rules.OpEq, Value: source},
		Actions:   []rules.Action{{Type: rules.ActionLog, Log: &rules.LogAction{Level: "warn"}}},
	}
}

func webhookRule(name, source, url string) rules.Rule {
	return rules.Rule{
		ID:        "rule_" + name,
		Name:      name,
		Enabled:   true,
		Condition: rules.Condition{Field: rules.FieldSource, Op: rules.OpEq, Value: source},
		Actions:   []rules.Action{{Type: rules.ActionWebhook, Webhook: &rules.Webhook{URL: url, SecretRef: "k1"}}},
	}
}

func evt(source string) domain.Event {
	return domain.Event{
		EntityID: "d-1",
		Version:  1,
		Op:       domain.OpUpdate,
		Source:   source,
		Changes:  []diff.Change{{Path: "status", Op: diff.OpChange, Old: "open", New: "closed"}},
	}
}

func TestRulesEngineMatchFiresLogAndWebhook(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{logRule("r-log", "crm"), webhookRule("r-hook", "crm", "https://x.test/hook")}
	pub := &capturePublisher{}
	met := newCountMetrics()
	e := NewRulesEngine(repo, nil, WithDeliveryPublisher(pub), WithRuleMetrics(met))

	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))

	if pub.count() != 1 {
		t.Fatalf("webhook deliveries = %d, want 1", pub.count())
	}
	if got := met.get("log"); got != 1 {
		t.Fatalf("log matches = %d, want 1", got)
	}
	if got := met.get("webhook"); got != 1 {
		t.Fatalf("webhook matches = %d, want 1", got)
	}
	req := pub.reqs[0]
	if req.RuleID != "rule_r-hook" || req.Webhook.URL != "https://x.test/hook" || req.Collection != "crm.deals" {
		t.Fatalf("delivery request wrong: %+v", req)
	}
}

func TestRulesEngineNonMatchFiresNothing(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{logRule("r-log", "crm"), webhookRule("r-hook", "crm", "https://x.test/hook")}
	pub := &capturePublisher{}
	met := newCountMetrics()
	e := NewRulesEngine(repo, nil, WithDeliveryPublisher(pub), WithRuleMetrics(met))

	e.Evaluate(authedCtx(), "crm.deals", evt("other"))

	if pub.count() != 0 || met.get("log") != 0 || met.get("webhook") != 0 {
		t.Fatalf("expected no firings; webhooks=%d log=%d hook=%d", pub.count(), met.get("log"), met.get("webhook"))
	}
}

func TestRulesEngineSkipsDisabledRules(t *testing.T) {
	repo := newFakeRuleRepo()
	disabled := logRule("r-off", "crm")
	disabled.Enabled = false
	repo.byColl["crm.deals"] = []rules.Rule{disabled}
	met := newCountMetrics()
	e := NewRulesEngine(repo, nil, WithRuleMetrics(met))

	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))

	if met.get("log") != 0 {
		t.Fatalf("disabled rule fired: log matches = %d", met.get("log"))
	}
}

func TestRulesEngineCachesCompiledRules(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{logRule("r-log", "crm")}
	e := NewRulesEngine(repo, nil)

	for range 5 {
		e.Evaluate(authedCtx(), "crm.deals", evt("crm"))
	}
	if repo.calls() != 1 {
		t.Fatalf("repo List called %d times, want 1 (cache should serve hits)", repo.calls())
	}
}

func TestRulesEngineInvalidateDropsCache(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{logRule("r-log", "crm")}
	e := NewRulesEngine(repo, nil)

	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))
	e.Invalidate(tenantDBFor("acme"), "crm.deals")
	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))

	if repo.calls() != 2 {
		t.Fatalf("repo List called %d times, want 2 (invalidate forces a re-read)", repo.calls())
	}
}

func TestRulesEngineTTLReReads(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{logRule("r-log", "crm")}
	clock := &fakeClock{t: time.Unix(0, 0)}
	e := NewRulesEngine(repo, nil, WithRuleCacheTTL(time.Second))
	e.now = clock.now

	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))
	clock.advance(2 * time.Second) // past the TTL
	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))

	if repo.calls() != 2 {
		t.Fatalf("repo List called %d times, want 2 (TTL should force a re-read)", repo.calls())
	}
}

func TestRulesEngineRepoErrorDoesNotPanic(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.listErr = errors.New("mongo down")
	pub := &capturePublisher{}
	e := NewRulesEngine(repo, nil, WithDeliveryPublisher(pub))

	// Must not panic and must fire nothing: the event is already durable.
	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))
	if pub.count() != 0 {
		t.Fatalf("expected no deliveries on repo error, got %d", pub.count())
	}
}

func TestRulesEnginePublishErrorSwallowed(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{webhookRule("r-hook", "crm", "https://x.test/hook")}
	pub := &capturePublisher{failWith: errors.New("queue down")}
	met := newCountMetrics()
	e := NewRulesEngine(repo, nil, WithDeliveryPublisher(pub), WithRuleMetrics(met))

	// A publish failure is logged and swallowed; the match is still counted.
	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))
	if met.get("webhook") != 1 {
		t.Fatalf("webhook match count = %d, want 1 even when publish fails", met.get("webhook"))
	}
}

func TestRulesEngineWebhookWithoutPublisherDropped(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{webhookRule("r-hook", "crm", "https://x.test/hook")}
	e := NewRulesEngine(repo, nil) // no publisher wired

	// Must not panic; the webhook is dropped with a warning, the match still counts
	// would require metrics — here we only assert no panic and the cache works.
	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))
}

func TestRulesEngineNilIsNoOp(t *testing.T) {
	var e *RulesEngine
	// A nil engine and a nil-receiver Invalidate must be safe.
	e.Evaluate(authedCtx(), "crm.deals", evt("crm"))
	e.Invalidate("hm_t_acme", "crm.deals")
}

func TestRulesEngineNoTenantIsNoOp(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{logRule("r-log", "crm")}
	e := NewRulesEngine(repo, nil)

	e.Evaluate(context.Background(), "crm.deals", evt("crm")) // no principal
	if repo.calls() != 0 {
		t.Fatalf("repo consulted without a tenant: %d calls", repo.calls())
	}
}

// tenantDBFor mirrors tenant.Tenant.DatabaseName for the default (no override)
// case so a test can build the same cache key the engine uses.
func tenantDBFor(id string) string { return "hm_t_" + id }

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}
