package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
	"github.com/max-trifonov/letopis/internal/rules"
)

// chainPlugin is a stand-in hash-chain: it links each event off the prev event's
// integrity, so a test can assert the carry-forward EntityView on both write paths.
type chainPlugin struct {
	name  string
	mu    sync.Mutex
	calls int
}

func (p *chainPlugin) Name() string { return p.name }
func (p *chainPlugin) PreStore(_ context.Context, d *plugin.EventDraft, prev plugin.EntityView) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	prevHash := "genesis:" + d.Collection()
	if prev.LastEvent != nil && prev.LastEvent.Integrity != nil {
		prevHash = prev.LastEvent.Integrity.Hash
	}
	p.calls++
	d.SetIntegrity(fmt.Sprintf("%s|%s#%d", prevHash, d.EntityID(), p.calls), prevHash)
	return nil
}

// failPlugin always errors; used for fail-open/closed policy tests.
type failPlugin struct{ name string }

func (p *failPlugin) Name() string { return p.name }
func (p *failPlugin) PreStore(context.Context, *plugin.EventDraft, plugin.EntityView) error {
	return errors.New("plugin boom")
}

type countPost struct {
	name string
	mu   sync.Mutex
	evs  []domain.Event
}

func (p *countPost) Name() string { return p.name }
func (p *countPost) PostStore(_ context.Context, ev domain.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.evs = append(p.evs, ev)
}
func (p *countPost) count() int { p.mu.Lock(); defer p.mu.Unlock(); return len(p.evs) }

type pluginErrMetrics struct {
	mu sync.Mutex
	n  map[string]int
}

func newPluginErrMetrics() *pluginErrMetrics { return &pluginErrMetrics{n: map[string]int{}} }
func (m *pluginErrMetrics) IncPluginError(p, hook string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n[p+"/"+hook]++
}

// pluginFixture wires an Ingester over in-memory repos with a configured plugin
// host and a collection whose plugin config matches.
func pluginFixture(t *testing.T, host *plugin.Host, plugins map[string]domain.PluginConfig) (*Ingester, *fakeEvents) {
	t.Helper()
	repo := newFakeRepo()
	repo.configs["crm.deals"] = domain.CollectionConfig{Name: "crm.deals", Plugins: plugins}
	events := newFakeEvents()
	resolver := NewConfigResolver(repo, Options{AutoCreate: true})
	ing := NewIngester(resolver, events, newFakeCurrent(), WithPlugins(host))
	return ing, events
}

func enabledClosed(name string) map[string]domain.PluginConfig {
	return map[string]domain.PluginConfig{name: {Enabled: true, FailMode: domain.FailClosed}}
}

func TestPreStoreRunsOnStrictPath(t *testing.T) {
	hc := &chainPlugin{name: "hc"}
	host := plugin.NewHost([]plugin.PreStorePlugin{hc}, nil, nil, nil)
	ing, events := pluginFixture(t, host, enabledClosed("hc"))
	ctx := authedCtx()

	if _, err := ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if _, err := ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(250)}); err != nil {
		t.Fatalf("v2: %v", err)
	}
	evs := events.byEntity["crm.deals|d-1"]
	if len(evs) != 2 {
		t.Fatalf("want 2 events, got %d", len(evs))
	}
	if evs[0].Integrity == nil || evs[1].Integrity == nil {
		t.Fatalf("integrity not set: %+v / %+v", evs[0].Integrity, evs[1].Integrity)
	}
	// The second link chains off the first: carry-forward across two writes via
	// the stored LastEvent.
	if evs[1].Integrity.PrevHash != evs[0].Integrity.Hash {
		t.Fatalf("chain broken: prev=%q want %q", evs[1].Integrity.PrevHash, evs[0].Integrity.Hash)
	}
}

func TestPreStoreFailClosedAbortsStrict(t *testing.T) {
	host := plugin.NewHost([]plugin.PreStorePlugin{&failPlugin{name: "hc"}}, nil, nil, nil)
	ing, events := pluginFixture(t, host, enabledClosed("hc"))

	_, err := ing.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)})
	var fc *plugin.FailClosedError
	if !errors.As(err, &fc) {
		t.Fatalf("err = %v, want *FailClosedError", err)
	}
	if got := len(events.byEntity["crm.deals|d-1"]); got != 0 {
		t.Fatalf("fail-closed must not commit; wrote %d events", got)
	}
}

func TestPreStoreFailOpenCommitsAndCounts(t *testing.T) {
	met := newPluginErrMetrics()
	host := plugin.NewHost([]plugin.PreStorePlugin{&failPlugin{name: "hc"}}, nil, nil, met)
	plugins := map[string]domain.PluginConfig{"hc": {Enabled: true, FailMode: domain.FailOpen}}
	ing, events := pluginFixture(t, host, plugins)

	if _, err := ing.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}); err != nil {
		t.Fatalf("fail-open must commit: %v", err)
	}
	evs := events.byEntity["crm.deals|d-1"]
	if len(evs) != 1 || evs[0].Integrity != nil {
		t.Fatalf("fail-open should write the event without a plugin contribution: %+v", evs)
	}
	if met.n["hc/pre_store"] != 1 {
		t.Fatalf("fail-open did not count plugin_errors_total: %+v", met.n)
	}
}

func TestPostStoreRunsBestEffort(t *testing.T) {
	post := &countPost{name: "idx"}
	host := plugin.NewHost(nil, []plugin.PostStorePlugin{post}, nil, nil)
	plugins := map[string]domain.PluginConfig{"idx": {Enabled: true}}
	ing, _ := pluginFixture(t, host, plugins)

	if _, err := ing.State(authedCtx(), IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}); err != nil {
		t.Fatalf("State: %v", err)
	}
	if post.count() != 1 {
		t.Fatalf("post-store ran %d times, want 1", post.count())
	}
}

func TestPreStoreCarryForwardInBatch(t *testing.T) {
	hc := &chainPlugin{name: "hc"}
	host := plugin.NewHost([]plugin.PreStorePlugin{hc}, nil, nil, nil)
	ing, events := pluginFixture(t, host, enabledClosed("hc"))
	ctx := authedCtx()

	// Two events of the same entity in one fast batch: the second must chain off
	// the first via the in-memory carry-forward, not the (empty) stored tail.
	errs := ing.IngestBatch(ctx, []BatchItem{
		{Kind: KindState, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}},
		{Kind: KindState, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(250)}},
	})
	for i, e := range errs {
		if e != nil {
			t.Fatalf("item %d: %v", i, e)
		}
	}
	evs := events.byEntity["crm.deals|d-1"]
	if len(evs) != 2 {
		t.Fatalf("want 2 events, got %d", len(evs))
	}
	if evs[1].Integrity.PrevHash != evs[0].Integrity.Hash {
		t.Fatalf("batch chain broken: prev=%q want %q", evs[1].Integrity.PrevHash, evs[0].Integrity.Hash)
	}
}

func TestPreStoreFailClosedInBatchIsolatesItem(t *testing.T) {
	host := plugin.NewHost([]plugin.PreStorePlugin{&failPlugin{name: "hc"}}, nil, nil, nil)
	ing, events := pluginFixture(t, host, enabledClosed("hc"))

	errs := ing.IngestBatch(authedCtx(), []BatchItem{
		{Kind: KindState, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}},
	})
	var fc *plugin.FailClosedError
	if !errors.As(errs[0], &fc) {
		t.Fatalf("errs[0] = %v, want *FailClosedError", errs[0])
	}
	if got := len(events.byEntity["crm.deals|d-1"]); got != 0 {
		t.Fatalf("fail-closed must not commit in batch; wrote %d", got)
	}
}

// dispatchAction records the events handed to it through the rules engine.
type dispatchAction struct {
	typ    string
	mu     sync.Mutex
	calls  int
	params json.RawMessage
}

func (a *dispatchAction) Type() string { return a.typ }
func (a *dispatchAction) Execute(_ context.Context, _ domain.Event, params json.RawMessage) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	a.params = params
	return nil
}
func (a *dispatchAction) count() int { a.mu.Lock(); defer a.mu.Unlock(); return a.calls }

func TestRulesEngineDispatchesPluginAction(t *testing.T) {
	act := &dispatchAction{typ: "notify"}
	host := plugin.NewHost(nil, nil, []plugin.ActionPlugin{act}, nil)
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{{
		ID: "r1", Name: "notify-all", Enabled: true,
		Condition: rules.Condition{All: []rules.Condition{}}, // empty All ⇒ always true
		Actions:   []rules.Action{{Type: "notify", Params: json.RawMessage(`{"to":"ops"}`)}},
	}}
	engine := NewRulesEngine(repo, nil, WithActionDispatcher(host))

	engine.Evaluate(authedCtx(), "crm.deals", domain.Event{EntityID: "d-1", Op: domain.OpCreate, Version: 1})
	if act.count() != 1 {
		t.Fatalf("plugin action dispatched %d times, want 1", act.count())
	}
	if string(act.params) != `{"to":"ops"}` {
		t.Fatalf("params not forwarded: %s", act.params)
	}
}

func TestRulesEngineUnknownActionWithoutHost(t *testing.T) {
	repo := newFakeRuleRepo()
	repo.byColl["crm.deals"] = []rules.Rule{{
		ID: "r1", Enabled: true,
		Condition: rules.Condition{All: []rules.Condition{}},
		Actions:   []rules.Action{{Type: "notify"}},
	}}
	// No dispatcher wired: the unknown action is skipped, not a panic.
	engine := NewRulesEngine(repo, nil)
	engine.Evaluate(authedCtx(), "crm.deals", domain.Event{EntityID: "d-1", Op: domain.OpCreate, Version: 1})
}
