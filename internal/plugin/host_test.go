package plugin

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

// stubPre is a configurable pre-store plugin: it records its calls, optionally
// fails, and optionally mutates the draft so a test can assert the setters land.
type stubPre struct {
	name     string
	calls    int
	failWith error
	mutate   func(d *EventDraft, prev EntityView)
}

func (s *stubPre) Name() string { return s.name }
func (s *stubPre) PreStore(_ context.Context, d *EventDraft, prev EntityView) error {
	s.calls++
	if s.mutate != nil {
		s.mutate(d, prev)
	}
	return s.failWith
}

type stubPost struct {
	name  string
	calls int
}

func (s *stubPost) Name() string                            { return s.name }
func (s *stubPost) PostStore(context.Context, domain.Event) { s.calls++ }

type stubAction struct {
	typ    string
	calls  int
	gotEv  domain.Event
	gotRaw json.RawMessage
}

func (s *stubAction) Type() string { return s.typ }
func (s *stubAction) Execute(_ context.Context, ev domain.Event, params json.RawMessage) error {
	s.calls++
	s.gotEv = ev
	s.gotRaw = params
	return nil
}

type countMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func newCountMetrics() *countMetrics { return &countMetrics{counts: map[string]int{}} }
func (m *countMetrics) IncPluginError(plugin, hook string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[plugin+"/"+hook]++
}

func enabled(name string, mode domain.FailMode) *domain.CollectionConfig {
	return &domain.CollectionConfig{Plugins: map[string]domain.PluginConfig{
		name: {Enabled: true, FailMode: mode},
	}}
}

func TestHostNilIsNoOp(t *testing.T) {
	var h *Host
	cfg := enabled("x", domain.FailClosed)
	if h.HasPreStore(cfg) {
		t.Fatal("nil host must report no pre-store plugins")
	}
	if err := h.RunPreStore(context.Background(), cfg, "c", &domain.Event{}, EntityView{}); err != nil {
		t.Fatalf("nil host RunPreStore: %v", err)
	}
	h.RunPostStore(context.Background(), cfg, domain.Event{})
	if _, ok := h.Action("any"); ok {
		t.Fatal("nil host must resolve no actions")
	}
}

func TestHostResolvesEnabledOnly(t *testing.T) {
	on := &stubPre{name: "on"}
	off := &stubPre{name: "off"}
	h := NewHost([]PreStorePlugin{on, off}, nil, nil, nil)

	cfg := &domain.CollectionConfig{Plugins: map[string]domain.PluginConfig{
		"on":  {Enabled: true},
		"off": {Enabled: false},
	}}
	if !h.HasPreStore(cfg) {
		t.Fatal("expected an enabled pre-store plugin")
	}
	if err := h.RunPreStore(context.Background(), cfg, "c", &domain.Event{EntityID: "e"}, EntityView{}); err != nil {
		t.Fatalf("RunPreStore: %v", err)
	}
	if on.calls != 1 {
		t.Fatalf("enabled plugin calls = %d, want 1", on.calls)
	}
	if off.calls != 0 {
		t.Fatalf("disabled plugin ran %d times", off.calls)
	}
}

func TestHostNoPluginsConfigured(t *testing.T) {
	on := &stubPre{name: "on"}
	h := NewHost([]PreStorePlugin{on}, nil, nil, nil)
	cfg := &domain.CollectionConfig{} // no Plugins map at all
	if h.HasPreStore(cfg) {
		t.Fatal("a collection with no plugin config must have no pre-store plugins")
	}
}

func TestRunPreStoreOrderDeterministic(t *testing.T) {
	var order []string
	mk := func(name string) *stubPre {
		return &stubPre{name: name, mutate: func(*EventDraft, EntityView) { order = append(order, name) }}
	}
	h := NewHost([]PreStorePlugin{mk("a"), mk("b"), mk("c")}, nil, nil, nil)
	cfg := &domain.CollectionConfig{Plugins: map[string]domain.PluginConfig{
		"a": {Enabled: true}, "b": {Enabled: true}, "c": {Enabled: true},
	}}
	if err := h.RunPreStore(context.Background(), cfg, "c", &domain.Event{}, EntityView{}); err != nil {
		t.Fatalf("RunPreStore: %v", err)
	}
	if got := order; len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("plugin order = %v, want [a b c] (registration order)", got)
	}
}

func TestRunPreStoreFailClosedAborts(t *testing.T) {
	boom := errors.New("boom")
	p := &stubPre{name: "hc", failWith: boom}
	h := NewHost([]PreStorePlugin{p}, nil, nil, nil)
	err := h.RunPreStore(context.Background(), enabled("hc", domain.FailClosed), "c", &domain.Event{}, EntityView{})
	var fc *FailClosedError
	if !errors.As(err, &fc) {
		t.Fatalf("err = %v, want *FailClosedError", err)
	}
	if fc.Plugin != "hc" || !errors.Is(err, boom) {
		t.Fatalf("FailClosedError wrong: %+v", fc)
	}
}

func TestRunPreStoreFailOpenContinues(t *testing.T) {
	met := newCountMetrics()
	p := &stubPre{name: "hc", failWith: errors.New("boom")}
	h := NewHost([]PreStorePlugin{p}, nil, nil, met)
	// Unset fail mode defaults to fail-open: no error surfaces.
	err := h.RunPreStore(context.Background(), enabled("hc", ""), "c", &domain.Event{}, EntityView{})
	if err != nil {
		t.Fatalf("fail-open must not surface an error, got %v", err)
	}
	if met.counts["hc/pre_store"] != 1 {
		t.Fatalf("fail-open did not increment plugin_errors_total: %+v", met.counts)
	}
}

func TestRunPreStoreSettersLand(t *testing.T) {
	p := &stubPre{name: "hc", mutate: func(d *EventDraft, _ EntityView) {
		d.SetIntegrity("sha256:h", "sha256:p")
		d.PutMeta("checked", true)
	}}
	h := NewHost([]PreStorePlugin{p}, nil, nil, nil)
	ev := &domain.Event{EntityID: "e", Op: domain.OpCreate, Changes: []diff.Change{{Path: "a", Op: diff.OpAdd, New: 1}}}
	if err := h.RunPreStore(context.Background(), enabled("hc", domain.FailClosed), "crm.deals", ev, EntityView{}); err != nil {
		t.Fatalf("RunPreStore: %v", err)
	}
	if ev.Integrity == nil || ev.Integrity.Hash != "sha256:h" || ev.Integrity.PrevHash != "sha256:p" {
		t.Fatalf("SetIntegrity did not land: %+v", ev.Integrity)
	}
	if ev.Meta["checked"] != true {
		t.Fatalf("PutMeta did not land: %+v", ev.Meta)
	}
}

func TestDraftExposesReadFields(t *testing.T) {
	ev := &domain.Event{EntityID: "e", Op: domain.OpUpdate, Author: "alice", Source: "crm"}
	var seen struct {
		coll, eid, author, source string
		op                        domain.EntityOp
	}
	p := &stubPre{name: "hc", mutate: func(d *EventDraft, _ EntityView) {
		seen.coll, seen.eid, seen.op = d.Collection(), d.EntityID(), d.Op()
		seen.author, seen.source = d.Author(), d.Source()
	}}
	h := NewHost([]PreStorePlugin{p}, nil, nil, nil)
	if err := h.RunPreStore(context.Background(), enabled("hc", domain.FailClosed), "crm.deals", ev, EntityView{}); err != nil {
		t.Fatalf("RunPreStore: %v", err)
	}
	if seen.coll != "crm.deals" || seen.eid != "e" || seen.op != domain.OpUpdate || seen.author != "alice" || seen.source != "crm" {
		t.Fatalf("draft read fields wrong: %+v", seen)
	}
}

func TestRunPostStoreEnabledOnly(t *testing.T) {
	on := &stubPost{name: "idx"}
	off := &stubPost{name: "other"}
	h := NewHost(nil, []PostStorePlugin{on, off}, nil, nil)
	cfg := &domain.CollectionConfig{Plugins: map[string]domain.PluginConfig{"idx": {Enabled: true}}}
	h.RunPostStore(context.Background(), cfg, domain.Event{EntityID: "e"})
	if on.calls != 1 || off.calls != 0 {
		t.Fatalf("post-store ran wrong set: on=%d off=%d", on.calls, off.calls)
	}
}

func TestActionResolution(t *testing.T) {
	a := &stubAction{typ: "notify"}
	h := NewHost(nil, nil, []ActionPlugin{a}, nil)
	got, ok := h.Action("notify")
	if !ok || got != a {
		t.Fatalf("Action(notify) = %v, %v", got, ok)
	}
	if _, ok := h.Action("unknown"); ok {
		t.Fatal("unknown action type must not resolve")
	}
}
