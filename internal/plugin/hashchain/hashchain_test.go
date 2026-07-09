package hashchain

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
)

// link runs the plugin over a planned event with a given previous event and
// returns the integrity it stamped on the event.
func link(t *testing.T, collection string, ev *domain.Event, last *domain.Event) *domain.Integrity {
	t.Helper()
	host := plugin.NewHost([]plugin.PreStorePlugin{New()}, nil, nil, nil)
	cfg := &domain.CollectionConfig{Plugins: map[string]domain.PluginConfig{
		Name: {Enabled: true, FailMode: domain.FailClosed},
	}}
	if err := host.RunPreStore(context.Background(), cfg, collection, ev, plugin.EntityView{LastEvent: last}); err != nil {
		t.Fatalf("RunPreStore: %v", err)
	}
	if ev.Integrity == nil {
		t.Fatal("plugin did not set integrity")
	}
	return ev.Integrity
}

func change(path string, v any) []diff.Change {
	return []diff.Change{{Path: path, Op: diff.OpAdd, New: v}}
}

func TestGenesisDeterministicAndCollectionBound(t *testing.T) {
	a1 := Genesis("crm.deals")
	a2 := Genesis("crm.deals")
	b := Genesis("crm.contacts")
	if a1 != a2 {
		t.Fatalf("genesis not deterministic: %q vs %q", a1, a2)
	}
	if a1 == b {
		t.Fatal("genesis must differ across collections")
	}
	if !strings.HasPrefix(a1, "sha256:") {
		t.Fatalf("genesis missing scheme: %q", a1)
	}
}

func TestFirstLinkChainsOffGenesis(t *testing.T) {
	ev := &domain.Event{EntityID: "d-1", Op: domain.OpCreate, Changes: change("amount", 100.0)}
	intg := link(t, "crm.deals", ev, nil)
	if intg.PrevHash != Genesis("crm.deals") {
		t.Fatalf("first link prev_hash = %q, want genesis %q", intg.PrevHash, Genesis("crm.deals"))
	}
	if !strings.HasPrefix(intg.Hash, "sha256:") {
		t.Fatalf("hash missing scheme: %q", intg.Hash)
	}
}

func TestChainContinuity(t *testing.T) {
	collection := "crm.deals"
	e1 := &domain.Event{EntityID: "d-1", Op: domain.OpCreate, Changes: change("amount", 100.0)}
	i1 := link(t, collection, e1, nil)
	e1.Integrity = i1

	e2 := &domain.Event{EntityID: "d-1", Op: domain.OpUpdate, Changes: change("amount", 250.0)}
	i2 := link(t, collection, e2, e1)
	if i2.PrevHash != i1.Hash {
		t.Fatalf("link 2 prev_hash = %q, want %q", i2.PrevHash, i1.Hash)
	}
}

func TestDeterminism(t *testing.T) {
	mk := func() *domain.Event {
		return &domain.Event{EntityID: "d-1", Op: domain.OpUpdate, Author: "alice", Source: "crm", Changes: change("amount", 250.0)}
	}
	a := link(t, "crm.deals", mk(), nil)
	b := link(t, "crm.deals", mk(), nil)
	if a.Hash != b.Hash {
		t.Fatalf("same event produced different hashes: %q vs %q", a.Hash, b.Hash)
	}
}

func TestSensitivityToSignedFields(t *testing.T) {
	base := func() *domain.Event {
		return &domain.Event{EntityID: "d-1", Op: domain.OpUpdate, Author: "alice", Source: "crm", Changes: change("amount", 250.0)}
	}
	ref := link(t, "crm.deals", base(), nil).Hash

	cases := map[string]func(*domain.Event){
		"op":        func(e *domain.Event) { e.Op = domain.OpCreate },
		"entity_id": func(e *domain.Event) { e.EntityID = "d-2" },
		"author":    func(e *domain.Event) { e.Author = "bob" },
		"source":    func(e *domain.Event) { e.Source = "erp" },
		"changes":   func(e *domain.Event) { e.Changes = change("amount", 999.0) },
		"ts_source": func(e *domain.Event) { e.TSSource = time.Unix(1000, 0) },
		"flow":      func(e *domain.Event) { e.Flow = &domain.Flow{ID: "f-1"} },
	}
	for name, mutate := range cases {
		ev := base()
		mutate(ev)
		if got := link(t, "crm.deals", ev, nil).Hash; got == ref {
			t.Fatalf("changing %s did not change the hash", name)
		}
	}
}

// FR-8.5: adding an absent optional field must not change the canonical form of
// events that omit it, so old chains keep verifying after a schema extension.
func TestToleranceToAbsentOptionalFields(t *testing.T) {
	withFlow := &domain.Event{EntityID: "d-1", Op: domain.OpCreate, Changes: change("amount", 100.0), Flow: nil}
	bare := &domain.Event{EntityID: "d-1", Op: domain.OpCreate, Changes: change("amount", 100.0)}
	if link(t, "crm.deals", withFlow, nil).Hash != link(t, "crm.deals", bare, nil).Hash {
		t.Fatal("a nil flow must not contribute to the hash (FR-8.5)")
	}
	// An empty changes list (a delete) is treated as absent, not as [].
	d1 := &domain.Event{EntityID: "d-1", Op: domain.OpDelete}
	d2 := &domain.Event{EntityID: "d-1", Op: domain.OpDelete, Changes: nil}
	if link(t, "crm.deals", d1, nil).Hash != link(t, "crm.deals", d2, nil).Hash {
		t.Fatal("nil vs empty changes must hash identically")
	}
}

// A delete link followed by a reincarnation create continues the chain: the
// delete is itself a link.
func TestReincarnationContinuesChain(t *testing.T) {
	collection := "crm.deals"
	create := &domain.Event{EntityID: "d-1", Op: domain.OpCreate, Changes: change("amount", 100.0)}
	ci := link(t, collection, create, nil)
	create.Integrity = ci

	del := &domain.Event{EntityID: "d-1", Op: domain.OpDelete}
	di := link(t, collection, del, create)
	if di.PrevHash != ci.Hash {
		t.Fatalf("delete link prev_hash = %q, want %q", di.PrevHash, ci.Hash)
	}
	del.Integrity = di

	reborn := &domain.Event{EntityID: "d-1", Op: domain.OpCreate, Changes: change("amount", 500.0)}
	ri := link(t, collection, reborn, del)
	if ri.PrevHash != di.Hash {
		t.Fatalf("reincarnation link prev_hash = %q, want %q", ri.PrevHash, di.Hash)
	}
}

// The write-time projection and the verify-time projection (rebuilt from the
// stored event) must hash identically — the single-source-of-truth contract.
func TestProjectionFromEventMatchesDraft(t *testing.T) {
	ev := &domain.Event{
		EntityID: "d-1", Op: domain.OpUpdate, Author: "alice", Source: "crm",
		TSSource: time.Date(2026, 6, 16, 10, 0, 0, 0, time.UTC),
		Changes:  change("amount", 250.0),
		Flow:     &domain.Flow{ID: "f-1", Step: "approve"},
	}
	writeHash := link(t, "crm.deals", ev, nil).Hash

	// Rebuild from the (now stamped) event as :verify would, with the same prev.
	verifyHash, err := Hash(Genesis("crm.deals"), ProjectionFromEvent(*ev))
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if writeHash != verifyHash {
		t.Fatalf("verify projection diverged from write: %q vs %q", verifyHash, writeHash)
	}
}

// A full flow block (every ref shape) and a changes list covering add/change/
// remove must canonicalize without error and contribute deterministically.
func TestRichFlowAndChangeKindsCanonicalize(t *testing.T) {
	ev := &domain.Event{
		EntityID: "d-1", Op: domain.OpUpdate,
		Changes: []diff.Change{
			{Path: "a", Op: diff.OpAdd, New: 1.0},
			{Path: "b", Op: diff.OpChange, Old: 1.0, New: 2.0},
			{Path: "c", Op: diff.OpRemove, Old: "x"},
		},
		Flow: &domain.Flow{
			ID:   "f-1",
			Step: "approve",
			CausedBy: []domain.FlowRef{
				{ActivityID: "act-1"},
				{Collection: "crm.deals", EntityID: "d-0", Version: 3},
				{Collection: "crm.deals", EntityID: "d-0", EventID: "ev-9"},
			},
		},
	}
	first := link(t, "crm.deals", ev, nil).Hash

	again := &domain.Event{EntityID: "d-1", Op: domain.OpUpdate, Changes: ev.Changes, Flow: ev.Flow}
	if link(t, "crm.deals", again, nil).Hash != first {
		t.Fatal("rich flow/changes hash is not deterministic")
	}
	// Dropping one caused_by ref must change the hash (the flow block is signed).
	pruned := &domain.Event{
		EntityID: "d-1", Op: domain.OpUpdate, Changes: ev.Changes,
		Flow: &domain.Flow{ID: "f-1", Step: "approve", CausedBy: ev.Flow.CausedBy[:2]},
	}
	if link(t, "crm.deals", pruned, nil).Hash == first {
		t.Fatal("removing a caused_by ref must change the hash")
	}
}

// ts_source at sub-millisecond precision must not change the hash, because
// storage round-trips only milliseconds (BSON datetime).
func TestTSSourceTruncatedToMillis(t *testing.T) {
	base := time.Date(2026, 6, 16, 10, 0, 0, 123_000_000, time.UTC)
	sub := base.Add(456 * time.Microsecond)
	a, _ := Hash("x", Projection{Op: domain.OpCreate, EntityID: "e", TSSource: base})
	b, _ := Hash("x", Projection{Op: domain.OpCreate, EntityID: "e", TSSource: sub})
	if a != b {
		t.Fatal("sub-millisecond ts_source changed the hash; storage cannot reproduce it")
	}
}
