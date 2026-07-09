package service

import (
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
	"github.com/max-trifonov/letopis/internal/plugin/hashchain"
)

// realHashChainFixture wires the actual hash-chain plugin through the Ingester
// over in-memory repos, so the chain is produced by the real algorithm on the
// real write paths (not a stub).
func realHashChainFixture(t *testing.T) (*Ingester, *fakeEvents) {
	t.Helper()
	host := plugin.NewHost([]plugin.PreStorePlugin{hashchain.New()}, nil, nil, nil)
	return pluginFixture(t, host, enabledClosed(hashchain.Name))
}

// verifyChain replays the stored events and checks each link against a fresh
// recomputation — the same projection :verify (S5-03) will use.
func verifyChain(t *testing.T, collection string, evs []domain.Event) {
	t.Helper()
	prev := hashchain.Genesis(collection)
	for i, ev := range evs {
		if ev.Integrity == nil {
			t.Fatalf("event %d (v%d) has no integrity link", i, ev.Version)
		}
		if ev.Integrity.PrevHash != prev {
			t.Fatalf("event v%d prev_hash = %q, want %q", ev.Version, ev.Integrity.PrevHash, prev)
		}
		want, err := hashchain.Hash(prev, hashchain.ProjectionFromEvent(ev))
		if err != nil {
			t.Fatalf("recompute v%d: %v", ev.Version, err)
		}
		if ev.Integrity.Hash != want {
			t.Fatalf("event v%d hash = %q, recomputed %q", ev.Version, ev.Integrity.Hash, want)
		}
		prev = ev.Integrity.Hash
	}
}

func TestRealHashChainStrictPathAndReincarnation(t *testing.T) {
	ing, events := realHashChainFixture(t)
	ctx := authedCtx()

	if _, err := ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}); err != nil {
		t.Fatalf("v1: %v", err)
	}
	if _, err := ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(250)}); err != nil {
		t.Fatalf("v2: %v", err)
	}
	if _, err := ing.Delete(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1"}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := ing.State(ctx, IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(500)}); err != nil {
		t.Fatalf("reincarnate: %v", err)
	}

	evs := events.byEntity["crm.deals|d-1"]
	if len(evs) != 4 {
		t.Fatalf("want 4 events, got %d", len(evs))
	}
	verifyChain(t, "crm.deals", evs)
}

func TestRealHashChainFastBatch(t *testing.T) {
	ing, events := realHashChainFixture(t)

	errs := ing.IngestBatch(authedCtx(), []BatchItem{
		{Kind: KindState, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(100)}},
		{Kind: KindState, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1", State: deal(250)}},
		{Kind: KindDelete, Command: IngestCommand{Collection: "crm.deals", EntityID: "d-1"}},
	})
	for i, e := range errs {
		if e != nil {
			t.Fatalf("item %d: %v", i, e)
		}
	}
	verifyChain(t, "crm.deals", events.byEntity["crm.deals|d-1"])
}
