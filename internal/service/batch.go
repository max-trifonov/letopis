package service

import (
	"context"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
)

// BatchItem is one task in a fast-mode flush: the write kind and its command,
// in arrival order. The worker decodes queue payloads into these.
type BatchItem struct {
	Kind    Kind
	Command IngestCommand
}

// IngestBatch applies a run of fast-mode writes with one insertMany per
// collection. Returns a slice aligned to items: nil = success, non-nil = per-item
// failure. Versions are assigned locally — fast mode owns each entity's write tail
// (one goroutine per shard); the unique {eid,v} index is the final guard.
func (i *Ingester) IngestBatch(ctx context.Context, items []BatchItem) []error {
	errs := make([]error, len(items))
	order := []string{}
	byColl := map[string][]int{}
	for k, it := range items {
		c := it.Command.Collection
		if _, seen := byColl[c]; !seen {
			order = append(order, c)
		}
		byColl[c] = append(byColl[c], k)
	}
	for _, coll := range order {
		i.batchCollection(ctx, coll, items, byColl[coll], errs)
	}
	return errs
}

// batchCollection folds every item targeting one collection into a single
// insertMany, then refreshes the affected entities' current state. A per-item
// failure (config resolution, current-state load, or an unapplyable diff) is
// recorded against that item alone and does not abort its neighbours; a failed
// insertMany taints every item that fed it (the events did not land).
func (i *Ingester) batchCollection(ctx context.Context, collection string, items []BatchItem, idxs []int, errs []error) {
	cfg, err := i.cfg.EnsureCollection(ctx, collection)
	if err != nil {
		for _, k := range idxs {
			errs[k] = err
		}
		return
	}

	running := map[string]*domain.CurrentState{} // entity → state as the fold advances
	finals := map[string]*domain.CurrentState{}  // entity → state to upsert after the insert
	finalOrder := []string{}
	hasPre := i.plugins.HasPreStore(cfg)
	lastEv := map[string]*domain.Event{} // entity → its last event (carry-forward, hash-chain)
	var (
		events   []*domain.Event
		eventIdx []int           // parallel to events: the item index that produced each
		snaps    []snapCandidate // one per appended event, for the post-store snapshot pass
	)

	for _, k := range idxs {
		cmd := items[k].Command
		eid := cmd.EntityID
		cur, loaded := running[eid]
		if !loaded {
			c, lerr := i.load(ctx, collection, eid)
			if lerr != nil {
				errs[k] = lerr
				continue
			}
			cur = c
			running[eid] = cur
			if hasPre {
				last, lerr := i.lastEvent(ctx, collection, eid)
				if lerr != nil {
					errs[k] = lerr
					continue
				}
				lastEv[eid] = last
			}
		}
		ev, newState, deleted, noChange, perr := i.planEvent(cfg, items[k].Kind, cmd, cur)
		if perr != nil {
			errs[k] = perr
			continue
		}
		if noChange {
			continue // nothing to write; the ticket still settles as stored
		}
		nextV := int64(1)
		if cur != nil {
			nextV = cur.Version + 1
		}
		ev.Version = nextV
		if ev.TSStored.IsZero() {
			ev.TSStored = i.now().UTC()
		}
		// Pre-store hooks run in fold order so events of one entity chain off the
		// in-memory predecessor (lastEv), not the stored tail. A fail-closed error
		// fails this item alone and does not advance the chain.
		if hasPre {
			prev := plugin.EntityView{LastEvent: lastEv[eid], Current: cur}
			if perr := i.plugins.RunPreStore(ctx, cfg, collection, ev, prev); perr != nil {
				errs[k] = perr
				continue
			}
			lastEv[eid] = ev
		}
		events = append(events, ev)
		eventIdx = append(eventIdx, k)
		// One snapshot candidate per event: a batch may straddle multiple interval
		// boundaries, so we record each one rather than just the entity's final state.
		snaps = append(snaps, snapCandidate{entityID: eid, version: nextV, ts: ev.TSReceived, state: newState, deleted: deleted})

		next := &domain.CurrentState{EntityID: eid, Version: nextV, TS: ev.TSReceived, Deleted: deleted, State: newState}
		running[eid] = next
		if _, seen := finals[eid]; !seen {
			finalOrder = append(finalOrder, eid)
		}
		finals[eid] = next
	}

	if len(events) == 0 {
		return
	}
	if err := i.events.AppendEvents(ctx, collection, events); err != nil {
		for _, k := range eventIdx {
			errs[k] = err
		}
		return
	}
	// cur_* is a reconstructable materialization — a failed refresh doesn't undo
	// the durable history. Last write per entity wins; finalOrder is stable.
	for _, eid := range finalOrder {
		_ = i.current.Upsert(ctx, collection, finals[eid])
	}
	// Post-store side effects: best-effort, never fail the batch.
	for _, s := range snaps {
		i.snapshots.Build(ctx, collection, s.entityID, s.version, s.ts, s.state, s.deleted, cfg.SnapshotInterval)
	}
	for _, ev := range events {
		i.rules.Evaluate(ctx, collection, *ev)
		i.plugins.RunPostStore(ctx, cfg, *ev)
	}
}

// snapCandidate carries one event's per-version state for the post-store
// snapshot pass; the batch is fully folded before any snapshot is taken.
type snapCandidate struct {
	entityID string
	version  int64
	ts       time.Time
	state    map[string]any
	deleted  bool
}
