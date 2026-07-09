package service

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
)

// ErrInvalidDiff is returned when a client-supplied diff cannot be applied to
// the entity's current state. Transport maps it to 400 — the diff is malformed
// relative to what the server holds.
var ErrInvalidDiff = errors.New("service: diff does not apply to current state")

// IngestCommand is the boundary DTO between transport and the write use-case.
type IngestCommand struct {
	Collection      string
	EntityID        string
	Op              domain.EntityOp        // optional; "" lets the use-case derive it
	Mode            domain.ReliabilityMode // per-request override; "" defers to the collection default
	ExpectedVersion *int64                 // optimistic lock; nil disables the check
	Author          string
	Source          string
	TSSource        time.Time
	EventID         string
	RequestID       string
	ClientIP        string
	Meta            map[string]any
	State           map[string]any
	Changes         []diff.Change
	Flow            *domain.Flow // optional business-flow block
}

// IngestResult is what a write produced. NoChanges=true → 200 (no-op);
// Async=true → 202 with Ticket; Deduplicated=true → repeat caught by a barrier.
type IngestResult struct {
	EntityID     string
	Version      int64
	ChangesCount int
	Op           domain.EntityOp
	NoChanges    bool
	Async        bool
	Ticket       string
	Deduplicated bool
}

// Ingester orchestrates synchronous writes: resolve config, plan the diff,
// append the event, and refresh the materialized current state.
type Ingester struct {
	cfg       *ConfigResolver
	events    domain.EventRepository
	current   domain.CurrentRepository
	snapshots *SnapshotBuilder // optional; nil disables snapshot materialization
	rules     *RulesEngine     // optional; nil disables rule evaluation
	plugins   *plugin.Host     // optional; nil-safe, no plugin hooks run
	now       func() time.Time
}

// IngesterOption configures an Ingester at construction.
type IngesterOption func(*Ingester)

// WithSnapshots enables snapshot materialization on the version interval.
// A nil builder leaves snapshots disabled.
func WithSnapshots(b *SnapshotBuilder) IngesterOption {
	return func(i *Ingester) { i.snapshots = b }
}

// WithRules enables post-store rule evaluation. A nil engine is a no-op.
func WithRules(e *RulesEngine) IngesterOption {
	return func(i *Ingester) { i.rules = e }
}

// WithPlugins wires the plugin host so the pre-store and post-store hooks run on
// both write paths. A nil host runs no hooks, leaving core behaviour unchanged.
func WithPlugins(h *plugin.Host) IngesterOption {
	return func(i *Ingester) { i.plugins = h }
}

func NewIngester(cfg *ConfigResolver, events domain.EventRepository, current domain.CurrentRepository, opts ...IngesterOption) *Ingester {
	i := &Ingester{cfg: cfg, events: events, current: current, now: time.Now}
	for _, opt := range opts {
		opt(i)
	}
	return i
}

// Config resolves (and, with auto-create, provisions) the collection. Transport
// calls it before reading the body so it can enforce the per-collection size limit;
// the value is cached, so the subsequent write does not re-read it.
func (i *Ingester) Config(ctx context.Context, collection string) (*domain.CollectionConfig, error) {
	return i.cfg.EnsureCollection(ctx, collection)
}

// State accepts a full state and derives the diff against what is stored.
// An identical state is a no-op.
func (i *Ingester) State(ctx context.Context, cmd IngestCommand) (IngestResult, error) {
	return i.applyOne(ctx, KindState, cmd)
}

// Diff accepts a ready-made diff. For op=create a full State may be supplied
// instead of changes; otherwise the diff is applied to the current state and a
// diff that does not apply is rejected.
func (i *Ingester) Diff(ctx context.Context, cmd IngestCommand) (IngestResult, error) {
	return i.applyOne(ctx, KindDiff, cmd)
}

// Delete records a deletion: a delete event with no changes, the current state
// flagged deleted but its last value retained, and history preserved so a later
// create reincarnates the entity.
func (i *Ingester) Delete(ctx context.Context, cmd IngestCommand) (IngestResult, error) {
	return i.applyOne(ctx, KindDelete, cmd)
}

// applyOne is the synchronous single-write path shared by strict and worker.
func (i *Ingester) applyOne(ctx context.Context, kind Kind, cmd IngestCommand) (IngestResult, error) {
	cfg, cur, err := i.prepare(ctx, cmd)
	if err != nil {
		return IngestResult{}, err
	}
	_, curVersion := baseState(cur)
	if !versionMatches(cmd.ExpectedVersion, curVersion) {
		return IngestResult{}, domain.ErrVersionConflict
	}
	ev, newState, deleted, noChange, err := i.planEvent(cfg, kind, cmd, cur)
	if err != nil {
		return IngestResult{}, err
	}
	if noChange {
		return IngestResult{EntityID: cmd.EntityID, Version: curVersion, NoChanges: true}, nil
	}
	// Pre-store hooks run before commit and may enrich the event (e.g. the
	// hash-chain writes Integrity). A fail-closed plugin error aborts the write;
	// in strict it surfaces as 422, in the worker it settles the ticket as failed.
	if i.plugins.HasPreStore(cfg) {
		last, err := i.lastEvent(ctx, cmd.Collection, cmd.EntityID)
		if err != nil {
			return IngestResult{}, err
		}
		prev := plugin.EntityView{LastEvent: last, Current: cur}
		if err := i.plugins.RunPreStore(ctx, cfg, cmd.Collection, ev, prev); err != nil {
			return IngestResult{}, err
		}
	}
	if err := i.commitEvent(ctx, cmd, ev, newState, deleted); err != nil {
		if errors.Is(err, domain.ErrDuplicateEvent) {
			// The storage barrier rejected a repeat event_id: the original write is
			// already durable, so we leave cur_* untouched and report a no-op.
			// Reached on a reclaimed duplicate (XAUTOCLAIM) or a repeat whose Redis
			// dedup key had expired.
			return IngestResult{EntityID: cmd.EntityID, Version: curVersion, Deduplicated: true}, nil
		}
		return IngestResult{}, err
	}
	// Post-store side effects: best-effort, never fail the durable write.
	i.snapshots.Build(ctx, cmd.Collection, cmd.EntityID, ev.Version, ev.TSReceived, newState, deleted, cfg.SnapshotInterval)
	i.rules.Evaluate(ctx, cmd.Collection, *ev)
	i.plugins.RunPostStore(ctx, cfg, *ev)
	return IngestResult{EntityID: cmd.EntityID, Version: ev.Version, ChangesCount: len(ev.Changes), Op: ev.Op}, nil
}

// lastEvent loads the entity's most recent event, the pre-store hooks' source of
// the hash-chain prev_hash. A missing entity returns nil (its first event).
func (i *Ingester) lastEvent(ctx context.Context, collection, entityID string) (*domain.Event, error) {
	ev, err := i.events.LastEvent(ctx, collection, entityID)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return ev, nil
}

// planEvent turns a command into the event and new state it would produce, with
// no storage access. Version is not assigned here. Returns noChange=true for
// identical-state no-ops; returns ErrInvalidDiff when a client diff does not apply.
func (i *Ingester) planEvent(cfg *domain.CollectionConfig, kind Kind, cmd IngestCommand, cur *domain.CurrentState) (*domain.Event, map[string]any, bool, bool, error) {
	base, _ := baseState(cur)
	switch kind {
	case KindState:
		changes := diff.Diff(base, cmd.State, diff.Options{ArrayKeys: cfg.ArrayKeys})
		if len(changes) == 0 {
			return nil, nil, false, true, nil
		}
		op := resolveOp(cmd.Op, cur, cfg)
		return i.buildEvent(cmd, op, changes), cmd.State, false, false, nil
	case KindDiff:
		op := cmd.Op
		if op == "" {
			op = resolveOp("", cur, cfg)
		}
		if op == domain.OpDelete {
			return i.buildEvent(cmd, op, cmd.Changes), base, true, false, nil
		}
		var (
			changes  []diff.Change
			newState map[string]any
		)
		if op == domain.OpCreate && cmd.Changes == nil {
			// Create-with-state: derive the diff from an empty base so history
			// records per-field adds rather than one opaque root replacement.
			changes = diff.Diff(map[string]any{}, cmd.State, diff.Options{ArrayKeys: cfg.ArrayKeys})
			newState = cmd.State
		} else {
			changes = cmd.Changes
			applied, err := diff.Apply(base, changes)
			if err != nil {
				return nil, nil, false, false, fmt.Errorf("%w: %v", ErrInvalidDiff, err)
			}
			m, ok := applied.(map[string]any)
			if !ok {
				return nil, nil, false, false, fmt.Errorf("%w: result is not an object", ErrInvalidDiff)
			}
			newState = m
		}
		if len(changes) == 0 {
			return nil, nil, false, true, nil
		}
		return i.buildEvent(cmd, op, changes), newState, false, false, nil
	case KindDelete:
		return i.buildEvent(cmd, domain.OpDelete, nil), base, true, false, nil
	default:
		return nil, nil, false, false, fmt.Errorf("service: unknown ingest kind %q", kind)
	}
}

// prepare resolves the collection config and loads the current state, the two
// reads every write path begins with.
func (i *Ingester) prepare(ctx context.Context, cmd IngestCommand) (*domain.CollectionConfig, *domain.CurrentState, error) {
	cfg, err := i.cfg.EnsureCollection(ctx, cmd.Collection)
	if err != nil {
		return nil, nil, err
	}
	cur, err := i.load(ctx, cmd.Collection, cmd.EntityID)
	if err != nil {
		return nil, nil, err
	}
	return cfg, cur, nil
}

func (i *Ingester) load(ctx context.Context, collection, entityID string) (*domain.CurrentState, error) {
	cur, err := i.current.Get(ctx, collection, entityID)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return cur, nil
}

// commitEvent appends the event and refreshes the current state. Not wrapped
// in a multi-document transaction: the event store is the source of truth and
// cur_* is a reconstructable materialization, so a crash between the two
// leaves history intact and the cache rebuildable.
func (i *Ingester) commitEvent(ctx context.Context, cmd IngestCommand, ev *domain.Event, newState map[string]any, deleted bool) error {
	if err := i.events.AppendEvent(ctx, cmd.Collection, ev); err != nil {
		return err
	}
	st := &domain.CurrentState{
		EntityID: cmd.EntityID,
		Version:  ev.Version,
		TS:       ev.TSReceived,
		Deleted:  deleted,
		State:    newState,
	}
	return i.current.Upsert(ctx, cmd.Collection, st)
}

func (i *Ingester) buildEvent(cmd IngestCommand, op domain.EntityOp, changes []diff.Change) *domain.Event {
	return &domain.Event{
		EntityID:   cmd.EntityID,
		Op:         op,
		Author:     cmd.Author,
		Source:     cmd.Source,
		TSSource:   cmd.TSSource,
		TSReceived: i.now().UTC(),
		EventID:    cmd.EventID,
		RequestID:  cmd.RequestID,
		Changes:    changes,
		Meta:       serverMeta(cmd.Meta, cmd.ClientIP),
		Flow:       cmd.Flow,
	}
}

// serverMeta records the caller IP alongside the client's metadata without
// mutating the request map or overriding a client-supplied ip.
func serverMeta(client map[string]any, ip string) map[string]any {
	if ip == "" {
		return client
	}
	out := map[string]any{}
	maps.Copy(out, client)
	if _, ok := out["ip"]; !ok {
		out["ip"] = ip
	}
	return out
}

// baseState returns the state a diff is computed against and the entity's
// current version. A missing or deleted entity has an empty base, so the next
// write is a fresh create (or reincarnation after delete); its version still
// carries forward for optimistic-lock checks.
func baseState(cur *domain.CurrentState) (map[string]any, int64) {
	if cur == nil {
		return map[string]any{}, 0
	}
	if cur.Deleted {
		return map[string]any{}, cur.Version
	}
	return cur.State, cur.Version
}

// resolveOp picks the entity-level op when the client did not state one:
// an update to a live entity, otherwise the collection's first_event_op for
// the first event (or for a create after delete).
func resolveOp(explicit domain.EntityOp, cur *domain.CurrentState, cfg *domain.CollectionConfig) domain.EntityOp {
	if explicit != "" {
		return explicit
	}
	if cur != nil && !cur.Deleted {
		return domain.OpUpdate
	}
	if cfg.FirstEventOp == domain.FirstEventUpdate {
		return domain.OpUpdate
	}
	return domain.OpCreate
}

func versionMatches(expected *int64, current int64) bool {
	return expected == nil || *expected == current
}
