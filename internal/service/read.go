package service

import (
	"context"
	"errors"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// Reader serves history, current-state, and point-in-time reads. A thin
// orchestrator: query building lives in transport, index use in storage.
type Reader struct {
	events    domain.EventRepository
	current   domain.CurrentRepository
	snapshots domain.SnapshotRepository // optional; nil replays every read from genesis
}

func NewReader(events domain.EventRepository, current domain.CurrentRepository, snapshots domain.SnapshotRepository) *Reader {
	return &Reader{events: events, current: current, snapshots: snapshots}
}

// History returns the events matching the filter. The caller has already
// validated and bounded it (order, limit, cursor).
func (r *Reader) History(ctx context.Context, collection string, f domain.EventFilter) ([]domain.Event, error) {
	return r.events.ListEvents(ctx, collection, f)
}

func (r *Reader) CurrentState(ctx context.Context, collection, entityID string) (*domain.CurrentState, error) {
	return r.current.Get(ctx, collection, entityID)
}

// PointInTimeQuery selects the target version for a StateAt read. Pointers
// let "unset" be distinct from zero; both nil means "latest version".
type PointInTimeQuery struct {
	Version *int64
	At      *time.Time
}

// ReconstructedState is a point-in-time read result. SnapshotVersion is the
// anchor used (0 = replayed from genesis) and EventsApplied the tail length —
// together these form the reconstructed_from block the API exposes.
type ReconstructedState struct {
	EntityID        string
	Version         int64
	TS              time.Time
	Deleted         bool
	State           map[string]any
	SnapshotVersion int64
	EventsApplied   int
}

// StateAt reconstructs an entity's state at a past version or time. Returns
// domain.ErrNotFound when the entity has no history or an `at` cutoff predates
// its first event.
func (r *Reader) StateAt(ctx context.Context, collection, entityID string, q PointInTimeQuery) (*ReconstructedState, error) {
	last, err := r.events.LastEvent(ctx, collection, entityID)
	if err != nil {
		return nil, err // ErrNotFound → 404 (unknown entity)
	}

	target, err := r.resolveTarget(ctx, collection, entityID, last, q)
	if err != nil {
		return nil, err
	}

	base, baseDeleted, snapVersion, snapTS := r.anchor(ctx, collection, entityID, target)

	// Tail: events (snapVersion, target] in version order.
	f := domain.EventFilter{EntityID: entityID, MaxVersion: target, OrderBy: domain.OrderVersion, Order: domain.OrderAsc}
	if snapVersion > 0 {
		f.After = &domain.Position{Version: snapVersion}
	}
	tail, err := r.events.ListEvents(ctx, collection, f)
	if err != nil {
		return nil, err
	}

	state, deleted, err := domain.Reconstruct(base, baseDeleted, tail)
	if err != nil {
		return nil, err
	}

	ts := snapTS
	if n := len(tail); n > 0 {
		ts = tail[n-1].TSReceived // the target version's received time
	}
	return &ReconstructedState{
		EntityID:        entityID,
		Version:         target,
		TS:              ts,
		Deleted:         deleted,
		State:           state,
		SnapshotVersion: snapVersion,
		EventsApplied:   len(tail),
	}, nil
}

// resolveTarget converts the query to a concrete version. An explicit version is
// clamped to the latest. An `at` cutoff resolves to the highest version received
// at or before it; ErrNotFound when the cutoff predates the entity.
func (r *Reader) resolveTarget(ctx context.Context, collection, entityID string, last *domain.Event, q PointInTimeQuery) (int64, error) {
	switch {
	case q.Version != nil:
		return min(*q.Version, last.Version), nil
	case q.At != nil:
		evs, err := r.events.ListEvents(ctx, collection, domain.EventFilter{
			EntityID: entityID, To: *q.At, OrderBy: domain.OrderVersion, Order: domain.OrderDesc, Limit: 1,
		})
		if err != nil {
			return 0, err
		}
		if len(evs) == 0 {
			return 0, domain.ErrNotFound // cutoff is before the first event
		}
		return evs[0].Version, nil
	default:
		return last.Version, nil
	}
}

// anchor returns the nearest snapshot at or below target. When none exists or
// the snapshot store is absent, returns the genesis base (empty, version 0).
func (r *Reader) anchor(ctx context.Context, collection, entityID string, target int64) (state map[string]any, deleted bool, version int64, ts time.Time) {
	if r.snapshots == nil {
		return nil, false, 0, time.Time{}
	}
	snap, err := r.snapshots.Nearest(ctx, collection, entityID, target)
	if errors.Is(err, domain.ErrNotFound) || err != nil {
		// Snapshot miss or lookup failure: degrade to genesis replay. Snapshots are
		// an optimization, not a correctness requirement.
		return nil, false, 0, time.Time{}
	}
	return snap.State, snap.Deleted, snap.Version, snap.TS
}
