package domain

import (
	"context"
	"time"
)

// OrderField selects which event field history is ordered by. Version is the
// canonical arrival order; the empty value means version, so the zero EventFilter
// keeps its historical "by version" behaviour.
type OrderField string

const (
	OrderVersion  OrderField = ""            // monotonic version (arrival order)
	OrderSource   OrderField = "ts_source"   // source-observed time
	OrderReceived OrderField = "ts_received" // API-received time
)

// SortOrder is the direction of an ordering. The empty value is ascending.
type SortOrder string

const (
	OrderAsc  SortOrder = ""
	OrderDesc SortOrder = "desc"
)

// Position is a decoded pagination cursor: the sort key of the last event on the
// previous page. Version is always set (unique within an entity, breaks ties when
// a timestamp repeats); TS is set only for time-based orderings.
type Position struct {
	Version int64
	TS      time.Time
}

// EventFilter narrows ListEvents. The zero value lists an entity's whole history
// oldest-first by version. From/To bound the received-time window; Path matches
// events touching a field at or under that path; After is the pagination cursor
// (nil = first page). Empty string / zero fields impose no constraint.
type EventFilter struct {
	EntityID string
	From     time.Time
	To       time.Time
	Author   string
	Op       EntityOp
	Path     string
	Source   string
	OrderBy  OrderField
	Order    SortOrder
	Limit    int // 0 means no limit
	After    *Position
	// MaxVersion bounds results to versions ≤ this value (0 = unbounded).
	// Paired with After it selects the events (snapshot_version, target] for reconstruction.
	MaxVersion int64
}

// EventRepository is the append-only event store port. The logical collection name
// selects the physical ev_* collection; tenant database comes from ctx.
type EventRepository interface {
	// AppendEvent assigns the next monotonic version within the entity and inserts
	// the event, retrying on the unique {eid,v} guard so concurrent writers serialize.
	// On success ev.Version and ev.TSStored are set.
	AppendEvent(ctx context.Context, collection string, ev *Event) error

	// AppendEvents inserts a pre-versioned batch in one insertMany. Unlike AppendEvent
	// it does not assign versions or retry: the caller (the fast batcher) has already
	// sequenced them per entity. The unique {eid,v} index remains the final guard.
	AppendEvents(ctx context.Context, collection string, evs []*Event) error

	// LastEvent returns the highest-version event for the entity, or ErrNotFound.
	LastEvent(ctx context.Context, collection, entityID string) (*Event, error)

	// ListEvents returns events matching the filter.
	ListEvents(ctx context.Context, collection string, f EventFilter) ([]Event, error)
}

// CurrentRepository maintains the materialized current state (cur_*).
type CurrentRepository interface {
	Get(ctx context.Context, collection, entityID string) (*CurrentState, error)
	Upsert(ctx context.Context, collection string, st *CurrentState) error
}

// SnapshotRepository stores point-in-time snapshots (sn_*). The snapshot builder
// writes; point-in-time reads call Nearest then replay the event tail.
type SnapshotRepository interface {
	// Save upserts a snapshot by {entity, version}, so the builder may re-run
	// for a version it already snapped without creating a duplicate.
	Save(ctx context.Context, collection string, snap *Snapshot) error

	// Nearest returns the snapshot with the greatest version ≤ maxVersion, or
	// ErrNotFound if none exists at or below it.
	Nearest(ctx context.Context, collection, entityID string, maxVersion int64) (*Snapshot, error)
}

// FlowStore is the activity/flow port. Activities are appended to ev__flow;
// reading a flow fans out over the tenant's ev_* collections for change events
// tagged with the flow. Tenant database is taken from ctx.
type FlowStore interface {
	// AppendActivity persists an activity. ActivityID and FlowID are assigned by
	// the caller before the call; on success a.TSStored is set.
	AppendActivity(ctx context.Context, a *Activity) error

	// FlowActivities returns the activities recorded under a flow.
	FlowActivities(ctx context.Context, flowID string) ([]Activity, error)

	// FlowEvents fans out across the tenant's ev_* collections and returns change
	// events whose flow block carries the flow id, each tagged with its collection.
	FlowEvents(ctx context.Context, flowID string) ([]FlowEvent, error)
}
