// Package domain holds the core entities and repository ports. Pure:
// knows the diff format but nothing about MongoDB, BSON, or HTTP.
package domain

import (
	"errors"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
)

// Storage sentinels — match with errors.Is, never on message text.
var (
	ErrNotFound = errors.New("domain: not found")

	// ErrVersionConflict means a concurrent writer claimed the version this append
	// targeted. Repos retry internally on the unique {eid,v} index and only surface
	// this if they can't converge.
	ErrVersionConflict = errors.New("domain: entity version conflict")

	// ErrDuplicateEvent is the storage-level idempotency barrier: the unique
	// {event_id} sparse index rejected a duplicate. Treat as a benign no-op —
	// the original write already landed. Catches repeats whose Redis dedup key expired
	// or that were redelivered via XAUTOCLAIM.
	ErrDuplicateEvent = errors.New("domain: duplicate event_id")
)

// EntityOp is the entity-level lifecycle transition (distinct from per-field diff ops).
type EntityOp string

const (
	OpCreate EntityOp = "create"
	OpUpdate EntityOp = "update"
	OpDelete EntityOp = "delete"
)

// Event is one immutable entry in an entity's history. Version is monotonic and
// gap-free within EntityID; storage assigns it on append.
type Event struct {
	EntityID   string
	Version    int64
	Op         EntityOp
	Author     string
	Source     string
	TSSource   time.Time // observed at the source system
	TSReceived time.Time // accepted by the API
	TSStored   time.Time // committed to storage
	EventID    string    // optional source event id, for idempotency
	RequestID  string    // request id, for tracing
	Changes    []diff.Change
	Meta       map[string]any
	Flow       *Flow
	Integrity  *Integrity
}

// Flow is the optional business-flow block. It ties the event into a business
// flow without Letopis interpreting it. ID is empty when the event is outside any flow.
type Flow struct {
	ID       string
	CausedBy []FlowRef
	Step     string
}

// FlowRef is a cause/reference edge in a flow. One of three shapes, distinguished
// by which fields are set: an activity ref (ActivityID), an event ref by version
// (Collection+EntityID+Version), or an event ref by client event id
// (Collection+EntityID+EventID). Dangling edges are allowed and resolved at read time.
type FlowRef struct {
	ActivityID string
	Collection string
	EntityID   string
	Version    int64
	EventID    string
}

// Integrity is the optional hash-chain block. The field exists so events
// round-trip without loss once the hash-chain plugin is enabled.
type Integrity struct {
	Hash     string
	PrevHash string
}

// CurrentState is the materialized latest state of an entity (cur_*).
// Exists so reads and diff-on-ingest avoid replaying history.
type CurrentState struct {
	EntityID string
	Version  int64
	TS       time.Time
	Deleted  bool
	State    map[string]any
}

// Snapshot is a materialized state pinned to a specific version (sn_*).
// Point-in-time reads start from the nearest snapshot with v ≤ target and replay
// only the tail of events, bounding reconstruction cost by the snapshot interval.
// Unlike cur_*, which keeps one document per entity, sn_* keeps many.
// Deleted marks a snapshot taken at a delete; State is the last non-empty state or nil.
type Snapshot struct {
	EntityID string
	Version  int64
	TS       time.Time
	Deleted  bool
	State    map[string]any
}
