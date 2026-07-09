// Package plugin is the pipeline extension surface of the Letopis core.
// Defines the three hook contracts — pre-store, post-store and action — and
// the Host that resolves and runs the hooks enabled for a collection.
// pkg/ext re-exports the public names so a distribution can supply its own plugins.
//
// A plugin sees a read-only view of the event and the previous entity state,
// and may mutate the event only through a narrow set of setters on EventDraft.
// It never touches a repository, so the model survives a future move to
// out-of-process plugins (WASM/gRPC) without a contract break.
package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

type PreStorePlugin interface {
	Name() string
	PreStore(ctx context.Context, draft *EventDraft, prev EntityView) error
}

type PostStorePlugin interface {
	Name() string
	PostStore(ctx context.Context, ev domain.Event)
}

type ActionPlugin interface {
	Type() string
	Execute(ctx context.Context, ev domain.Event, params json.RawMessage) error
}

// EntityView is the read-only view of an entity's state a PreStorePlugin is
// handed just before a new event is committed. LastEvent is the most recent stored
// event (nil for the entity's first event), the source of the hash-chain prev_hash.
// Within a fast batch it's the in-memory predecessor computed for the previous event
// of the same entity, not the stored tail — so a run of events for one entity
// chains correctly inside a single insertMany.
type EntityView struct {
	LastEvent *domain.Event
	Current   *domain.CurrentState
}

// EventDraft is the mutable wrapper a PreStorePlugin receives: read access to the
// planned event's descriptive fields plus a narrow set of setters. The version is
// not assigned yet (storage assigns it on append), so it's not exposed.
// Read fields are exposed by value only, so a plugin can't rewrite Op/Changes/Author
// behind the core's back.
type EventDraft struct {
	collection string
	ev         *domain.Event
}

func newDraft(collection string, ev *domain.Event) *EventDraft {
	return &EventDraft{collection: collection, ev: ev}
}

// Collection is the logical collection the event is being written to;
// the hash-chain genesis is bound to it.
func (d *EventDraft) Collection() string { return d.collection }

func (d *EventDraft) EntityID() string        { return d.ev.EntityID }
func (d *EventDraft) Op() domain.EntityOp     { return d.ev.Op }
func (d *EventDraft) Author() string          { return d.ev.Author }
func (d *EventDraft) Source() string          { return d.ev.Source }
func (d *EventDraft) TSSource() time.Time     { return d.ev.TSSource }

// Changes is the per-field diff. The slice is shared, not copied — a plugin must not mutate it.
func (d *EventDraft) Changes() []diff.Change { return d.ev.Changes }

func (d *EventDraft) Flow() *domain.Flow { return d.ev.Flow }

// SetIntegrity is the only way a plugin writes an integrity block.
func (d *EventDraft) SetIntegrity(hash, prevHash string) {
	d.ev.Integrity = &domain.Integrity{Hash: hash, PrevHash: prevHash}
}

// PutMeta lets a plugin annotate the event without being able to drop the
// client's existing metadata wholesale.
func (d *EventDraft) PutMeta(key string, value any) {
	if d.ev.Meta == nil {
		d.ev.Meta = map[string]any{}
	}
	d.ev.Meta[key] = value
}

// FailClosedError wraps the error of a fail-closed pre-store plugin: the write is
// rejected. Transport maps it to 422 (the event couldn't be processed — not a client
// validation error, not a core 500); the worker treats it as permanent and settles
// the ticket as failed.
type FailClosedError struct {
	Plugin string
	Err    error
}

func (e *FailClosedError) Error() string {
	return fmt.Sprintf("plugin %q rejected the write (fail-closed): %v", e.Plugin, e.Err)
}

func (e *FailClosedError) Unwrap() error { return e.Err }

// Metrics is the host's observability port: a fail-open plugin error is counted,
// labelled by plugin and hook. Nil disables instrumentation.
// Declared here to keep the package free of the metrics import.
type Metrics interface {
	IncPluginError(plugin, hook string)
}
