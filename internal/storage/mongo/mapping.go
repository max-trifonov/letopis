package mongo

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

// BSON documents use short keys to keep per-event storage small at scale —
// events are the dominant volume.

type eventDoc struct {
	ID       bson.ObjectID  `bson:"_id,omitempty"`
	EID      string         `bson:"eid"`
	V        int64          `bson:"v"`
	Op       string         `bson:"op"`
	Author   string         `bson:"author,omitempty"`
	Src      string         `bson:"src,omitempty"`
	TSSource time.Time      `bson:"ts_src"`
	TSRcv    time.Time      `bson:"ts_rcv"`
	TSStored time.Time      `bson:"ts_st"`
	EventID  string         `bson:"event_id,omitempty"`
	RID      string         `bson:"rid,omitempty"`
	Changes  []changeDoc    `bson:"changes"`
	Meta     map[string]any `bson:"meta,omitempty"`
	Flow     *flowDoc       `bson:"flow,omitempty"`
	Intg     *intgDoc       `bson:"intg,omitempty"`
}

// changeDoc mirrors diff.Change with the storage's short op codes (o: a|c|r).
type changeDoc struct {
	P   string `bson:"p"`
	O   string `bson:"o"`
	Old any    `bson:"old"`
	New any    `bson:"new"`
}

type flowDoc struct {
	F    string       `bson:"f"`
	By   []flowRefDoc `bson:"by,omitempty"`
	Step string       `bson:"step,omitempty"`
}

// flowRefDoc holds whichever ref shape was set: aid for an activity ref, or
// (c,eid) plus v or ev for an event ref. All keys are omitempty so the stored
// document reflects only the fields that were set.
type flowRefDoc struct {
	AID string `bson:"aid,omitempty"`
	C   string `bson:"c,omitempty"`
	EID string `bson:"eid,omitempty"`
	V   int64  `bson:"v,omitempty"`
	Ev  string `bson:"ev,omitempty"`
}

func toFlowRefDocs(refs []domain.FlowRef) []flowRefDoc {
	if len(refs) == 0 {
		return nil
	}
	out := make([]flowRefDoc, len(refs))
	for i, r := range refs {
		out[i] = flowRefDoc{AID: r.ActivityID, C: r.Collection, EID: r.EntityID, V: r.Version, Ev: r.EventID}
	}
	return out
}

func fromFlowRefDocs(docs []flowRefDoc) []domain.FlowRef {
	if len(docs) == 0 {
		return nil
	}
	out := make([]domain.FlowRef, len(docs))
	for i, d := range docs {
		out[i] = domain.FlowRef{ActivityID: d.AID, Collection: d.C, EntityID: d.EID, Version: d.V, EventID: d.Ev}
	}
	return out
}

type intgDoc struct {
	H  string `bson:"h"`
	PH string `bson:"ph,omitempty"`
}

var (
	opToCode = map[diff.Op]string{diff.OpAdd: "a", diff.OpChange: "c", diff.OpRemove: "r"}
	codeToOp = map[string]diff.Op{"a": diff.OpAdd, "c": diff.OpChange, "r": diff.OpRemove}
)

func toEventDoc(ev *domain.Event) eventDoc {
	d := eventDoc{
		EID:      ev.EntityID,
		V:        ev.Version,
		Op:       string(ev.Op),
		Author:   ev.Author,
		Src:      ev.Source,
		TSSource: ev.TSSource,
		TSRcv:    ev.TSReceived,
		TSStored: ev.TSStored,
		EventID:  ev.EventID,
		RID:      ev.RequestID,
		Meta:     ev.Meta,
	}
	d.Changes = make([]changeDoc, len(ev.Changes))
	for i, c := range ev.Changes {
		d.Changes[i] = changeDoc{P: c.Path, O: opToCode[c.Op], Old: c.Old, New: c.New}
	}
	if ev.Flow != nil {
		d.Flow = &flowDoc{F: ev.Flow.ID, Step: ev.Flow.Step, By: toFlowRefDocs(ev.Flow.CausedBy)}
	}
	if ev.Integrity != nil {
		d.Intg = &intgDoc{H: ev.Integrity.Hash, PH: ev.Integrity.PrevHash}
	}
	return d
}

func fromEventDoc(d eventDoc) (*domain.Event, error) {
	ev := &domain.Event{
		EntityID:   d.EID,
		Version:    d.V,
		Op:         domain.EntityOp(d.Op),
		Author:     d.Author,
		Source:     d.Src,
		TSSource:   d.TSSource,
		TSReceived: d.TSRcv,
		TSStored:   d.TSStored,
		EventID:    d.EventID,
		RequestID:  d.RID,
		Meta:       d.Meta,
	}
	ev.Changes = make([]diff.Change, len(d.Changes))
	for i, c := range d.Changes {
		op, ok := codeToOp[c.O]
		if !ok {
			return nil, fmt.Errorf("mongo: unknown change op code %q at %q", c.O, c.P)
		}
		// Old/New carry arbitrary JSON; canonicalize so reconstruction and JSON
		// Patch export see neutral types, not bson.D/bson.A.
		ev.Changes[i] = diff.Change{Path: c.P, Op: op, Old: canonicalJSON(c.Old), New: canonicalJSON(c.New)}
	}
	if d.Flow != nil {
		ev.Flow = &domain.Flow{ID: d.Flow.F, Step: d.Flow.Step, CausedBy: fromFlowRefDocs(d.Flow.By)}
	}
	if d.Intg != nil {
		ev.Integrity = &domain.Integrity{Hash: d.Intg.H, PrevHash: d.Intg.PH}
	}
	return ev, nil
}

type currentDoc struct {
	ID      string         `bson:"_id"`
	V       int64          `bson:"v"`
	TS      time.Time      `bson:"ts"`
	Deleted bool           `bson:"deleted"`
	State   map[string]any `bson:"state"`
}

func toCurrentDoc(st *domain.CurrentState) currentDoc {
	return currentDoc{ID: st.EntityID, V: st.Version, TS: st.TS, Deleted: st.Deleted, State: st.State}
}

func fromCurrentDoc(d currentDoc) *domain.CurrentState {
	return &domain.CurrentState{EntityID: d.ID, Version: d.V, TS: d.TS, Deleted: d.Deleted, State: canonicalState(d.State)}
}

// snapDoc is one sn_* document. Unlike currentDoc the _id is driver-assigned:
// an entity has many snapshots keyed by {eid,v}, so version cannot serve as _id.
type snapDoc struct {
	ID      bson.ObjectID  `bson:"_id,omitempty"`
	EID     string         `bson:"eid"`
	V       int64          `bson:"v"`
	TS      time.Time      `bson:"ts"`
	Deleted bool           `bson:"deleted"`
	State   map[string]any `bson:"state"`
}

func toSnapDoc(snap *domain.Snapshot) snapDoc {
	return snapDoc{EID: snap.EntityID, V: snap.Version, TS: snap.TS, Deleted: snap.Deleted, State: snap.State}
}

func fromSnapDoc(d snapDoc) *domain.Snapshot {
	return &domain.Snapshot{EntityID: d.EID, Version: d.V, TS: d.TS, Deleted: d.Deleted, State: canonicalState(d.State)}
}

// canonicalJSON converts a BSON-decoded `any` to neutral JSON types
// (map[string]any, []any, scalars). The BSON driver returns bson.D/bson.A for
// objects/arrays; those named types fail diff.Apply, so they must be normalized
// before any diff or reconstruction consumer touches the value.
func canonicalJSON(v any) any {
	switch t := v.(type) {
	case bson.D:
		m := make(map[string]any, len(t))
		for _, e := range t {
			m[e.Key] = canonicalJSON(e.Value)
		}
		return m
	case bson.M:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[k] = canonicalJSON(val)
		}
		return m
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, val := range t {
			m[k] = canonicalJSON(val)
		}
		return m
	case bson.A:
		a := make([]any, len(t))
		for i, val := range t {
			a[i] = canonicalJSON(val)
		}
		return a
	case []any:
		a := make([]any, len(t))
		for i, val := range t {
			a[i] = canonicalJSON(val)
		}
		return a
	default:
		return v
	}
}

// canonicalState normalizes a decoded state document, preserving a nil map (an
// entity with no state, e.g. after delete) as nil rather than an empty map.
func canonicalState(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out, _ := canonicalJSON(m).(map[string]any)
	return out
}

// activityDoc is the ev__flow document. aid is always set (server- or client-
// supplied); flow is the flow id.
type activityDoc struct {
	ID       bson.ObjectID  `bson:"_id,omitempty"`
	AID      string         `bson:"aid"`
	Type     string         `bson:"type,omitempty"`
	Flow     string         `bson:"flow"`
	By       []flowRefDoc   `bson:"by,omitempty"`
	Refs     []flowRefDoc   `bson:"refs,omitempty"`
	Author   string         `bson:"author,omitempty"`
	Src      string         `bson:"src,omitempty"`
	TSSource time.Time      `bson:"ts_src,omitempty"`
	TSRcv    time.Time      `bson:"ts_rcv"`
	TSStored time.Time      `bson:"ts_st"`
	Data     map[string]any `bson:"data,omitempty"`
	Meta     map[string]any `bson:"meta,omitempty"`
}

func toActivityDoc(a *domain.Activity) activityDoc {
	return activityDoc{
		AID:      a.ActivityID,
		Type:     a.Type,
		Flow:     a.FlowID,
		By:       toFlowRefDocs(a.CausedBy),
		Refs:     toFlowRefDocs(a.Refs),
		Author:   a.Author,
		Src:      a.Source,
		TSSource: a.TSSource,
		TSRcv:    a.TSReceived,
		TSStored: a.TSStored,
		Data:     a.Data,
		Meta:     a.Meta,
	}
}

func fromActivityDoc(d activityDoc) domain.Activity {
	return domain.Activity{
		ActivityID: d.AID,
		Type:       d.Type,
		FlowID:     d.Flow,
		CausedBy:   fromFlowRefDocs(d.By),
		Refs:       fromFlowRefDocs(d.Refs),
		Author:     d.Author,
		Source:     d.Src,
		TSSource:   d.TSSource,
		TSReceived: d.TSRcv,
		TSStored:   d.TSStored,
		Data:       d.Data,
		Meta:       d.Meta,
	}
}
