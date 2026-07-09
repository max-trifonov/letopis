package domain

import (
	"sort"
	"strconv"
	"time"
)

// Activity is a business event that is not a change to a tracked entity —
// a recalculation, a notification, an external job. Letopis records it as an
// opaque node of a flow. Activities live in ev__flow, outside any entity's hash-chain.
type Activity struct {
	ActivityID string
	Type       string
	FlowID     string
	CausedBy   []FlowRef
	Refs       []FlowRef
	Author     string
	Source     string
	TSSource   time.Time
	TSReceived time.Time
	TSStored   time.Time
	Data       map[string]any
	Meta       map[string]any
}

// FlowEvent pairs a change event with the logical collection it came from.
// The flow fan-out sweeps many ev_* collections, so the collection is not
// implied by context the way it is on a single-collection read.
type FlowEvent struct {
	Collection string
	Event      Event
}

// FlowNodeKind tags a flow node as an activity or a change event.
type FlowNodeKind string

const (
	FlowNodeEvent    FlowNodeKind = "event"
	FlowNodeActivity FlowNodeKind = "activity"
)

// FlowNode is one node of an assembled flow: an activity or a change event,
// merged into a single chronological sequence. Exactly one of Event / Activity
// is set, per Kind.
type FlowNode struct {
	Kind       FlowNodeKind
	Collection string    // set for event nodes
	Event      *Event    // set when Kind == FlowNodeEvent
	Activity   *Activity // set when Kind == FlowNodeActivity
}

// TS is the node's received time — the primary key of its pagination cursor.
func (n FlowNode) TS() time.Time {
	if n.Kind == FlowNodeActivity {
		return n.Activity.TSReceived
	}
	return n.Event.TSReceived
}

// nodeID is a stable secondary sort/cursor key, breaking ties when two nodes
// share a received time. Activities key on their id; events on the
// (collection, entity, version) triple, which is unique within a tenant.
func (n FlowNode) nodeID() string {
	if n.Kind == FlowNodeActivity {
		return "a|" + n.Activity.ActivityID
	}
	return "e|" + n.Collection + "|" + n.Event.EntityID + "|" + strconv.FormatInt(n.Event.Version, 10)
}

// FlowPosition is a decoded flow-pagination cursor: the (received-time, node)
// key of the last node on the previous page.
type FlowPosition struct {
	TS time.Time
	ID string
}

// after reports whether node n sorts strictly after position p.
func (n FlowNode) after(p FlowPosition) bool {
	t := n.TS()
	if t.After(p.TS) {
		return true
	}
	if t.Before(p.TS) {
		return false
	}
	return n.nodeID() > p.ID
}

func (n FlowNode) position() FlowPosition {
	return FlowPosition{TS: n.TS(), ID: n.nodeID()}
}

// AssembleFlow merges a flow's activities and change events into one chronological page.
// Nodes are ordered by received time with a stable per-node tie-breaker so paging
// neither drops nor duplicates a node when timestamps collide. after bounds the page
// to nodes past the cursor (nil = first page); limit caps its size (0 = no limit).
// The returned position is the cursor for the next page, or nil when this is the last.
func AssembleFlow(activities []Activity, events []FlowEvent, after *FlowPosition, limit int) ([]FlowNode, *FlowPosition) {
	nodes := make([]FlowNode, 0, len(activities)+len(events))
	for i := range activities {
		nodes = append(nodes, FlowNode{Kind: FlowNodeActivity, Activity: &activities[i]})
	}
	for i := range events {
		nodes = append(nodes, FlowNode{Kind: FlowNodeEvent, Collection: events[i].Collection, Event: &events[i].Event})
	}

	sort.Slice(nodes, func(i, j int) bool {
		ti, tj := nodes[i].TS(), nodes[j].TS()
		if !ti.Equal(tj) {
			return ti.Before(tj)
		}
		return nodes[i].nodeID() < nodes[j].nodeID()
	})

	if after != nil {
		start := 0
		for start < len(nodes) && !nodes[start].after(*after) {
			start++
		}
		nodes = nodes[start:]
	}

	if limit > 0 && len(nodes) > limit {
		last := nodes[limit-1].position()
		return nodes[:limit], &last
	}
	return nodes, nil
}
