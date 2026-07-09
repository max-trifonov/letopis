package domain

import (
	"testing"
	"time"
)

func ts(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

func act(id string, sec int) Activity {
	return Activity{ActivityID: id, TSReceived: ts(sec)}
}

func flowEvent(coll, eid string, v int64, sec int) FlowEvent {
	return FlowEvent{Collection: coll, Event: Event{EntityID: eid, Version: v, TSReceived: ts(sec)}}
}

func TestAssembleFlowMergesInReceivedOrder(t *testing.T) {
	activities := []Activity{act("act-2", 20), act("act-4", 40)}
	events := []FlowEvent{flowEvent("crm.deals", "d-1", 1, 10), flowEvent("crm.deals", "d-1", 2, 30)}

	nodes, next := AssembleFlow(activities, events, nil, 0)
	if next != nil {
		t.Fatalf("unbounded page must not return a cursor: %+v", next)
	}
	wantTS := []int{10, 20, 30, 40}
	if len(nodes) != len(wantTS) {
		t.Fatalf("got %d nodes, want %d", len(nodes), len(wantTS))
	}
	for i, sec := range wantTS {
		if !nodes[i].TS().Equal(ts(sec)) {
			t.Fatalf("node %d ts = %v, want %v", i, nodes[i].TS(), ts(sec))
		}
	}
	if nodes[0].Kind != FlowNodeEvent || nodes[1].Kind != FlowNodeActivity {
		t.Fatalf("kinds interleaved wrong: %v, %v", nodes[0].Kind, nodes[1].Kind)
	}
}

// When two nodes share a received time, the nodeID tie-breaker must impose a
// stable, deterministic order so paging neither drops nor duplicates a node.
func TestAssembleFlowStableTieBreak(t *testing.T) {
	activities := []Activity{act("act-z", 10)}
	events := []FlowEvent{flowEvent("crm.deals", "d-1", 7, 10)}

	nodes, _ := AssembleFlow(activities, events, nil, 0)
	// "a|act-z" sorts before "e|crm.deals|d-1|7" lexicographically.
	if nodes[0].Kind != FlowNodeActivity || nodes[1].Kind != FlowNodeEvent {
		t.Fatalf("tie-break unstable: %v, %v", nodes[0].Kind, nodes[1].Kind)
	}
}

func TestAssembleFlowPaginates(t *testing.T) {
	activities := []Activity{act("act-1", 10), act("act-3", 30)}
	events := []FlowEvent{flowEvent("crm.deals", "d-1", 1, 20), flowEvent("crm.deals", "d-1", 2, 40)}

	page1, next := AssembleFlow(activities, events, nil, 2)
	if next == nil {
		t.Fatal("first page of 2/4 must return a cursor")
	}
	if len(page1) != 2 || !page1[0].TS().Equal(ts(10)) || !page1[1].TS().Equal(ts(20)) {
		t.Fatalf("page1 wrong: %+v", page1)
	}

	page2, next2 := AssembleFlow(activities, events, next, 2)
	if next2 != nil {
		t.Fatalf("final page must not return a cursor: %+v", next2)
	}
	if len(page2) != 2 || !page2[0].TS().Equal(ts(30)) || !page2[1].TS().Equal(ts(40)) {
		t.Fatalf("page2 wrong: %+v", page2)
	}
}

func TestAssembleFlowEmpty(t *testing.T) {
	nodes, next := AssembleFlow(nil, nil, nil, 10)
	if len(nodes) != 0 || next != nil {
		t.Fatalf("empty flow must yield no nodes and no cursor: %+v / %+v", nodes, next)
	}
}
