package service

import (
	"context"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// fakeFlowStore is an in-memory FlowStore: it records appended activities and
// serves canned events, so the use-case can be tested without Mongo.
type fakeFlowStore struct {
	activities []domain.Activity
	events     []domain.FlowEvent
}

func (f *fakeFlowStore) AppendActivity(_ context.Context, a *domain.Activity) error {
	a.TSStored = time.Now().UTC()
	f.activities = append(f.activities, *a)
	return nil
}

func (f *fakeFlowStore) FlowActivities(_ context.Context, flowID string) ([]domain.Activity, error) {
	var out []domain.Activity
	for _, a := range f.activities {
		if a.FlowID == flowID {
			out = append(out, a)
		}
	}
	return out, nil
}

func (f *fakeFlowStore) FlowEvents(_ context.Context, flowID string) ([]domain.FlowEvent, error) {
	var out []domain.FlowEvent
	for _, e := range f.events {
		if e.Event.Flow != nil && e.Event.Flow.ID == flowID {
			out = append(out, e)
		}
	}
	return out, nil
}

func TestRecordMintsIDsWhenAbsent(t *testing.T) {
	store := &fakeFlowStore{}
	svc := NewActivities(store)

	res, err := svc.Record(context.Background(), RecordActivityCommand{Type: "recalc.prices"})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if res.ActivityID == "" || res.FlowID == "" {
		t.Fatalf("ids not minted: %+v", res)
	}
	if len(store.activities) != 1 || store.activities[0].TSReceived.IsZero() {
		t.Fatalf("activity not persisted with received time: %+v", store.activities)
	}
}

func TestRecordKeepsSuppliedIDs(t *testing.T) {
	store := &fakeFlowStore{}
	svc := NewActivities(store)

	res, err := svc.Record(context.Background(), RecordActivityCommand{ActivityID: "act-7", FlowID: "f-1"})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}
	if res.ActivityID != "act-7" || res.FlowID != "f-1" {
		t.Fatalf("supplied ids overwritten: %+v", res)
	}
}

func TestFlowMergesActivitiesAndEvents(t *testing.T) {
	store := &fakeFlowStore{
		activities: []domain.Activity{
			{ActivityID: "act-1", FlowID: "f-1", TSReceived: time.Unix(20, 0).UTC()},
			{ActivityID: "act-2", FlowID: "other", TSReceived: time.Unix(99, 0).UTC()},
		},
		events: []domain.FlowEvent{
			{Collection: "crm.deals", Event: domain.Event{EntityID: "d-1", Version: 3, TSReceived: time.Unix(10, 0).UTC(), Flow: &domain.Flow{ID: "f-1"}}},
		},
	}
	svc := NewActivities(store)

	res, err := svc.Flow(context.Background(), "f-1", nil, 0)
	if err != nil {
		t.Fatalf("Flow: %v", err)
	}
	if len(res.Nodes) != 2 {
		t.Fatalf("got %d nodes, want 2 (other flow leaked?)", len(res.Nodes))
	}
	if res.Nodes[0].Kind != domain.FlowNodeEvent || res.Nodes[1].Kind != domain.FlowNodeActivity {
		t.Fatalf("nodes not in received order: %+v", res.Nodes)
	}
}
