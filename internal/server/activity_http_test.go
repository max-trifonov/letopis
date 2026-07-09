package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/service"
)

// fakeActivity implements ActivityService, recording the command/flow request
// so the transport mapping can be asserted, and returning canned results.
type fakeActivity struct {
	recordRes  service.RecordActivityResult
	flowRes    service.FlowResult
	lastCmd    service.RecordActivityCommand
	lastFlowID string
	lastAfter  *domain.FlowPosition
	lastLimit  int
}

func (f *fakeActivity) Record(_ context.Context, cmd service.RecordActivityCommand) (service.RecordActivityResult, error) {
	f.lastCmd = cmd
	return f.recordRes, nil
}

func (f *fakeActivity) Flow(_ context.Context, flowID string, after *domain.FlowPosition, limit int) (service.FlowResult, error) {
	f.lastFlowID, f.lastAfter, f.lastLimit = flowID, after, limit
	return f.flowRes, nil
}

func activityRouter(t *testing.T, svc ActivityService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), testResolver(t), nil, nil, svc, nil, nil, nil, nil, nil, nil)
}

const activitiesPath = "/api/v1/activities"

func TestActivityRecordReturnsIDs(t *testing.T) {
	svc := &fakeActivity{recordRes: service.RecordActivityResult{ActivityID: "act_X", FlowID: "f_Y"}}
	body := `{"type":"recalc.prices","author_id":"42","caused_by":[{"collection":"crm.deals","entity_id":"d-1","event_id":"src-981"}]}`
	rec := post(t, activityRouter(t, svc), activitiesPath, "Bearer key-a", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var got struct {
		ActivityID string `json:"activity_id"`
		FlowID     string `json:"flow_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.ActivityID != "act_X" || got.FlowID != "f_Y" {
		t.Fatalf("response ids wrong: %s", rec.Body.String())
	}
	if svc.lastCmd.Type != "recalc.prices" || len(svc.lastCmd.CausedBy) != 1 {
		t.Fatalf("command mapping wrong: %+v", svc.lastCmd)
	}
	if svc.lastCmd.CausedBy[0].EventID != "src-981" || svc.lastCmd.CausedBy[0].Collection != "crm.deals" {
		t.Fatalf("caused_by ref not mapped: %+v", svc.lastCmd.CausedBy[0])
	}
}

func TestActivityRecordRejectsEmptyRef(t *testing.T) {
	svc := &fakeActivity{}
	rec := post(t, activityRouter(t, svc), activitiesPath, "Bearer key-a", `{"caused_by":[{}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (empty ref)", rec.Code)
	}
}

// key-b has only the read scope; recording an activity must be 403 on scope.
func TestActivityRecordInsufficientScope(t *testing.T) {
	rec := post(t, activityRouter(t, &fakeActivity{}), activitiesPath, "Bearer key-b", `{"type":"x"}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestActivityRecordUnauthenticated(t *testing.T) {
	rec := post(t, activityRouter(t, &fakeActivity{}), activitiesPath, "", `{"type":"x"}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestFlowAssemblesNodes(t *testing.T) {
	svc := &fakeActivity{flowRes: service.FlowResult{
		FlowID: "f-1",
		Nodes: []domain.FlowNode{
			{Kind: domain.FlowNodeEvent, Collection: "crm.deals", Event: &domain.Event{
				EntityID: "d-1", Version: 17, Op: domain.OpUpdate, TSReceived: time.Unix(10, 0).UTC(),
				Flow: &domain.Flow{ID: "f-1", Step: "deal-approved"},
			}},
			{Kind: domain.FlowNodeActivity, Activity: &domain.Activity{
				ActivityID: "act_1", Type: "recalc.prices", TSReceived: time.Unix(20, 0).UTC(),
				CausedBy: []domain.FlowRef{{Collection: "crm.deals", EntityID: "d-1", Version: 17}},
			}},
		},
	}}
	rec := doGet(t, activityRouter(t, svc), "/api/v1/flows/f-1", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		FlowID     string           `json:"flow_id"`
		Nodes      []map[string]any `json:"nodes"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.FlowID != "f-1" || len(body.Nodes) != 2 {
		t.Fatalf("flow body wrong: %s", rec.Body.String())
	}
	if body.Nodes[0]["kind"] != "event" || body.Nodes[0]["step"] != "deal-approved" {
		t.Fatalf("event node shape wrong: %v", body.Nodes[0])
	}
	if body.Nodes[1]["kind"] != "activity" || body.Nodes[1]["type"] != "recalc.prices" {
		t.Fatalf("activity node shape wrong: %v", body.Nodes[1])
	}
	cb := body.Nodes[1]["caused_by"].([]any)
	if len(cb) != 1 || cb[0].(map[string]any)["version"].(float64) != 17 {
		t.Fatalf("activity caused_by not rendered: %v", body.Nodes[1]["caused_by"])
	}
	if body.NextCursor != nil {
		t.Fatalf("short page must have null next_cursor: %v", *body.NextCursor)
	}
}

func TestFlowEmitsNextCursor(t *testing.T) {
	svc := &fakeActivity{flowRes: service.FlowResult{
		FlowID: "f-1",
		Nodes:  []domain.FlowNode{{Kind: domain.FlowNodeActivity, Activity: &domain.Activity{ActivityID: "act_1", TSReceived: time.Unix(10, 0).UTC()}}},
		Next:   &domain.FlowPosition{TS: time.Unix(10, 0).UTC(), ID: "a|act_1"},
	}}
	rec := doGet(t, activityRouter(t, svc), "/api/v1/flows/f-1?limit=1", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		NextCursor *string `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.NextCursor == nil {
		t.Fatal("full page must emit a next_cursor")
	}
	if svc.lastLimit != 1 {
		t.Fatalf("limit not passed through: %d", svc.lastLimit)
	}
	// The emitted cursor must decode back to the service-supplied position.
	pos, err := decodeFlowCursor(*body.NextCursor)
	if err != nil || pos.ID != "a|act_1" {
		t.Fatalf("cursor round-trip failed: %v / %+v", err, pos)
	}
}

func TestFlowPassesCursor(t *testing.T) {
	svc := &fakeActivity{}
	cursor := encodeFlowCursor(domain.FlowPosition{TS: time.Unix(15, 0).UTC(), ID: "a|act_1"})
	rec := doGet(t, activityRouter(t, svc), "/api/v1/flows/f-1?cursor="+cursor, "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if svc.lastAfter == nil || svc.lastAfter.ID != "a|act_1" {
		t.Fatalf("cursor not decoded into filter: %+v", svc.lastAfter)
	}
}

func TestFlowRejectsBadCursor(t *testing.T) {
	rec := doGet(t, activityRouter(t, &fakeActivity{}), "/api/v1/flows/f-1?cursor=!!!notbase64", "Bearer key-a")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}
