package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/service"
)

// fakeRead implements ReadService, recording the filter/query it was given (so
// the mapping can be asserted) and returning canned events/state.
type fakeRead struct {
	events     []domain.Event
	state      *domain.CurrentState
	stateErr   error
	lastFilter domain.EventFilter

	reconstructed *service.ReconstructedState
	stateAtErr    error
	lastQuery     service.PointInTimeQuery
}

func (f *fakeRead) History(_ context.Context, _ string, fl domain.EventFilter) ([]domain.Event, error) {
	f.lastFilter = fl
	return f.events, nil
}

func (f *fakeRead) CurrentState(_ context.Context, _, _ string) (*domain.CurrentState, error) {
	return f.state, f.stateErr
}

func (f *fakeRead) StateAt(_ context.Context, _, _ string, q service.PointInTimeQuery) (*service.ReconstructedState, error) {
	f.lastQuery = q
	return f.reconstructed, f.stateAtErr
}

func readRouter(t *testing.T, svc ReadService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), testResolver(t), nil, svc, nil, nil, nil, nil, nil, nil, nil)
}

const historyPath = "/api/v1/collections/crm.deals/entities/d-1/history"

func sampleEvents() []domain.Event {
	return []domain.Event{
		{
			EntityID: "d-1", Version: 2, Op: domain.OpUpdate, Author: "42", TSReceived: time.Unix(200, 0).UTC(),
			Changes: []diff.Change{{Path: "amount", Op: diff.OpChange, Old: float64(100), New: float64(250)}},
		},
		{
			EntityID: "d-1", Version: 1, Op: domain.OpCreate, TSReceived: time.Unix(100, 0).UTC(),
			Changes: []diff.Change{{Path: "title", Op: diff.OpAdd, New: "deal"}},
		},
	}
}

func TestHistoryNativeFormat(t *testing.T) {
	svc := &fakeRead{events: sampleEvents()}
	requireAuthGet(t, readRouter(t, svc), historyPath) // route sits behind auth

	rec := doGet(t, readRouter(t, svc), historyPath, "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		EntityID   string           `json:"entity_id"`
		Events     []map[string]any `json:"events"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.EntityID != "d-1" || len(body.Events) != 2 {
		t.Fatalf("body wrong: %s", rec.Body.String())
	}
	ch := body.Events[0]["changes"].([]any)[0].(map[string]any)
	if ch["path"] != "amount" || ch["op"] != "change" || ch["old"].(float64) != 100 {
		t.Fatalf("native change shape wrong: %v", ch)
	}
	if body.NextCursor != nil {
		t.Fatalf("next_cursor should be nil for a short page: %v", *body.NextCursor)
	}
}

func TestHistoryExposesIntegrity(t *testing.T) {
	evs := sampleEvents()
	evs[0].Integrity = &domain.Integrity{Hash: "sha256:aaa", PrevHash: "sha256:bbb"}
	svc := &fakeRead{events: evs}

	rec := doGet(t, readRouter(t, svc), historyPath, "Bearer key-a")
	var body struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	intg, ok := body.Events[0]["integrity"].(map[string]any)
	if !ok || intg["hash"] != "sha256:aaa" || intg["prev_hash"] != "sha256:bbb" {
		t.Fatalf("integrity block missing/wrong: %v", body.Events[0]["integrity"])
	}
	// An event without a link must not carry the block.
	if _, present := body.Events[1]["integrity"]; present {
		t.Fatalf("event without integrity should omit the block: %v", body.Events[1])
	}
}

func TestHistoryJSONPatchFormat(t *testing.T) {
	svc := &fakeRead{events: sampleEvents()}
	rec := doGet(t, readRouter(t, svc), historyPath+"?format=json-patch", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Events []map[string]any `json:"events"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	op := body.Events[0]["changes"].([]any)[0].(map[string]any)
	// RFC 6902: change → replace, JSON Pointer path, no "old".
	if op["op"] != "replace" || op["path"] != "/amount" {
		t.Fatalf("json-patch op wrong: %v", op)
	}
	if _, hasOld := op["old"]; hasOld {
		t.Fatalf("json-patch must not carry old: %v", op)
	}
}

func TestHistoryNextCursorWhenPageFull(t *testing.T) {
	svc := &fakeRead{events: sampleEvents()} // 2 events
	rec := doGet(t, readRouter(t, svc), historyPath+"?limit=2", "Bearer key-a")
	var body struct {
		NextCursor *string `json:"next_cursor"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.NextCursor == nil {
		t.Fatal("a full page must yield a next_cursor")
	}
	// Feed the cursor back: it must decode into filter.After (version of the
	// last event, 1, since default order is desc by version).
	rec2 := doGet(t, readRouter(t, svc), historyPath+"?limit=2&cursor="+*body.NextCursor, "Bearer key-a")
	if rec2.Code != http.StatusOK {
		t.Fatalf("paged request status = %d", rec2.Code)
	}
	if svc.lastFilter.After == nil || svc.lastFilter.After.Version != 1 {
		t.Fatalf("cursor did not round-trip into filter.After: %+v", svc.lastFilter.After)
	}
}

func TestHistoryFilterMapping(t *testing.T) {
	svc := &fakeRead{}
	url := historyPath + "?op=update&author_id=42&source=crm&path=amount&order_by=ts_source&order=asc&from=2026-06-01T00:00:00Z&to=2026-06-30T00:00:00Z&limit=50"
	rec := doGet(t, readRouter(t, svc), url, "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	f := svc.lastFilter
	if f.Op != domain.OpUpdate || f.Author != "42" || f.Source != "crm" || f.Path != "amount" {
		t.Fatalf("filter fields wrong: %+v", f)
	}
	if f.OrderBy != domain.OrderSource || f.Order != domain.OrderAsc || f.Limit != 50 {
		t.Fatalf("ordering/limit wrong: %+v", f)
	}
	if f.From.IsZero() || f.To.IsZero() {
		t.Fatalf("time window not parsed: %+v", f)
	}
}

func TestHistoryDefaults(t *testing.T) {
	svc := &fakeRead{}
	doGet(t, readRouter(t, svc), historyPath, "Bearer key-a")
	if svc.lastFilter.Order != domain.OrderDesc || svc.lastFilter.Limit != defaultHistoryLimit {
		t.Fatalf("defaults wrong: order=%q limit=%d", svc.lastFilter.Order, svc.lastFilter.Limit)
	}
}

func TestHistoryLimitCapped(t *testing.T) {
	svc := &fakeRead{}
	doGet(t, readRouter(t, svc), historyPath+"?limit=99999", "Bearer key-a")
	if svc.lastFilter.Limit != maxHistoryLimit {
		t.Fatalf("limit = %d, want capped to %d", svc.lastFilter.Limit, maxHistoryLimit)
	}
}

func TestHistoryBadParams(t *testing.T) {
	svc := &fakeRead{}
	for _, q := range []string{"?order_by=nope", "?order=sideways", "?format=xml", "?limit=-3", "?limit=abc", "?from=not-a-date", "?cursor=@@@"} {
		rec := doGet(t, readRouter(t, svc), historyPath+q, "Bearer key-a")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("query %q: status = %d, want 400", q, rec.Code)
		}
	}
}

func TestHistoryReadScopeRequired(t *testing.T) {
	// key-a has read+write and crm.* — allowed. A docs.* read with key-a is 403.
	rec := doGet(t, readRouter(t, &fakeRead{}), "/api/v1/collections/docs.x/entities/d-1/history", "Bearer key-a")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestCurrentStateOK(t *testing.T) {
	svc := &fakeRead{state: &domain.CurrentState{EntityID: "d-1", Version: 12, TS: time.Unix(300, 0).UTC(), State: map[string]any{"amount": float64(250)}}}
	rec := doGet(t, readRouter(t, svc), "/api/v1/collections/crm.deals/entities/d-1/state", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		Version int64          `json:"version"`
		Deleted bool           `json:"deleted"`
		State   map[string]any `json:"state"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Version != 12 || body.State["amount"].(float64) != 250 {
		t.Fatalf("state body wrong: %s", rec.Body.String())
	}
}

func TestCurrentStateNotFound(t *testing.T) {
	svc := &fakeRead{stateErr: domain.ErrNotFound}
	rec := doGet(t, readRouter(t, svc), "/api/v1/collections/crm.deals/entities/ghost/state", "Bearer key-a")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestStateAtByVersion(t *testing.T) {
	svc := &fakeRead{reconstructed: &service.ReconstructedState{
		EntityID: "d-1", Version: 12, TS: time.Unix(300, 0).UTC(), Deleted: false,
		State: map[string]any{"amount": float64(250)}, SnapshotVersion: 10, EventsApplied: 2,
	}}
	rec := doGet(t, readRouter(t, svc), statePath+"?version=12", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if svc.lastQuery.Version == nil || *svc.lastQuery.Version != 12 || svc.lastQuery.At != nil {
		t.Fatalf("query not mapped to version: %+v", svc.lastQuery)
	}
	var body struct {
		Version           int64          `json:"version"`
		State             map[string]any `json:"state"`
		ReconstructedFrom map[string]any `json:"reconstructed_from"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Version != 12 || body.State["amount"].(float64) != 250 {
		t.Fatalf("body wrong: %s", rec.Body.String())
	}
	if body.ReconstructedFrom["snapshot_version"].(float64) != 10 || body.ReconstructedFrom["events_applied"].(float64) != 2 {
		t.Fatalf("reconstructed_from wrong: %v", body.ReconstructedFrom)
	}
}

func TestStateAtByTime(t *testing.T) {
	svc := &fakeRead{reconstructed: &service.ReconstructedState{EntityID: "d-1", Version: 5, State: map[string]any{}}}
	rec := doGet(t, readRouter(t, svc), statePath+"?at=2026-06-01T00:00:00Z", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if svc.lastQuery.At == nil || svc.lastQuery.Version != nil {
		t.Fatalf("query not mapped to at: %+v", svc.lastQuery)
	}
}

// Genesis reconstruction (no snapshot) reports snapshot_version: null.
func TestStateAtGenesisNullSnapshot(t *testing.T) {
	svc := &fakeRead{reconstructed: &service.ReconstructedState{EntityID: "d-1", Version: 3, State: map[string]any{}, SnapshotVersion: 0, EventsApplied: 3}}
	rec := doGet(t, readRouter(t, svc), statePath+"?version=3", "Bearer key-a")
	var body struct {
		ReconstructedFrom map[string]any `json:"reconstructed_from"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if got, ok := body.ReconstructedFrom["snapshot_version"]; !ok || got != nil {
		t.Fatalf("snapshot_version should be null for genesis: %v", body.ReconstructedFrom)
	}
}

func TestStateAtBadParams(t *testing.T) {
	svc := &fakeRead{reconstructed: &service.ReconstructedState{}}
	cases := map[string]int{
		"?version=12&at=2026-06-01T00:00:00Z": http.StatusBadRequest, // mutually exclusive
		"?at_source=2026-06-01T00:00:00Z":     http.StatusBadRequest, // reserved
		"?version=0":                          http.StatusBadRequest, // must be ≥1
		"?version=-2":                         http.StatusBadRequest,
		"?version=abc":                        http.StatusBadRequest,
		"?at=not-a-date":                      http.StatusBadRequest,
	}
	for q, want := range cases {
		rec := doGet(t, readRouter(t, svc), statePath+q, "Bearer key-a")
		if rec.Code != want {
			t.Fatalf("query %q: status = %d, want %d", q, rec.Code, want)
		}
	}
}

func TestStateAtNotFound(t *testing.T) {
	svc := &fakeRead{stateAtErr: domain.ErrNotFound}
	rec := doGet(t, readRouter(t, svc), statePath+"?version=99", "Bearer key-a")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// No point-in-time params keeps the current-state branch (S1-06).
func TestStateNoParamsServesCurrent(t *testing.T) {
	svc := &fakeRead{state: &domain.CurrentState{EntityID: "d-1", Version: 7, State: map[string]any{"amount": float64(1)}}}
	rec := doGet(t, readRouter(t, svc), statePath, "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if _, ok := body["reconstructed_from"]; ok {
		t.Fatalf("current-state read must not carry reconstructed_from: %s", rec.Body.String())
	}
}

// doGet issues an authenticated GET.
func doGet(t *testing.T, h http.Handler, path, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func requireAuthGet(t *testing.T, h http.Handler, path string) {
	t.Helper()
	rec := get(t, h, path) // no auth header
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated GET %s = %d, want 401", path, rec.Code)
	}
}
