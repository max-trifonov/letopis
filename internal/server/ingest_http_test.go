package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/service"
)

// fakeIngest implements IngestService, recording the command it received so a
// test can assert the transport→service mapping, and returning canned results
// to exercise the response codes.
type fakeIngest struct {
	cfg     *domain.CollectionConfig
	cfgErr  error
	result  service.IngestResult
	err     error
	lastCmd service.IngestCommand
}

func (f *fakeIngest) Config(_ context.Context, _ string) (*domain.CollectionConfig, error) {
	if f.cfgErr != nil {
		return nil, f.cfgErr
	}
	cfg := f.cfg
	if cfg == nil {
		cfg = &domain.CollectionConfig{MaxEventSizeBytes: domain.DefaultMaxEventSizeBytes}
	}
	return cfg, nil
}

func (f *fakeIngest) State(_ context.Context, cmd service.IngestCommand) (service.IngestResult, error) {
	f.lastCmd = cmd
	return f.result, f.err
}

func (f *fakeIngest) Diff(_ context.Context, cmd service.IngestCommand) (service.IngestResult, error) {
	f.lastCmd = cmd
	return f.result, f.err
}

func (f *fakeIngest) Delete(_ context.Context, cmd service.IngestCommand) (service.IngestResult, error) {
	f.lastCmd = cmd
	return f.result, f.err
}

func ingestRouter(t *testing.T, svc IngestService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), testResolver(t), svc, nil, nil, nil, nil, nil, nil, nil, nil)
}

func post(t *testing.T, h http.Handler, path, auth, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

const statePath = "/api/v1/collections/crm.deals/entities/d-1/state"

func TestIngestStateCreated(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Version: 17, ChangesCount: 3}}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"state":{"amount":250}}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		EntityID     string `json:"entity_id"`
		Version      int64  `json:"version"`
		ChangesCount int    `json:"changes_count"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Version != 17 || body.ChangesCount != 3 || body.EntityID != "d-1" {
		t.Fatalf("response body wrong: %s", rec.Body.String())
	}
}

func TestIngestStateMapsMetadata(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Version: 1}}
	exp := int64(4)
	body := `{"op":"update","author_id":"42","source":"crm","expected_version":4,"ts_source":"2026-06-11T10:00:00Z","state":{"amount":1}}`
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	got := svc.lastCmd
	if got.Collection != "crm.deals" || got.EntityID != "d-1" || got.Author != "42" || got.Source != "crm" {
		t.Fatalf("command mapping wrong: %+v", got)
	}
	if got.Op != domain.OpUpdate || got.ExpectedVersion == nil || *got.ExpectedVersion != exp {
		t.Fatalf("op/expected_version mapping wrong: %+v", got)
	}
	if got.TSSource.IsZero() || got.ClientIP == "" {
		t.Fatalf("ts_source/client ip not set: %+v", got)
	}
}

func TestIngestStateNoChanges(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Version: 1, NoChanges: true}}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"state":{"amount":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "no_changes") {
		t.Fatalf("body = %s, want no_changes", rec.Body.String())
	}
}

func TestIngestStateRequiresState(t *testing.T) {
	svc := &fakeIngest{}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"author_id":"42"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (state required)", rec.Code)
	}
}

func TestIngestInvalidJSON(t *testing.T) {
	rec := post(t, ingestRouter(t, &fakeIngest{}), statePath, "Bearer key-a", `{not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestIngestInvalidOp(t *testing.T) {
	rec := post(t, ingestRouter(t, &fakeIngest{}), statePath, "Bearer key-a", `{"op":"frobnicate","state":{}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestIngestVersionConflict(t *testing.T) {
	svc := &fakeIngest{err: domain.ErrVersionConflict}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"expected_version":1,"state":{}}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rec.Code)
	}
}

func TestIngestAutoCreateDisabled(t *testing.T) {
	svc := &fakeIngest{cfgErr: service.ErrAutoCreateDisabled}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"state":{}}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestIngestTooLarge(t *testing.T) {
	svc := &fakeIngest{cfg: &domain.CollectionConfig{MaxEventSizeBytes: 16}}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"state":{"amount":1234567890}}`)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

// key-a is scoped to crm.* — a write to docs.* must be 403, even authenticated.
func TestIngestCollectionForbidden(t *testing.T) {
	svc := &fakeIngest{}
	rec := post(t, ingestRouter(t, svc), "/api/v1/collections/docs.x/entities/d-1/state", "Bearer key-a", `{"state":{}}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

// key-b has only the read scope; a write must be 403 on scope alone.
func TestIngestInsufficientScope(t *testing.T) {
	svc := &fakeIngest{}
	rec := post(t, ingestRouter(t, svc), "/api/v1/collections/docs.deals/entities/d-1/state", "Bearer key-b", `{"state":{}}`)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestIngestUnauthenticated(t *testing.T) {
	rec := post(t, ingestRouter(t, &fakeIngest{}), statePath, "", `{"state":{}}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestIngestDiffRequiresChanges(t *testing.T) {
	svc := &fakeIngest{}
	rec := post(t, ingestRouter(t, svc), "/api/v1/collections/crm.deals/entities/d-1/diff", "Bearer key-a", `{"op":"update"}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (changes required)", rec.Code)
	}
}

func TestIngestDiffMapsChanges(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Version: 2, ChangesCount: 1}}
	body := `{"op":"update","changes":[{"path":"amount","op":"change","old":100,"new":250}]}`
	rec := post(t, ingestRouter(t, svc), "/api/v1/collections/crm.deals/entities/d-1/diff", "Bearer key-a", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	if len(svc.lastCmd.Changes) != 1 || svc.lastCmd.Changes[0].Path != "amount" {
		t.Fatalf("changes not mapped: %+v", svc.lastCmd.Changes)
	}
}

func TestIngestDiffRejectsBadChangeOp(t *testing.T) {
	svc := &fakeIngest{}
	body := `{"op":"update","changes":[{"path":"amount","op":"bogus"}]}`
	rec := post(t, ingestRouter(t, svc), "/api/v1/collections/crm.deals/entities/d-1/diff", "Bearer key-a", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestIngestDelete(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Version: 3}}
	rec := post(t, ingestRouter(t, svc), "/api/v1/collections/crm.deals/entities/d-1/delete", "Bearer key-a", `{"author_id":"42"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	if svc.lastCmd.Op != domain.OpDelete {
		t.Fatalf("delete op = %s, want delete", svc.lastCmd.Op)
	}
}

func TestIngestStateMapsFlowBlock(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Version: 1}}
	body := `{"state":{"amount":1},"flow":{"flow_id":"f-1","step":"approved","caused_by":[{"activity_id":"act-7"}]}}`
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", body)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	f := svc.lastCmd.Flow
	if f == nil || f.ID != "f-1" || f.Step != "approved" {
		t.Fatalf("flow block not mapped: %+v", f)
	}
	if len(f.CausedBy) != 1 || f.CausedBy[0].ActivityID != "act-7" {
		t.Fatalf("caused_by not mapped: %+v", f.CausedBy)
	}
}

func TestIngestStateRejectsEmptyFlowRef(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Version: 1}}
	body := `{"state":{"amount":1},"flow":{"caused_by":[{}]}}`
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", body)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (empty flow ref)", rec.Code)
	}
}

// postMode posts with an explicit X-Letopis-Mode header.
func postMode(t *testing.T, h http.Handler, path, auth, mode, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Authorization", auth)
	if mode != "" {
		req.Header.Set("X-Letopis-Mode", mode)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// An async accept (durable/fast) is 202 with a ticket id, not 201.
func TestIngestAsyncAccepted(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{Async: true, Ticket: "tkt_1"}}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"state":{"amount":1}}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	var body struct {
		TicketID string `json:"ticket_id"`
		Status   string `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.TicketID != "tkt_1" || body.Status != "accepted" {
		t.Fatalf("202 body wrong: %s", rec.Body.String())
	}
}

// The mode header reaches the service command (FR-1.7).
func TestIngestModeHeaderMapped(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{Async: true, Ticket: "tkt_1"}}
	rec := postMode(t, ingestRouter(t, svc), statePath, "Bearer key-a", "fast", `{"state":{"amount":1}}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if svc.lastCmd.Mode != domain.ReliabilityFast {
		t.Fatalf("mode = %q, want fast", svc.lastCmd.Mode)
	}
}

func TestIngestInvalidModeHeader(t *testing.T) {
	rec := postMode(t, ingestRouter(t, &fakeIngest{}), statePath, "Bearer key-a", "turbo", `{"state":{"amount":1}}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (invalid mode)", rec.Code)
	}
}

// The Idempotency-Key header is folded onto event_id when the body omits it
// (FR-1.6), so both barriers key off a single field.
func TestIngestIdempotencyKeyHeaderMapped(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{Async: true, Ticket: "tkt_1"}}
	req := httptest.NewRequest(http.MethodPost, statePath, strings.NewReader(`{"state":{"amount":1}}`))
	req.Header.Set("Authorization", "Bearer key-a")
	req.Header.Set("Idempotency-Key", "idem-9")
	rec := httptest.NewRecorder()
	ingestRouter(t, svc).ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", rec.Code)
	}
	if svc.lastCmd.EventID != "idem-9" {
		t.Fatalf("event_id = %q, want idem-9 from header", svc.lastCmd.EventID)
	}
}

// A body event_id takes precedence over the header.
func TestIngestEventIDBeatsHeader(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{Async: true, Ticket: "tkt_1"}}
	req := httptest.NewRequest(http.MethodPost, statePath, strings.NewReader(`{"event_id":"body-1","state":{"amount":1}}`))
	req.Header.Set("Authorization", "Bearer key-a")
	req.Header.Set("Idempotency-Key", "header-1")
	rec := httptest.NewRecorder()
	ingestRouter(t, svc).ServeHTTP(rec, req)
	if svc.lastCmd.EventID != "body-1" {
		t.Fatalf("event_id = %q, want body-1", svc.lastCmd.EventID)
	}
}

// A deduplicated sync write is a 200 "duplicate", not a 201.
func TestIngestDeduplicated(t *testing.T) {
	svc := &fakeIngest{result: service.IngestResult{EntityID: "d-1", Deduplicated: true}}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"event_id":"evt-1","state":{"amount":1}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "duplicate") {
		t.Fatalf("body = %s, want duplicate", rec.Body.String())
	}
}

// Backpressure surfaces as 429 with a Retry-After header (NFR-2.3).
func TestIngestBackpressure(t *testing.T) {
	svc := &fakeIngest{err: &service.BackpressureError{RetryAfter: 5 * time.Second}}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"state":{"amount":1}}`)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if ra := rec.Header().Get("Retry-After"); ra != "5" {
		t.Fatalf("Retry-After = %q, want 5", ra)
	}
}

func TestIngestQueueUnavailable(t *testing.T) {
	svc := &fakeIngest{err: queue.ErrQueueUnavailable}
	rec := post(t, ingestRouter(t, svc), statePath, "Bearer key-a", `{"state":{"amount":1}}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestIngestInvalidCollectionName(t *testing.T) {
	rec := post(t, ingestRouter(t, &fakeIngest{}), "/api/v1/collections/CRM..bad/entities/d-1/state", "Bearer key-a", `{"state":{}}`)
	// key-a's mask is crm.* so the bad name is rejected by access control
	// first (403); the point is it never reaches the service as a 500.
	if rec.Code != http.StatusBadRequest && rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 400 or 403", rec.Code)
	}
}
