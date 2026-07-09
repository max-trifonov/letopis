package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/service"
)

// fakeBatch implements BatchService, capturing the entries and prior rejects it
// was handed so a test can assert the transport→service mapping, and echoing the
// rejects back so the response shape can be checked.
type fakeBatch struct {
	entries []service.BatchEntry
	prior   []service.BatchReject
	mode    domain.ReliabilityMode
	err     error
}

func (f *fakeBatch) Ingest(_ context.Context, entries []service.BatchEntry, prior []service.BatchReject, mode domain.ReliabilityMode) (service.BatchResult, error) {
	f.entries, f.prior, f.mode = entries, prior, mode
	if f.err != nil {
		return service.BatchResult{}, f.err
	}
	return service.BatchResult{Ticket: "tkt_batch", Accepted: len(entries), Rejected: prior}, nil
}

func batchRouter(t *testing.T, svc BatchService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), testResolver(t), nil, nil, nil, nil, nil, nil, svc, nil, nil)
}

const batchPath = "/api/v1/events:batch"

type batchResp struct {
	TicketID string `json:"ticket_id"`
	Accepted int    `json:"accepted"`
	Rejected []struct {
		Index int `json:"index"`
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	} `json:"rejected"`
}

func TestBatchAccepted(t *testing.T) {
	svc := &fakeBatch{}
	body := `{"events":[
		{"collection":"crm.deals","entity_id":"d-1","type":"state","payload":{"state":{"amount":100}}},
		{"collection":"crm.deals","entity_id":"d-2","type":"diff","payload":{"op":"update","changes":[{"path":"amount","op":"change","old":1,"new":2}]}},
		{"collection":"crm.deals","entity_id":"d-3","type":"delete","payload":{}}
	]}`
	rec := post(t, batchRouter(t, svc), batchPath, "Bearer key-a", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if len(svc.entries) != 3 {
		t.Fatalf("service got %d entries, want 3", len(svc.entries))
	}
	// Mapping: kinds and the diff's changes survived the boundary.
	if svc.entries[0].Kind != service.KindState || svc.entries[1].Kind != service.KindDiff || svc.entries[2].Kind != service.KindDelete {
		t.Fatalf("kinds wrong: %+v", svc.entries)
	}
	if len(svc.entries[1].Command.Changes) != 1 || svc.entries[1].Command.Changes[0].Path != "amount" {
		t.Fatalf("diff changes not mapped: %+v", svc.entries[1].Command)
	}
	if svc.entries[2].Command.Op != domain.OpDelete {
		t.Fatalf("delete op not set: %+v", svc.entries[2].Command)
	}

	var resp batchResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.TicketID != "tkt_batch" || resp.Accepted != 3 || len(resp.Rejected) != 0 {
		t.Fatalf("response = %+v", resp)
	}
}

// Structural failures are rejected up front with their original indices, while
// the valid items still reach the service.
func TestBatchPartialStructural(t *testing.T) {
	svc := &fakeBatch{}
	body := `{"events":[
		{"collection":"crm.deals","entity_id":"d-1","type":"state","payload":{"state":{"amount":100}}},
		{"collection":"crm.deals","entity_id":"","type":"state","payload":{"state":{}}},
		{"collection":"crm.deals","entity_id":"d-3","type":"frobnicate","payload":{}},
		{"collection":"crm.deals","entity_id":"d-4","type":"state","payload":{}}
	]}`
	rec := post(t, batchRouter(t, svc), batchPath, "Bearer key-a", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if len(svc.entries) != 1 || svc.entries[0].Index != 0 {
		t.Fatalf("only index 0 should be valid: %+v", svc.entries)
	}
	codes := map[int]string{}
	for _, r := range svc.prior {
		codes[r.Index] = r.Code
	}
	if codes[1] != batchRejectInvalidEntity || codes[2] != batchRejectInvalidType || codes[3] != batchRejectInvalidPayload {
		t.Fatalf("structural rejects wrong: %+v", svc.prior)
	}
}

// The per-request mode override is parsed once and passed to the service; an
// unknown value is a clean 400 before any work.
func TestBatchModeOverride(t *testing.T) {
	svc := &fakeBatch{}
	body := `{"events":[{"collection":"crm.deals","entity_id":"d-1","type":"state","payload":{"state":{"amount":1}}}]}`
	rec := postH(t, batchRouter(t, svc), batchPath, "Bearer key-a", body, map[string]string{modeHeader: "fast"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if svc.mode != domain.ReliabilityFast {
		t.Fatalf("mode = %q, want fast", svc.mode)
	}
}

func TestBatchInvalidMode(t *testing.T) {
	svc := &fakeBatch{}
	body := `{"events":[{"collection":"crm.deals","entity_id":"d-1","type":"state","payload":{"state":{"amount":1}}}]}`
	rec := postH(t, batchRouter(t, svc), batchPath, "Bearer key-a", body, map[string]string{modeHeader: "loud"})
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if svc.entries != nil {
		t.Fatal("service must not be called on a bad mode")
	}
}

func TestBatchEmpty(t *testing.T) {
	rec := post(t, batchRouter(t, &fakeBatch{}), batchPath, "Bearer key-a", `{"events":[]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestBatchTooMany(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"events":[`)
	for i := 0; i < maxBatchItems+1; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"collection":"crm.deals","entity_id":"d","type":"state","payload":{"state":{}}}`)
	}
	b.WriteString(`]}`)
	rec := post(t, batchRouter(t, &fakeBatch{}), batchPath, "Bearer key-a", b.String())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestBatchBadJSON(t *testing.T) {
	rec := post(t, batchRouter(t, &fakeBatch{}), batchPath, "Bearer key-a", `{not json`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

// The write scope is required (FR-6.2): a read-only key is 403.
func TestBatchRequiresWriteScope(t *testing.T) {
	body := `{"events":[{"collection":"docs.x","entity_id":"d-1","type":"state","payload":{"state":{}}}]}`
	rec := post(t, batchRouter(t, &fakeBatch{}), batchPath, "Bearer key-b", body)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

// A collection outside the key's mask is rejected per item (FR-7.2), not as a
// whole-request 403 — a batch may legitimately span collections.
func TestBatchMaskedCollectionRejected(t *testing.T) {
	svc := &fakeBatch{}
	body := `{"events":[{"collection":"docs.secret","entity_id":"d-1","type":"state","payload":{"state":{}}}]}`
	rec := post(t, batchRouter(t, svc), batchPath, "Bearer key-a", body)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if len(svc.entries) != 0 || len(svc.prior) != 1 || svc.prior[0].Code != batchRejectForbidden {
		t.Fatalf("masked collection should be a forbidden reject: entries=%+v prior=%+v", svc.entries, svc.prior)
	}
}

// postH is post with extra headers, for the mode-override cases.
func postH(t *testing.T, h http.Handler, path, auth, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
