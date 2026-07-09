package server

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
)

// fakeDLQ is a transport-level stub DLQService: it serves a fixed page and
// records redeliver calls so the handler's scope, mask, paging and status
// mapping can be asserted without the real service.
type fakeDLQ struct {
	items        []domain.DeadLetter
	listLimit    int
	listAfter    *domain.DLQCursor
	redeliverID  []string
	redeliverN   int
	redeliverErr error
}

func (f *fakeDLQ) List(_ context.Context, _ string, limit int, after *domain.DLQCursor) ([]domain.DeadLetter, error) {
	f.listLimit = limit
	f.listAfter = after
	return f.items, nil
}

func (f *fakeDLQ) Redeliver(_ context.Context, _ string, ids []string) (int, error) {
	f.redeliverID = ids
	if f.redeliverErr != nil {
		return 0, f.redeliverErr
	}
	if f.redeliverN != 0 {
		return f.redeliverN, nil
	}
	return len(ids), nil
}

func dlqRouter(t *testing.T, svc DLQService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), adminResolver(t), nil, nil, nil, nil, nil, nil, nil, nil, svc)
}

const dlqPath = "/api/v1/collections/crm.deals/rules/rule_1/dlq"

func TestDLQListReturnsItems(t *testing.T) {
	svc := &fakeDLQ{items: []domain.DeadLetter{
		{ID: "dlq_2", RuleID: "rule_1", Collection: "crm.deals", DeliveryID: "dlv_2", URL: "https://x.test", Attempts: 3, LastError: "status 500", FailedAt: time.Unix(2000, 0).UTC(), Body: []byte(`{"a":1}`)},
		{ID: "dlq_1", RuleID: "rule_1", Collection: "crm.deals", DeliveryID: "dlv_1", URL: "https://x.test", Attempts: 5, FailedAt: time.Unix(1000, 0).UTC()},
	}}
	rec := do(t, dlqRouter(t, svc), http.MethodGet, dlqPath, "Bearer key-admin")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Items      []map[string]any `json:"items"`
		NextCursor *string          `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("items = %d, want 2", len(resp.Items))
	}
	if resp.Items[0]["id"] != "dlq_2" || resp.Items[0]["delivery_id"] != "dlv_2" {
		t.Fatalf("first item wrong: %+v", resp.Items[0])
	}
	// Body is embedded as raw JSON, not a string.
	if _, ok := resp.Items[0]["body"].(map[string]any); !ok {
		t.Fatalf("body not embedded as JSON object: %+v", resp.Items[0]["body"])
	}
	// A short page (2 < default limit) has no next cursor.
	if resp.NextCursor != nil {
		t.Fatalf("next_cursor = %v, want null on a short page", *resp.NextCursor)
	}
}

func TestDLQListPagesWhenFull(t *testing.T) {
	// A full page (len == limit) must return a next cursor.
	items := make([]domain.DeadLetter, 2)
	base := time.Unix(5000, 0).UTC()
	for i := range items {
		items[i] = domain.DeadLetter{ID: "dlq_" + string(rune('a'+i)), RuleID: "rule_1", FailedAt: base.Add(-time.Duration(i) * time.Second)}
	}
	svc := &fakeDLQ{items: items}
	rec := do(t, dlqRouter(t, svc), http.MethodGet, dlqPath+"?limit=2", "Bearer key-admin")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if svc.listLimit != 2 {
		t.Fatalf("service got limit %d, want 2", svc.listLimit)
	}
	var resp struct {
		NextCursor *string `json:"next_cursor"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.NextCursor == nil {
		t.Fatal("next_cursor missing on a full page")
	}
	// The cursor round-trips back into a position the next request can pass.
	if _, err := decodeDLQCursor(*resp.NextCursor); err != nil {
		t.Fatalf("next_cursor not decodable: %v", err)
	}
}

func TestDLQListRejectsBadCursor(t *testing.T) {
	rec := do(t, dlqRouter(t, &fakeDLQ{}), http.MethodGet, dlqPath+"?cursor=!!notbase64!!", "Bearer key-admin")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestDLQRequiresAdminScope(t *testing.T) {
	// key-rw has read+write but not admin: the router rejects it.
	rec := do(t, dlqRouter(t, &fakeDLQ{}), http.MethodGet, dlqPath, "Bearer key-rw")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestDLQEnforcesCollectionMask(t *testing.T) {
	// The admin key's mask is crm.*; a docs.* collection is outside it.
	rec := do(t, dlqRouter(t, &fakeDLQ{}), http.MethodGet, "/api/v1/collections/docs.private/rules/rule_1/dlq", "Bearer key-admin")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

func TestDLQRedeliverAll(t *testing.T) {
	svc := &fakeDLQ{redeliverN: 3}
	rec := post(t, dlqRouter(t, svc), dlqPath+":redeliver", "Bearer key-admin", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Requeued int `json:"requeued"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	if resp.Requeued != 3 {
		t.Fatalf("requeued = %d, want 3", resp.Requeued)
	}
	if svc.redeliverID != nil {
		t.Fatalf("redeliver ids = %v, want nil (all)", svc.redeliverID)
	}
}

func TestDLQRedeliverIDs(t *testing.T) {
	svc := &fakeDLQ{}
	rec := post(t, dlqRouter(t, svc), dlqPath+":redeliver", "Bearer key-admin", `{"ids":["dlq_1","dlq_2"]}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", rec.Code, rec.Body.String())
	}
	if len(svc.redeliverID) != 2 {
		t.Fatalf("redeliver ids = %v, want 2", svc.redeliverID)
	}
}

func TestDLQRedeliverUnknownIs404(t *testing.T) {
	svc := &fakeDLQ{redeliverErr: domain.ErrNotFound}
	rec := post(t, dlqRouter(t, svc), dlqPath+":redeliver", "Bearer key-admin", `{"ids":["dlq_x"]}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}
