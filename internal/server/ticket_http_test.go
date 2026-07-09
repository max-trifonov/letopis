package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
)

// fakeTicketRead serves a single canned ticket (or not-found) so the handler's
// status mapping can be asserted without Redis.
type fakeTicketRead struct {
	ticket *domain.Ticket
	err    error
}

func (f *fakeTicketRead) Get(_ context.Context, _ string) (*domain.Ticket, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.ticket, nil
}

func ticketRouter(t *testing.T, svc TicketReadService) http.Handler {
	t.Helper()
	return newRouter(health.NewRegistry(), testResolver(t), nil, nil, nil, svc, nil, nil, nil, nil, nil)
}

func getAuth(t *testing.T, h http.Handler, path, auth string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestTicketGetOK(t *testing.T) {
	now := time.Date(2026, 6, 14, 10, 0, 0, 0, time.UTC)
	svc := &fakeTicketRead{ticket: &domain.Ticket{
		ID: "tkt_1", Status: domain.TicketStored, EntityCollection: "crm.deals", EntityID: "d-1",
		CreatedAt: now, UpdatedAt: now,
	}}
	rec := getAuth(t, ticketRouter(t, svc), "/api/v1/tickets/tkt_1", "Bearer key-a")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var body ticketResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.TicketID != "tkt_1" || body.Status != "stored" || body.EntityID != "d-1" {
		t.Fatalf("ticket body wrong: %s", rec.Body.String())
	}
}

// An unknown, expired, or out-of-scope ticket is all 404 (the store reports
// not-found for another tenant's id), leaking nothing.
func TestTicketGetNotFound(t *testing.T) {
	svc := &fakeTicketRead{err: domain.ErrTicketNotFound}
	rec := getAuth(t, ticketRouter(t, svc), "/api/v1/tickets/tkt_x", "Bearer key-a")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestTicketGetRequiresAuth(t *testing.T) {
	svc := &fakeTicketRead{ticket: &domain.Ticket{ID: "tkt_1", Status: domain.TicketAccepted}}
	rec := getAuth(t, ticketRouter(t, svc), "/api/v1/tickets/tkt_1", "")
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}
