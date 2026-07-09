package service

import (
	"context"
	"sync"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
)

// fakeTickets is an in-memory domain.TicketStore for the lifecycle tests.
type fakeTickets struct {
	mu sync.Mutex
	m  map[string]domain.Ticket
}

func newFakeTickets() *fakeTickets { return &fakeTickets{m: map[string]domain.Ticket{}} }

func (f *fakeTickets) Create(_ context.Context, t *domain.Ticket) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.m[t.ID] = *t
	return nil
}

func (f *fakeTickets) Get(_ context.Context, id string) (*domain.Ticket, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	t, ok := f.m[id]
	if !ok {
		return nil, domain.ErrTicketNotFound
	}
	return &t, nil
}

func (f *fakeTickets) Save(_ context.Context, t *domain.Ticket) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.m[t.ID]; !ok {
		return domain.ErrTicketNotFound
	}
	f.m[t.ID] = *t
	return nil
}

func TestTicketServiceOpenAndAdvance(t *testing.T) {
	store := newFakeTickets()
	svc := NewTicketService(store)
	ctx := context.Background()

	if err := svc.Open(ctx, "tkt_1", "crm.deals", "d-1"); err != nil {
		t.Fatalf("Open: %v", err)
	}
	got, err := svc.Get(ctx, "tkt_1")
	if err != nil || got.Status != domain.TicketAccepted || got.EntityID != "d-1" {
		t.Fatalf("after open = %+v, err=%v", got, err)
	}

	if err := svc.Mark(ctx, "tkt_1", domain.TicketProcessing, ""); err != nil {
		t.Fatalf("Mark processing: %v", err)
	}
	if err := svc.Mark(ctx, "tkt_1", domain.TicketStored, ""); err != nil {
		t.Fatalf("Mark stored: %v", err)
	}
	got, _ = svc.Get(ctx, "tkt_1")
	if got.Status != domain.TicketStored {
		t.Fatalf("status = %s, want stored", got.Status)
	}
}

// A reprocessed delivery (reclaim) must not drag a settled ticket backwards.
func TestTicketServiceMarkIsReclaimSafe(t *testing.T) {
	store := newFakeTickets()
	svc := NewTicketService(store)
	ctx := context.Background()
	_ = svc.Open(ctx, "tkt_1", "c", "e")
	_ = svc.Mark(ctx, "tkt_1", domain.TicketStored, "")

	// Reclaim path tries to set processing again; must be a no-op.
	if err := svc.Mark(ctx, "tkt_1", domain.TicketProcessing, ""); err != nil {
		t.Fatalf("Mark: %v", err)
	}
	got, _ := svc.Get(ctx, "tkt_1")
	if got.Status != domain.TicketStored {
		t.Fatalf("stored ticket regressed to %s", got.Status)
	}
}

// Marking an expired/unknown ticket is ignored: the write proceeds, only its
// tracking lapsed.
func TestTicketServiceMarkMissingIsNoop(t *testing.T) {
	svc := NewTicketService(newFakeTickets())
	if err := svc.Mark(context.Background(), "tkt_gone", domain.TicketStored, ""); err != nil {
		t.Fatalf("Mark missing = %v, want nil", err)
	}
	if err := svc.Mark(context.Background(), "", domain.TicketStored, ""); err != nil {
		t.Fatalf("Mark empty id = %v, want nil", err)
	}
}
