package service

import (
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
)

func TestEnvelopeRoundTrip(t *testing.T) {
	task := Task{
		Tenant:   TenantRef{ID: "acme", DBName: "hm_t_acme"},
		Kind:     KindState,
		Mode:     domain.ReliabilityFast,
		TicketID: "tkt_123",
		Command: IngestCommand{
			Collection: "crm.deals",
			EntityID:   "d-1",
			State:      map[string]any{"amount": float64(250)},
		},
	}
	b, err := Encode(task)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Tenant.ID != "acme" || got.Kind != KindState || got.Mode != domain.ReliabilityFast || got.TicketID != "tkt_123" {
		t.Errorf("round-trip header = %+v", got)
	}
	if got.Command.EntityID != "d-1" || got.Command.State["amount"] != float64(250) {
		t.Errorf("round-trip command = %+v", got.Command)
	}
}

func TestDecodeRejectsGarbage(t *testing.T) {
	if _, err := Decode([]byte("not json")); err == nil {
		t.Fatal("Decode accepted garbage")
	}
}
