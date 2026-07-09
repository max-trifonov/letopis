package service

import (
	"encoding/json"
	"fmt"

	"github.com/max-trifonov/letopis/internal/domain"
)

// Kind selects which Ingester entry point a task targets; the producer sets it
// from the request route.
type Kind string

const (
	KindState  Kind = "state"
	KindDiff   Kind = "diff"
	KindDelete Kind = "delete"
)

// TenantRef carries the minimum principal fields needed by the worker to select
// the tenant's database. Authorization already happened at ingest; scopes are not carried.
type TenantRef struct {
	ID     string `json:"id"`
	DBURI  string `json:"db_uri,omitempty"`
	DBName string `json:"db_name,omitempty"`
}

// Task is the cross-process envelope for an async write. Lives in service so
// producer (AsyncIngester) and consumer (worker) share one codec without an
// import cycle.
type Task struct {
	Tenant TenantRef              `json:"tenant"`
	Kind   Kind                   `json:"kind"`
	Mode   domain.ReliabilityMode `json:"mode"`
	// TicketID ties the async write to its tracking ticket. The worker moves it
	// through its lifecycle as it processes the task; empty when ticketing is disabled.
	TicketID string        `json:"ticket_id,omitempty"`
	Command  IngestCommand `json:"command"`
}

// Encode serializes a task into a queue payload.
func Encode(t Task) ([]byte, error) {
	return json.Marshal(t)
}

func Decode(b []byte) (Task, error) {
	var t Task
	if err := json.Unmarshal(b, &t); err != nil {
		return Task{}, fmt.Errorf("service: decode task envelope: %w", err)
	}
	return t, nil
}
