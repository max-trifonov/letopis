package service

import (
	"context"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// RecordActivityCommand is the boundary DTO for POST /activities. ActivityID
// and FlowID are optional: the service generates them when absent and always
// returns the effective values so the caller can reference them immediately.
type RecordActivityCommand struct {
	ActivityID string
	Type       string
	FlowID     string
	CausedBy   []domain.FlowRef
	Refs       []domain.FlowRef
	Author     string
	Source     string
	TSSource   time.Time
	Data       map[string]any
	Meta       map[string]any
}

type RecordActivityResult struct {
	ActivityID string
	FlowID     string
}

// FlowResult is an assembled flow page: nodes in received order with a cursor
// for the next page (nil on the last page).
type FlowResult struct {
	FlowID string
	Nodes  []domain.FlowNode
	Next   *domain.FlowPosition
}

// Activities orchestrates activity writes and flow reads. Flow reads merge
// ev__flow activities with a fan-out over the tenant's change collections.
type Activities struct {
	store     domain.FlowStore
	now       func() time.Time
	newFlowID func() string
	newActID  func() string
}

func NewActivities(store domain.FlowStore) *Activities {
	return &Activities{
		store:     store,
		now:       time.Now,
		newFlowID: domain.NewFlowID,
		newActID:  domain.NewActivityID,
	}
}

// Record persists an activity, minting any missing ids. A new flow is created
// when the caller supplies no flow id — an activity always belongs to exactly
// one flow.
func (s *Activities) Record(ctx context.Context, cmd RecordActivityCommand) (RecordActivityResult, error) {
	a := &domain.Activity{
		ActivityID: orElse(cmd.ActivityID, s.newActID),
		Type:       cmd.Type,
		FlowID:     orElse(cmd.FlowID, s.newFlowID),
		CausedBy:   cmd.CausedBy,
		Refs:       cmd.Refs,
		Author:     cmd.Author,
		Source:     cmd.Source,
		TSSource:   cmd.TSSource,
		TSReceived: s.now().UTC(),
		Data:       cmd.Data,
		Meta:       cmd.Meta,
	}
	if err := s.store.AppendActivity(ctx, a); err != nil {
		return RecordActivityResult{}, err
	}
	return RecordActivityResult{ActivityID: a.ActivityID, FlowID: a.FlowID}, nil
}

// Flow assembles a flow's nodes in received order with cursor pagination.
// Dangling references are not a concern: the flow is whatever activities and
// events carry its id, so an edge pointing nowhere simply has no matching node.
func (s *Activities) Flow(ctx context.Context, flowID string, after *domain.FlowPosition, limit int) (FlowResult, error) {
	activities, err := s.store.FlowActivities(ctx, flowID)
	if err != nil {
		return FlowResult{}, err
	}
	events, err := s.store.FlowEvents(ctx, flowID)
	if err != nil {
		return FlowResult{}, err
	}
	nodes, next := domain.AssembleFlow(activities, events, after, limit)
	return FlowResult{FlowID: flowID, Nodes: nodes, Next: next}, nil
}

func orElse(v string, gen func() string) string {
	if v != "" {
		return v
	}
	return gen()
}
