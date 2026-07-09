package server

import (
	"errors"
	"net/http"

	"github.com/max-trifonov/letopis/internal/domain"
)

// flowRefInput is the wire form of a caused_by / refs edge. Exactly one shape is
// expected: an activity_id, or a (collection, entity_id) pair with version or
// event_id. Version is a pointer to distinguish absent from a real zero.
type flowRefInput struct {
	ActivityID string `json:"activity_id"`
	Collection string `json:"collection"`
	EntityID   string `json:"entity_id"`
	Version    *int64 `json:"version"`
	EventID    string `json:"event_id"`
}

type flowBlockInput struct {
	FlowID   string         `json:"flow_id"`
	CausedBy []flowRefInput `json:"caused_by"`
	Step     string         `json:"step"`
}

// mapFlowRefs validates and converts the ref list. A ref must identify either
// an activity or an entity; dangling targets are allowed.
func mapFlowRefs(w http.ResponseWriter, in []flowRefInput) ([]domain.FlowRef, bool) {
	out, err := mapFlowRefsE(in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return out, true
}

// mapFlowRefsE is the error-returning core of mapFlowRefs, shared with the batch
// path, which collects per-item rejects rather than writing a response.
func mapFlowRefsE(in []flowRefInput) ([]domain.FlowRef, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]domain.FlowRef, len(in))
	for i, r := range in {
		hasActivity := r.ActivityID != ""
		hasEntity := r.Collection != "" && r.EntityID != ""
		if !hasActivity && !hasEntity {
			return nil, errors.New("flow ref must have activity_id or collection+entity_id")
		}
		ref := domain.FlowRef{
			ActivityID: r.ActivityID,
			Collection: r.Collection,
			EntityID:   r.EntityID,
			EventID:    r.EventID,
		}
		if r.Version != nil {
			ref.Version = *r.Version
		}
		out[i] = ref
	}
	return out, nil
}

// mapFlowBlock converts the optional event flow block. A nil input yields a nil
// domain block (the common case — most events are outside a flow).
func mapFlowBlock(w http.ResponseWriter, in *flowBlockInput) (*domain.Flow, bool) {
	flow, err := mapFlowBlockE(in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return flow, true
}

// mapFlowBlockE is the error-returning core of mapFlowBlock, shared with the
// batch path.
func mapFlowBlockE(in *flowBlockInput) (*domain.Flow, error) {
	if in == nil {
		return nil, nil
	}
	causedBy, err := mapFlowRefsE(in.CausedBy)
	if err != nil {
		return nil, err
	}
	return &domain.Flow{ID: in.FlowID, CausedBy: causedBy, Step: in.Step}, nil
}

// flowRefToDTO renders a domain ref back to its wire shape, emitting only the
// fields that are set so each ref keeps its original shape on read.
func flowRefToDTO(r domain.FlowRef) map[string]any {
	m := map[string]any{}
	if r.ActivityID != "" {
		m["activity_id"] = r.ActivityID
	}
	if r.Collection != "" {
		m["collection"] = r.Collection
	}
	if r.EntityID != "" {
		m["entity_id"] = r.EntityID
	}
	if r.Version != 0 {
		m["version"] = r.Version
	}
	if r.EventID != "" {
		m["event_id"] = r.EventID
	}
	return m
}

func flowRefsToDTO(refs []domain.FlowRef) []any {
	out := make([]any, len(refs))
	for i, r := range refs {
		out[i] = flowRefToDTO(r)
	}
	return out
}
