package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/service"
)

type ActivityService interface {
	Record(ctx context.Context, cmd service.RecordActivityCommand) (service.RecordActivityResult, error)
	Flow(ctx context.Context, flowID string, after *domain.FlowPosition, limit int) (service.FlowResult, error)
}

type activityHandlers struct {
	svc ActivityService
}

type activityRequest struct {
	ActivityID string         `json:"activity_id"`
	Type       string         `json:"type"`
	FlowID     string         `json:"flow_id"`
	AuthorID   string         `json:"author_id"`
	Source     string         `json:"source"`
	TSSource   *time.Time     `json:"ts_source"`
	CausedBy   []flowRefInput `json:"caused_by"`
	Refs       []flowRefInput `json:"refs"`
	Data       map[string]any `json:"data"`
	Meta       map[string]any `json:"meta"`
}

func (h activityHandlers) record(w http.ResponseWriter, r *http.Request) {
	// Activities are tenant-scoped, not bound to one collection; there is no
	// collection mask to check. The size cap is the default event size.
	body, ok := readLimited(w, r, 0)
	if !ok {
		return
	}
	var req activityRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	causedBy, ok := mapFlowRefs(w, req.CausedBy)
	if !ok {
		return
	}
	refs, ok := mapFlowRefs(w, req.Refs)
	if !ok {
		return
	}

	cmd := service.RecordActivityCommand{
		ActivityID: req.ActivityID,
		Type:       req.Type,
		FlowID:     req.FlowID,
		CausedBy:   causedBy,
		Refs:       refs,
		Author:     req.AuthorID,
		Source:     req.Source,
		Data:       req.Data,
		Meta:       req.Meta,
	}
	if req.TSSource != nil {
		cmd.TSSource = req.TSSource.UTC()
	}

	res, err := h.svc.Record(r.Context(), cmd)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"activity_id": res.ActivityID,
		"flow_id":     res.FlowID,
	})
}

func (h activityHandlers) flow(w http.ResponseWriter, r *http.Request) {
	flowID := chi.URLParam(r, "flowId")
	if flowID == "" {
		writeError(w, http.StatusBadRequest, "flow_id is required")
		return
	}

	limit := defaultHistoryLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return
		}
		limit = min(n, maxHistoryLimit)
	}

	var after *domain.FlowPosition
	if c := r.URL.Query().Get("cursor"); c != "" {
		pos, err := decodeFlowCursor(c)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return
		}
		after = &pos
	}

	res, err := h.svc.Flow(r.Context(), flowID, after, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, flowBody(res))
}

func flowBody(res service.FlowResult) map[string]any {
	nodes := make([]map[string]any, 0, len(res.Nodes))
	for _, n := range res.Nodes {
		nodes = append(nodes, flowNodeToDTO(n))
	}
	body := map[string]any{
		"flow_id":     res.FlowID,
		"nodes":       nodes,
		"next_cursor": nil,
	}
	if res.Next != nil {
		body["next_cursor"] = encodeFlowCursor(*res.Next)
	}
	return body
}

func flowNodeToDTO(n domain.FlowNode) map[string]any {
	if n.Kind == domain.FlowNodeActivity {
		a := n.Activity
		m := map[string]any{
			"kind":        string(domain.FlowNodeActivity),
			"activity_id": a.ActivityID,
			"ts_received": a.TSReceived,
			"caused_by":   flowRefsToDTO(a.CausedBy),
			"refs":        flowRefsToDTO(a.Refs),
		}
		if a.Type != "" {
			m["type"] = a.Type
		}
		if a.Data != nil {
			m["data"] = a.Data
		}
		return m
	}

	ev := n.Event
	m := map[string]any{
		"kind":        string(domain.FlowNodeEvent),
		"collection":  n.Collection,
		"entity_id":   ev.EntityID,
		"version":     ev.Version,
		"op":          string(ev.Op),
		"ts_received": ev.TSReceived,
	}
	if ev.Flow != nil {
		if ev.Flow.Step != "" {
			m["step"] = ev.Flow.Step
		}
		m["caused_by"] = flowRefsToDTO(ev.Flow.CausedBy)
	} else {
		m["caused_by"] = []any{}
	}
	return m
}

// flowCursorPayload is the opaque cursor form of a FlowPosition. Clients must
// treat it as opaque; the internal structure is not part of the API contract.
type flowCursorPayload struct {
	TS time.Time `json:"ts"`
	ID string    `json:"id"`
}

func encodeFlowCursor(p domain.FlowPosition) string {
	b, _ := json.Marshal(flowCursorPayload{TS: p.TS, ID: p.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeFlowCursor(s string) (domain.FlowPosition, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return domain.FlowPosition{}, err
	}
	var p flowCursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return domain.FlowPosition{}, err
	}
	return domain.FlowPosition{TS: p.TS, ID: p.ID}, nil
}
