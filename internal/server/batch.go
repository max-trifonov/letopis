package server

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5/middleware"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/service"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// maxBatchItems caps the events in one batch; beyond it the request is a 400
// rather than a partial accept, so a client cannot smuggle an unbounded fan-out.
const maxBatchItems = 1000

// maxBatchBodyBytes bounds the whole batch body so a malformed or hostile
// request cannot exhaust memory before the per-item size limit (enforced in the
// service against each collection's config) ever runs. It is deliberately
// generous — a batch is meant for many small events, not a few large ones.
const maxBatchBodyBytes = 32 << 20 // 32 MiB

// Transport-side reject codes for structural failures caught before the service.
// Config-dependent codes (size, auto-create) live in the service.
const (
	batchRejectInvalidCollection = "invalid_collection"
	batchRejectForbidden         = "forbidden"
	batchRejectInvalidEntity     = "invalid_entity_id"
	batchRejectInvalidType       = "invalid_type"
	batchRejectInvalidPayload    = "invalid_payload"
)

type BatchService interface {
	Ingest(ctx context.Context, entries []service.BatchEntry, priorRejects []service.BatchReject, modeOverride domain.ReliabilityMode) (service.BatchResult, error)
}

type batchHandlers struct {
	svc BatchService
}

var batchKinds = map[string]service.Kind{
	"state":  service.KindState,
	"diff":   service.KindDiff,
	"delete": service.KindDelete,
}

type batchRequest struct {
	Events []batchItemRequest `json:"events"`
}

type batchItemRequest struct {
	Collection string          `json:"collection"`
	EntityID   string          `json:"entity_id"`
	Type       string          `json:"type"`
	Payload    json.RawMessage `json:"payload"`
}

type batchResponse struct {
	TicketID string           `json:"ticket_id"`
	Accepted int              `json:"accepted"`
	Rejected []batchRejectDTO `json:"rejected"`
}

type batchRejectDTO struct {
	Index int         `json:"index"`
	Error batchErrDTO `json:"error"`
}

type batchErrDTO struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ingest serves POST /events:batch: up to 1000 self-contained events, partially
// accepted. Invalid elements go into rejected[] with their index; the rest are
// handed to the service. The batch is not atomic — one bad element never blocks
// its neighbours. A single X-Letopis-Mode override applies to every element.
func (h batchHandlers) ingest(w http.ResponseWriter, r *http.Request) {
	p, ok := tenant.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return
	}
	mode, ok := parseMode(w, r)
	if !ok {
		return
	}
	body, ok := readLimited(w, r, maxBatchBodyBytes)
	if !ok {
		return
	}
	var req batchRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(req.Events) == 0 {
		writeError(w, http.StatusBadRequest, "events must contain at least one event")
		return
	}
	if len(req.Events) > maxBatchItems {
		writeError(w, http.StatusBadRequest, "batch exceeds 1000 events")
		return
	}

	reqID := middleware.GetReqID(r.Context())
	ip := clientIP(r)
	entries := make([]service.BatchEntry, 0, len(req.Events))
	var rejects []service.BatchReject
	for i, item := range req.Events {
		entry, rej := buildBatchEntry(p, item, i, mode, reqID, ip)
		if rej != nil {
			rejects = append(rejects, *rej)
			continue
		}
		entries = append(entries, entry)
	}

	res, err := h.svc.Ingest(r.Context(), entries, rejects, mode)
	if err != nil {
		// Reuses the single-write error mapping: batch-wide backpressure → 429 + Retry-After.
		writeIngestError(w, err)
		return
	}

	out := batchResponse{TicketID: res.Ticket, Accepted: res.Accepted}
	out.Rejected = make([]batchRejectDTO, len(res.Rejected))
	for i, rj := range res.Rejected {
		out.Rejected[i] = batchRejectDTO{Index: rj.Index, Error: batchErrDTO{Code: rj.Code, Message: rj.Message}}
	}
	writeJSON(w, http.StatusAccepted, out)
}

// buildBatchEntry validates one element and maps it onto a service entry, or
// returns a reject with its index. The collection mask is enforced per element:
// a batch may span collections, so a forbidden one is rejected individually.
func buildBatchEntry(p tenant.Principal, item batchItemRequest, index int, mode domain.ReliabilityMode, reqID, ip string) (service.BatchEntry, *service.BatchReject) {
	reject := func(code, msg string) *service.BatchReject {
		return &service.BatchReject{Index: index, Code: code, Message: msg}
	}
	if !validCollectionName(item.Collection) {
		return service.BatchEntry{}, reject(batchRejectInvalidCollection, "invalid collection name")
	}
	if !p.CanAccess(item.Collection) {
		return service.BatchEntry{}, reject(batchRejectForbidden, "collection not allowed")
	}
	if item.EntityID == "" {
		return service.BatchEntry{}, reject(batchRejectInvalidEntity, "entity_id is required")
	}
	kind, ok := batchKinds[item.Type]
	if !ok {
		return service.BatchEntry{}, reject(batchRejectInvalidType, "invalid type "+item.Type+" (want state, diff or delete)")
	}
	if len(item.Payload) == 0 {
		return service.BatchEntry{}, reject(batchRejectInvalidPayload, "payload is required")
	}
	var req ingestRequest
	if err := json.Unmarshal(item.Payload, &req); err != nil {
		return service.BatchEntry{}, reject(batchRejectInvalidPayload, "invalid payload")
	}
	cmd, rej := buildBatchCommand(item, req, kind, index, mode, reqID, ip)
	if rej != nil {
		return service.BatchEntry{}, rej
	}
	return service.BatchEntry{Index: index, Kind: kind, Command: cmd, BodySize: int64(len(item.Payload))}, nil
}

// buildBatchCommand maps one element's payload onto the service command, applying
// the same per-kind rules as the single-write handlers: state requires a state
// object, diff needs changes unless it is a create-with-state or a delete, and
// the delete type does not accept a conflicting op. event_id rides on the payload
// only — a batch has no per-item Idempotency-Key header.
func buildBatchCommand(item batchItemRequest, req ingestRequest, kind service.Kind, index int, mode domain.ReliabilityMode, reqID, ip string) (service.IngestCommand, *service.BatchReject) {
	reject := func(msg string) *service.BatchReject {
		return &service.BatchReject{Index: index, Code: batchRejectInvalidPayload, Message: msg}
	}
	op, err := parseOpE(req.Op)
	if err != nil {
		return service.IngestCommand{}, reject(err.Error())
	}
	flow, err := mapFlowBlockE(req.Flow)
	if err != nil {
		return service.IngestCommand{}, reject(err.Error())
	}
	cmd := service.IngestCommand{
		Collection:      item.Collection,
		EntityID:        item.EntityID,
		Op:              op,
		Mode:            mode,
		ExpectedVersion: req.ExpectedVersion,
		Author:          req.AuthorID,
		Source:          req.Source,
		EventID:         req.EventID,
		RequestID:       reqID,
		ClientIP:        ip,
		Meta:            req.Meta,
		Flow:            flow,
	}
	if req.TSSource != nil {
		cmd.TSSource = req.TSSource.UTC()
	}

	switch kind {
	case service.KindState:
		if req.State == nil {
			return service.IngestCommand{}, reject("state is required")
		}
		cmd.State = req.State
	case service.KindDiff:
		if req.Changes != nil {
			changes, err := mapChangesE(req.Changes)
			if err != nil {
				return service.IngestCommand{}, reject(err.Error())
			}
			cmd.Changes = changes
		}
		switch {
		case op == domain.OpCreate && req.State != nil:
			cmd.State = req.State
		case op == domain.OpDelete:
		case cmd.Changes == nil:
			return service.IngestCommand{}, reject("changes is required")
		}
	case service.KindDelete:
		if req.Op != "" && req.Op != string(domain.OpDelete) {
			return service.IngestCommand{}, reject("delete item does not accept op " + req.Op)
		}
		cmd.Op = domain.OpDelete
	}
	return cmd, nil
}
