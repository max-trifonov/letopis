package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/service"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// modeHeader lets a client override the collection's reliability mode per
// request. An unknown value is 400, surfaced before any work starts.
const modeHeader = "X-Letopis-Mode"

// idempotencyKeyHeader is an alternative to the body's event_id; when the body
// omits it we fold the header value onto cmd.EventID.
const idempotencyKeyHeader = "Idempotency-Key"

// IngestService is the transport's view of the write use-case.
type IngestService interface {
	Config(ctx context.Context, collection string) (*domain.CollectionConfig, error)
	State(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error)
	Diff(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error)
	Delete(ctx context.Context, cmd service.IngestCommand) (service.IngestResult, error)
}

type ingestHandlers struct {
	svc IngestService
}

// ingestRequest is the wire body shared by /state, /diff and /delete. Op is a
// string, validated into a domain.EntityOp. expected_version uses *int64 to
// distinguish "absent" from an explicit zero.
type ingestRequest struct {
	EventID         string          `json:"event_id"`
	Op              string          `json:"op"`
	ExpectedVersion *int64          `json:"expected_version"`
	AuthorID        string          `json:"author_id"`
	TSSource        *time.Time      `json:"ts_source"`
	Source          string          `json:"source"`
	Meta            map[string]any  `json:"meta"`
	Flow            *flowBlockInput `json:"flow"`
	State           map[string]any  `json:"state"`
	Changes         []changeInput   `json:"changes"`
}

type changeInput struct {
	Path string `json:"path"`
	Op   string `json:"op"`
	Old  any    `json:"old"`
	New  any    `json:"new"`
}

func (h ingestHandlers) state(w http.ResponseWriter, r *http.Request) {
	collection, entityID, ok := pathParams(w, r)
	if !ok {
		return
	}
	req, ok := h.read(w, r, collection)
	if !ok {
		return
	}
	op, ok := parseOp(w, req.Op)
	if !ok {
		return
	}
	if req.State == nil {
		writeError(w, http.StatusBadRequest, "state is required")
		return
	}
	cmd, ok := buildCommand(w, r, collection, entityID, req, op)
	if !ok {
		return
	}
	cmd.State = req.State
	res, err := h.svc.State(r.Context(), cmd)
	h.respond(w, res, err)
}

func (h ingestHandlers) diff(w http.ResponseWriter, r *http.Request) {
	collection, entityID, ok := pathParams(w, r)
	if !ok {
		return
	}
	req, ok := h.read(w, r, collection)
	if !ok {
		return
	}
	op, ok := parseOp(w, req.Op)
	if !ok {
		return
	}
	cmd, ok := buildCommand(w, r, collection, entityID, req, op)
	if !ok {
		return
	}
	// A create may carry full state instead of a diff; a delete needs neither;
	// everything else must bring a changes array.
	switch {
	case op == domain.OpCreate && req.State != nil:
		cmd.State = req.State
	case op == domain.OpDelete:
	case cmd.Changes == nil:
		writeError(w, http.StatusBadRequest, "changes is required")
		return
	}
	res, err := h.svc.Diff(r.Context(), cmd)
	h.respond(w, res, err)
}

func (h ingestHandlers) delete(w http.ResponseWriter, r *http.Request) {
	collection, entityID, ok := pathParams(w, r)
	if !ok {
		return
	}
	req, ok := h.read(w, r, collection)
	if !ok {
		return
	}
	// The op is fixed; an explicit non-delete op in the body is a client error.
	if req.Op != "" && req.Op != string(domain.OpDelete) {
		writeError(w, http.StatusBadRequest, "delete endpoint does not accept op "+req.Op)
		return
	}
	cmd, ok := buildCommand(w, r, collection, entityID, req, domain.OpDelete)
	if !ok {
		return
	}
	res, err := h.svc.Delete(r.Context(), cmd)
	h.respond(w, res, err)
}

// read enforces the collection mask, resolves config (provisioning the
// collection on first use) and reads the body within the per-collection size
// limit. It returns the decoded request, or writes the appropriate error.
func (h ingestHandlers) read(w http.ResponseWriter, r *http.Request, collection string) (ingestRequest, bool) {
	if !canAccess(w, r, collection) {
		return ingestRequest{}, false
	}
	cfg, err := h.svc.Config(r.Context(), collection)
	if err != nil {
		writeIngestError(w, err)
		return ingestRequest{}, false
	}
	body, ok := readLimited(w, r, cfg.MaxEventSizeBytes)
	if !ok {
		return ingestRequest{}, false
	}
	var req ingestRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return ingestRequest{}, false
	}
	return req, true
}

func (h ingestHandlers) respond(w http.ResponseWriter, res service.IngestResult, err error) {
	if err != nil {
		writeIngestError(w, err)
		return
	}
	if res.Async {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"ticket_id": res.Ticket,
			"status":    string(domain.TicketAccepted),
		})
		return
	}
	if res.NoChanges {
		writeJSON(w, http.StatusOK, map[string]string{"status": "no_changes"})
		return
	}
	if res.Deduplicated {
		writeJSON(w, http.StatusOK, map[string]string{"status": "duplicate"})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"entity_id":     res.EntityID,
		"version":       res.Version,
		"changes_count": res.ChangesCount,
	})
}

// buildCommand maps the shared request fields and the change list into the
// service command. Per-endpoint fields (state) are set by the caller.
func buildCommand(w http.ResponseWriter, r *http.Request, collection, entityID string, req ingestRequest, op domain.EntityOp) (service.IngestCommand, bool) {
	eventID := req.EventID
	if eventID == "" {
		// The Idempotency-Key header is an alternative to the event_id field;
		// when both are absent the write simply carries no idempotency key.
		eventID = r.Header.Get(idempotencyKeyHeader)
	}
	cmd := service.IngestCommand{
		Collection:      collection,
		EntityID:        entityID,
		Op:              op,
		ExpectedVersion: req.ExpectedVersion,
		Author:          req.AuthorID,
		Source:          req.Source,
		EventID:         eventID,
		RequestID:       middleware.GetReqID(r.Context()),
		ClientIP:        clientIP(r),
		Meta:            req.Meta,
	}
	if req.TSSource != nil {
		cmd.TSSource = req.TSSource.UTC()
	}
	mode, ok := parseMode(w, r)
	if !ok {
		return service.IngestCommand{}, false
	}
	cmd.Mode = mode
	flow, ok := mapFlowBlock(w, req.Flow)
	if !ok {
		return service.IngestCommand{}, false
	}
	cmd.Flow = flow
	if req.Changes != nil {
		changes, ok := mapChanges(w, req.Changes)
		if !ok {
			return service.IngestCommand{}, false
		}
		cmd.Changes = changes
	}
	return cmd, true
}

var changeOps = map[string]diff.Op{
	string(diff.OpAdd):    diff.OpAdd,
	string(diff.OpChange): diff.OpChange,
	string(diff.OpRemove): diff.OpRemove,
}

func mapChanges(w http.ResponseWriter, in []changeInput) ([]diff.Change, bool) {
	out, err := mapChangesE(in)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	return out, true
}

// mapChangesE is the error-returning core of mapChanges, shared with the batch
// path, which collects per-item rejects rather than writing a response.
func mapChangesE(in []changeInput) ([]diff.Change, error) {
	out := make([]diff.Change, len(in))
	for i, c := range in {
		op, ok := changeOps[c.Op]
		if !ok {
			return nil, errors.New("invalid change op " + c.Op + " (want add, change or remove)")
		}
		out[i] = diff.Change{Path: c.Path, Op: op, Old: c.Old, New: c.New}
	}
	return out, nil
}

// parseOp validates the entity-level op. The empty string is allowed: the
// use-case derives the op (first_event_op or update) when the client omits it.
func parseOp(w http.ResponseWriter, op string) (domain.EntityOp, bool) {
	parsed, err := parseOpE(op)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return "", false
	}
	return parsed, true
}

// parseOpE is the error-returning core of parseOp, shared with the batch path.
func parseOpE(op string) (domain.EntityOp, error) {
	switch domain.EntityOp(op) {
	case "", domain.OpCreate, domain.OpUpdate, domain.OpDelete:
		return domain.EntityOp(op), nil
	default:
		return "", errors.New("invalid op " + op + " (want create, update or delete)")
	}
}

// parseMode reads the per-request reliability override. An absent header defers
// to the collection default; an unknown value is 400 before any work happens.
func parseMode(w http.ResponseWriter, r *http.Request) (domain.ReliabilityMode, bool) {
	v := r.Header.Get(modeHeader)
	if v == "" {
		return "", true
	}
	m := domain.ReliabilityMode(v)
	if !domain.ValidReliabilityMode(m) {
		writeError(w, http.StatusBadRequest, "invalid "+modeHeader+" (want strict, durable or fast)")
		return "", false
	}
	return m, true
}

func writeIngestError(w http.ResponseWriter, err error) {
	var bp *service.BackpressureError
	var fc *plugin.FailClosedError
	switch {
	case errors.Is(err, service.ErrAutoCreateDisabled):
		writeError(w, http.StatusNotFound, "collection does not exist")
	case errors.Is(err, domain.ErrVersionConflict):
		writeError(w, http.StatusConflict, "version conflict")
	case errors.Is(err, service.ErrInvalidDiff):
		writeError(w, http.StatusBadRequest, "diff does not apply to current state")
	case errors.As(err, &fc):
		// A fail-closed pre-store plugin rejected the write — not a client
		// validation error, not an internal 500; the event could not be processed.
		writeError(w, http.StatusUnprocessableEntity, "write rejected by a plugin")
	case errors.As(err, &bp):
		// Queue at capacity or Mongo down: refuse rather than grow the queue unbounded.
		writeRetryAfter(w, bp.RetryAfter)
		writeError(w, http.StatusTooManyRequests, "queue at capacity, retry later")
	case errors.Is(err, queue.ErrQueueUnavailable):
		writeError(w, http.StatusServiceUnavailable, "queue unavailable, retry later")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// writeRetryAfter sets the Retry-After header, rounded up so a sub-second
// hint never collapses to zero.
func writeRetryAfter(w http.ResponseWriter, d time.Duration) {
	if d <= 0 {
		return
	}
	secs := int64((d + time.Second - 1) / time.Second)
	w.Header().Set("Retry-After", strconv.FormatInt(secs, 10))
}

// readLimited reads the body up to max bytes. Reading one extra byte distinguishes
// "exactly at limit" from "over limit".
func readLimited(w http.ResponseWriter, r *http.Request, max int64) ([]byte, bool) {
	if max <= 0 {
		max = domain.DefaultMaxEventSizeBytes
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, max+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "cannot read request body")
		return nil, false
	}
	if int64(len(body)) > max {
		writeError(w, http.StatusRequestEntityTooLarge, "event exceeds max_event_size_bytes")
		return nil, false
	}
	return body, true
}

func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

// collectionNameRE mirrors the storage naming rule. A 400 here prevents a 500
// from storage; storage validates again for defence in depth.
var collectionNameRE = regexp.MustCompile(`^[a-z0-9]+(?:\.[a-z0-9]+)*$`)

func validCollectionName(name string) bool {
	return collectionNameRE.MatchString(name)
}

// canAccess enforces the key's collection mask: the collection comes from the
// path, the principal from the context, and a mismatch is 403.
func canAccess(w http.ResponseWriter, r *http.Request, collection string) bool {
	p, ok := tenant.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return false
	}
	if !p.CanAccess(collection) {
		writeError(w, http.StatusForbidden, "collection not allowed")
		return false
	}
	return true
}

// pathParams extracts and validates the collection and entity from the path.
// The collection name is checked here so a bad name is a 400 rather than a
// storage error.
func pathParams(w http.ResponseWriter, r *http.Request) (string, string, bool) {
	collection := chi.URLParam(r, "collection")
	entityID := chi.URLParam(r, "entityId")
	if !validCollectionName(collection) {
		writeError(w, http.StatusBadRequest, "invalid collection name")
		return "", "", false
	}
	if entityID == "" {
		writeError(w, http.StatusBadRequest, "entity_id is required")
		return "", "", false
	}
	return collection, entityID, true
}
