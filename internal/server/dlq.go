package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

type DLQService interface {
	List(ctx context.Context, ruleID string, limit int, after *domain.DLQCursor) ([]domain.DeadLetter, error)
	Redeliver(ctx context.Context, ruleID string, ids []string) (int, error)
}

type dlqHandlers struct {
	svc DLQService
}

// guard resolves the principal and enforces the collection mask, returning the
// collection and rule id from the path. DLQ routes are admin-scoped at the
// router (a dead letter body may carry PII); the per-collection mask is checked
// here, like the rule handlers.
func (h dlqHandlers) guard(w http.ResponseWriter, r *http.Request) (collection, ruleID string, ok bool) {
	p, found := tenant.FromContext(r.Context())
	if !found {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return "", "", false
	}
	collection = chi.URLParam(r, "collection")
	if !p.CanAccess(collection) {
		writeError(w, http.StatusForbidden, "collection not allowed")
		return "", "", false
	}
	return collection, chi.URLParam(r, "ruleId"), true
}

// list serves GET …/dlq → {items:[...], next_cursor}. The page is newest-first;
// next_cursor is set only when the page is full, so a client stops at null.
func (h dlqHandlers) list(w http.ResponseWriter, r *http.Request) {
	_, ruleID, ok := h.guard(w, r)
	if !ok {
		return
	}
	limit, after, ok := parseDLQQuery(w, r)
	if !ok {
		return
	}

	items, err := h.svc.List(r.Context(), ruleID, limit, after)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	out := make([]map[string]any, 0, len(items))
	for _, dl := range items {
		out = append(out, deadLetterDTO(dl))
	}
	body := map[string]any{"items": out, "next_cursor": nil}
	if limit > 0 && len(items) == limit {
		last := items[len(items)-1]
		body["next_cursor"] = encodeDLQCursor(domain.DLQCursor{FailedAt: last.FailedAt, ID: last.ID})
	}
	writeJSON(w, http.StatusOK, body)
}

// redeliver serves POST …/dlq:redeliver → 202 {requeued}. The optional body
// {ids:[...]} narrows the request to specific entries; absent, every dead letter
// of the rule is redelivered. An unknown id is 404.
func (h dlqHandlers) redeliver(w http.ResponseWriter, r *http.Request) {
	_, ruleID, ok := h.guard(w, r)
	if !ok {
		return
	}
	ids, ok := parseRedeliverBody(w, r)
	if !ok {
		return
	}

	requeued, err := h.svc.Redeliver(r.Context(), ruleID, ids)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "dead letter not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"requeued": requeued})
}

// parseDLQQuery reads the limit and cursor. An invalid value is 400; the limit
// defaults and is capped like the history listing.
func parseDLQQuery(w http.ResponseWriter, r *http.Request) (int, *domain.DLQCursor, bool) {
	q := r.URL.Query()
	limit := defaultHistoryLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return 0, nil, false
		}
		limit = min(n, maxHistoryLimit)
	}
	var after *domain.DLQCursor
	if c := q.Get("cursor"); c != "" {
		cur, err := decodeDLQCursor(c)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return 0, nil, false
		}
		after = &cur
	}
	return limit, after, true
}

// parseRedeliverBody reads the optional {ids:[...]} body. An empty or absent body
// means "redeliver all" and is valid; a malformed body is 400.
func parseRedeliverBody(w http.ResponseWriter, r *http.Request) ([]string, bool) {
	if r.Body == nil || r.ContentLength == 0 {
		return nil, true
	}
	var body struct {
		IDs []string `json:"ids"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, true // empty body: redeliver all
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return nil, false
	}
	return body.IDs, true
}

// deadLetterDTO renders a dead letter to the wire shape. The body is the exact
// payload that was to be POSTed; embedded as raw JSON, not re-encoded.
func deadLetterDTO(dl domain.DeadLetter) map[string]any {
	m := map[string]any{
		"id":          dl.ID,
		"rule_id":     dl.RuleID,
		"collection":  dl.Collection,
		"delivery_id": dl.DeliveryID,
		"url":         dl.URL,
		"attempts":    dl.Attempts,
		"failed_at":   dl.FailedAt,
	}
	if dl.SecretRef != "" {
		m["secret_ref"] = dl.SecretRef
	}
	if dl.LastError != "" {
		m["last_error"] = dl.LastError
	}
	if len(dl.Body) > 0 {
		m["body"] = json.RawMessage(dl.Body)
	}
	return m
}

// dlqCursorPayload is the opaque cursor form of a DLQ position (base64 JSON).
type dlqCursorPayload struct {
	FailedAt time.Time `json:"f"`
	ID       string    `json:"id"`
}

func encodeDLQCursor(c domain.DLQCursor) string {
	b, _ := json.Marshal(dlqCursorPayload{FailedAt: c.FailedAt, ID: c.ID})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeDLQCursor(s string) (domain.DLQCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return domain.DLQCursor{}, err
	}
	var p dlqCursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return domain.DLQCursor{}, err
	}
	return domain.DLQCursor{FailedAt: p.FailedAt, ID: p.ID}, nil
}
