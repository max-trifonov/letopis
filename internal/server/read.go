package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/service"
)

// ReadService is the transport's view of the read use-case.
type ReadService interface {
	History(ctx context.Context, collection string, f domain.EventFilter) ([]domain.Event, error)
	CurrentState(ctx context.Context, collection, entityID string) (*domain.CurrentState, error)
	StateAt(ctx context.Context, collection, entityID string, q service.PointInTimeQuery) (*service.ReconstructedState, error)
}

type readHandlers struct {
	svc ReadService
}

const (
	defaultHistoryLimit = 100
	maxHistoryLimit     = 1000
)

func (h readHandlers) history(w http.ResponseWriter, r *http.Request) {
	collection, entityID, ok := pathParams(w, r)
	if !ok {
		return
	}
	if !canAccess(w, r, collection) {
		return
	}
	f, format, ok := parseHistoryQuery(w, r, entityID)
	if !ok {
		return
	}

	events, err := h.svc.History(r.Context(), collection, f)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	body, ok := historyBody(w, entityID, events, f, format)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, body)
}

func (h readHandlers) state(w http.ResponseWriter, r *http.Request) {
	collection, entityID, ok := pathParams(w, r)
	if !ok {
		return
	}
	if !canAccess(w, r, collection) {
		return
	}

	q, pointInTime, ok := parseStateQuery(w, r)
	if !ok {
		return
	}
	if pointInTime {
		h.stateAt(w, r, collection, entityID, q)
		return
	}

	st, err := h.svc.CurrentState(r.Context(), collection, entityID)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "entity not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entity_id": st.EntityID,
		"version":   st.Version,
		"ts":        st.TS,
		"deleted":   st.Deleted,
		"state":     st.State,
	})
}

// stateAt serves the point-in-time branch: reconstruct via the nearest snapshot
// plus the event tail and report which snapshot anchored it.
func (h readHandlers) stateAt(w http.ResponseWriter, r *http.Request, collection, entityID string, q service.PointInTimeQuery) {
	rs, err := h.svc.StateAt(r.Context(), collection, entityID, q)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			writeError(w, http.StatusNotFound, "entity not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	// snapshot_version is null when the read replayed from genesis, so a client
	// can tell an anchored reconstruction from a full replay.
	var snapVersion any
	if rs.SnapshotVersion > 0 {
		snapVersion = rs.SnapshotVersion
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"entity_id": rs.EntityID,
		"version":   rs.Version,
		"ts":        rs.TS,
		"deleted":   rs.Deleted,
		"state":     rs.State,
		"reconstructed_from": map[string]any{
			"snapshot_version": snapVersion,
			"events_applied":   rs.EventsApplied,
		},
	})
}

// parseStateQuery reads the point-in-time parameters: ?version or ?at (mutually
// exclusive). Returns pointInTime=false when neither is present (current state).
// ?at_source is reserved for the source-ordering mode and rejected until it ships.
func parseStateQuery(w http.ResponseWriter, r *http.Request) (service.PointInTimeQuery, bool, bool) {
	q := r.URL.Query()
	versionRaw, hasVersion := q.Get("version"), q.Has("version")
	atRaw, hasAt := q.Get("at"), q.Has("at")

	if q.Has("at_source") {
		writeError(w, http.StatusBadRequest, "at_source is not supported (reserved for ordering: source)")
		return service.PointInTimeQuery{}, false, false
	}
	if hasVersion && hasAt {
		writeError(w, http.StatusBadRequest, "version and at are mutually exclusive")
		return service.PointInTimeQuery{}, false, false
	}

	switch {
	case hasVersion:
		v, err := strconv.ParseInt(versionRaw, 10, 64)
		if err != nil || v < 1 {
			writeError(w, http.StatusBadRequest, "invalid version (want integer ≥ 1)")
			return service.PointInTimeQuery{}, false, false
		}
		return service.PointInTimeQuery{Version: &v}, true, true
	case hasAt:
		ts, err := time.Parse(time.RFC3339, atRaw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid at (want RFC3339)")
			return service.PointInTimeQuery{}, false, false
		}
		return service.PointInTimeQuery{At: &ts}, true, true
	default:
		return service.PointInTimeQuery{}, false, true
	}
}

// parseHistoryQuery validates the query string into a domain filter and the
// response format. Invalid enums, dates, limits, or cursors are 400.
func parseHistoryQuery(w http.ResponseWriter, r *http.Request, entityID string) (domain.EventFilter, string, bool) {
	q := r.URL.Query()
	f := domain.EventFilter{
		EntityID: entityID,
		Author:   q.Get("author_id"),
		Path:     q.Get("path"),
		Source:   q.Get("source"),
		Order:    domain.OrderDesc, // API default; overridden below
		Limit:    defaultHistoryLimit,
	}

	orderBy, ok := parseOrderBy(w, q.Get("order_by"))
	if !ok {
		return f, "", false
	}
	f.OrderBy = orderBy

	if order := q.Get("order"); order != "" {
		switch order {
		case "asc":
			f.Order = domain.OrderAsc
		case "desc":
			f.Order = domain.OrderDesc
		default:
			writeError(w, http.StatusBadRequest, "invalid order (want asc or desc)")
			return f, "", false
		}
	}

	if op := q.Get("op"); op != "" {
		parsed, valid := parseOp(w, op)
		if !valid {
			return f, "", false
		}
		f.Op = parsed
	}

	for _, tf := range []struct {
		key string
		dst *time.Time
	}{{"from", &f.From}, {"to", &f.To}} {
		if v := q.Get(tf.key); v != "" {
			ts, err := time.Parse(time.RFC3339, v)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid "+tf.key+" (want RFC3339)")
				return f, "", false
			}
			*tf.dst = ts
		}
	}

	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "invalid limit")
			return f, "", false
		}
		f.Limit = min(n, maxHistoryLimit)
	}

	if c := q.Get("cursor"); c != "" {
		pos, err := decodeCursor(c)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid cursor")
			return f, "", false
		}
		f.After = &pos
	}

	format := q.Get("format")
	switch format {
	case "", "native":
		format = "native"
	case "json-patch":
	default:
		writeError(w, http.StatusBadRequest, "invalid format (want native or json-patch)")
		return f, "", false
	}

	return f, format, true
}

func parseOrderBy(w http.ResponseWriter, v string) (domain.OrderField, bool) {
	switch v {
	case "", "version":
		return domain.OrderVersion, true
	case "ts_source":
		return domain.OrderSource, true
	case "ts_received":
		return domain.OrderReceived, true
	default:
		writeError(w, http.StatusBadRequest, "invalid order_by (want version, ts_source or ts_received)")
		return "", false
	}
}

// historyBody assembles the response, including next_cursor (set only when the
// page is full, so a client stops when it sees null) and the per-event change
// projection in the requested format.
func historyBody(w http.ResponseWriter, entityID string, events []domain.Event, f domain.EventFilter, format string) (map[string]any, bool) {
	out := make([]map[string]any, 0, len(events))
	for _, ev := range events {
		changes, ok := projectChanges(ev.Changes, format)
		if !ok {
			writeError(w, http.StatusBadRequest, "collection uses keyed arrays; not exportable to json-patch")
			return nil, false
		}
		out = append(out, eventToDTO(ev, changes))
	}

	body := map[string]any{
		"entity_id":   entityID,
		"events":      out,
		"next_cursor": nil,
	}
	if f.Limit > 0 && len(events) == f.Limit {
		body["next_cursor"] = encodeCursor(positionOf(events[len(events)-1], f.OrderBy))
	}
	return body, true
}

func eventToDTO(ev domain.Event, changes []any) map[string]any {
	m := map[string]any{
		"version":     ev.Version,
		"op":          string(ev.Op),
		"ts_received": ev.TSReceived,
		"ts_stored":   ev.TSStored,
		"changes":     changes,
	}
	if ev.Author != "" {
		m["author_id"] = ev.Author
	}
	if ev.Source != "" {
		m["source"] = ev.Source
	}
	if !ev.TSSource.IsZero() {
		m["ts_source"] = ev.TSSource
	}
	if ev.Meta != nil {
		m["meta"] = ev.Meta
	}
	// The integrity block is absent for collections without the hash-chain plugin
	// or for events written before it was enabled.
	if ev.Integrity != nil {
		intg := map[string]any{"hash": ev.Integrity.Hash}
		if ev.Integrity.PrevHash != "" {
			intg["prev_hash"] = ev.Integrity.PrevHash
		}
		m["integrity"] = intg
	}
	return m
}

// projectChanges renders the change list in the requested format. native keeps
// the audit-friendly old/new shape; json-patch exports RFC 6902, which cannot
// express keyed-array paths — that case returns false.
func projectChanges(changes []diff.Change, format string) ([]any, bool) {
	if format == "json-patch" {
		ops, err := diff.ToJSONPatch(changes)
		if err != nil {
			return nil, false
		}
		out := make([]any, len(ops))
		for i, op := range ops {
			out[i] = op
		}
		return out, true
	}
	out := make([]any, len(changes))
	for i, c := range changes {
		m := map[string]any{"path": c.Path, "op": string(c.Op)}
		switch c.Op {
		case diff.OpAdd:
			m["new"] = c.New
		case diff.OpRemove:
			m["old"] = c.Old
		default:
			m["old"], m["new"] = c.Old, c.New
		}
		out[i] = m
	}
	return out, true
}

// positionOf is the cursor key for an event under the active ordering. Version
// is always carried; it breaks ties when a timestamp repeats.
func positionOf(ev domain.Event, orderBy domain.OrderField) domain.Position {
	switch orderBy {
	case domain.OrderSource:
		return domain.Position{Version: ev.Version, TS: ev.TSSource}
	case domain.OrderReceived:
		return domain.Position{Version: ev.Version, TS: ev.TSReceived}
	default:
		return domain.Position{Version: ev.Version}
	}
}

// cursorPayload is the wire form of a Position. The cursor is opaque to clients
// (base64 JSON), with no structure they can rely on.
type cursorPayload struct {
	V  int64     `json:"v"`
	TS time.Time `json:"ts,omitempty"`
}

func encodeCursor(p domain.Position) string {
	b, _ := json.Marshal(cursorPayload{V: p.Version, TS: p.TS})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (domain.Position, error) {
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return domain.Position{}, err
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return domain.Position{}, err
	}
	return domain.Position{Version: p.V, TS: p.TS}, nil
}
