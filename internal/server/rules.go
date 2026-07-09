package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/rules"
	"github.com/max-trifonov/letopis/internal/tenant"
)

type RuleService interface {
	Create(ctx context.Context, collection string, r *rules.Rule) (*rules.Rule, error)
	Get(ctx context.Context, collection, id string) (*rules.Rule, error)
	List(ctx context.Context, collection string) ([]rules.Rule, error)
	Update(ctx context.Context, collection, id string, r *rules.Rule) (*rules.Rule, error)
	Delete(ctx context.Context, collection, id string) error
}

type ruleHandlers struct {
	svc RuleService
}

// guard resolves the principal and enforces the collection mask, returning the
// collection name on success. Every rule handler is admin-scoped at the router;
// the mask is per-collection and is checked here.
func (h ruleHandlers) guard(w http.ResponseWriter, r *http.Request) (string, bool) {
	p, ok := tenant.FromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing API key")
		return "", false
	}
	collection := chi.URLParam(r, "collection")
	if !p.CanAccess(collection) {
		writeError(w, http.StatusForbidden, "collection not allowed")
		return "", false
	}
	return collection, true
}

func (h ruleHandlers) create(w http.ResponseWriter, r *http.Request) {
	collection, ok := h.guard(w, r)
	if !ok {
		return
	}
	rule, err := decodeRuleRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	created, err := h.svc.Create(r.Context(), collection, rule)
	if err != nil {
		writeRuleError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"rule": ruleResponse(created)})
}

func (h ruleHandlers) list(w http.ResponseWriter, r *http.Request) {
	collection, ok := h.guard(w, r)
	if !ok {
		return
	}
	rs, err := h.svc.List(r.Context(), collection)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	out := make([]map[string]any, 0, len(rs))
	for i := range rs {
		out = append(out, ruleResponse(&rs[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"rules": out})
}

func (h ruleHandlers) get(w http.ResponseWriter, r *http.Request) {
	collection, ok := h.guard(w, r)
	if !ok {
		return
	}
	rule, err := h.svc.Get(r.Context(), collection, chi.URLParam(r, "ruleId"))
	if err != nil {
		writeRuleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rule": ruleResponse(rule)})
}

func (h ruleHandlers) update(w http.ResponseWriter, r *http.Request) {
	collection, ok := h.guard(w, r)
	if !ok {
		return
	}
	rule, err := decodeRuleRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	updated, err := h.svc.Update(r.Context(), collection, chi.URLParam(r, "ruleId"), rule)
	if err != nil {
		writeRuleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"rule": ruleResponse(updated)})
}

func (h ruleHandlers) del(w http.ResponseWriter, r *http.Request) {
	collection, ok := h.guard(w, r)
	if !ok {
		return
	}
	if err := h.svc.Delete(r.Context(), collection, chi.URLParam(r, "ruleId")); err != nil {
		writeRuleError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeRuleError maps service errors to status codes: validation error → 400,
// name clash → 409, unknown id → 404, anything else → 500.
func writeRuleError(w http.ResponseWriter, err error) {
	var re *rules.RuleError
	switch {
	case errors.As(err, &re):
		writeError(w, http.StatusBadRequest, re.Error())
	case errors.Is(err, domain.ErrRuleNameConflict):
		writeError(w, http.StatusConflict, "rule name already exists in collection")
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "rule not found")
	default:
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

// --- wire DTOs ---

// ruleRequest is the POST/PUT body. id and version are server-assigned and
// ignored on input.
type ruleRequest struct {
	Name      string       `json:"name"`
	Enabled   bool         `json:"enabled"`
	Condition conditionDTO `json:"condition"`
	Actions   []actionDTO  `json:"actions"`
}

// conditionDTO is one condition node on the wire: a combinator (all/any/not), a
// scalar leaf (field + one operator key), or a change match. Operator keys use
// json.RawMessage so a present-but-null value is distinguishable from absent.
type conditionDTO struct {
	All []conditionDTO `json:"all,omitempty"`
	Any []conditionDTO `json:"any,omitempty"`
	Not *conditionDTO  `json:"not,omitempty"`

	Field  string          `json:"field,omitempty"`
	Eq     json.RawMessage `json:"eq,omitempty"`
	Ne     json.RawMessage `json:"ne,omitempty"`
	In     json.RawMessage `json:"in,omitempty"`
	Gt     json.RawMessage `json:"gt,omitempty"`
	Gte    json.RawMessage `json:"gte,omitempty"`
	Lt     json.RawMessage `json:"lt,omitempty"`
	Lte    json.RawMessage `json:"lte,omitempty"`
	Regex  json.RawMessage `json:"regex,omitempty"`
	Exists json.RawMessage `json:"exists,omitempty"`

	Match *matchDTO `json:"match,omitempty"`
}

type matchDTO struct {
	Path string          `json:"path"`
	Op   string          `json:"op,omitempty"`
	Old  json.RawMessage `json:"old,omitempty"`
	New  json.RawMessage `json:"new,omitempty"`
}

type actionDTO struct {
	Type string `json:"type"`
	// webhook fields (flat)
	URL       string    `json:"url,omitempty"`
	SecretRef string    `json:"secret_ref,omitempty"`
	TimeoutMS int       `json:"timeout_ms,omitempty"`
	Retry     *retryDTO `json:"retry,omitempty"`
	// log field
	Level string `json:"level,omitempty"`
}

type retryDTO struct {
	MaxAttempts int    `json:"max_attempts,omitempty"`
	Backoff     string `json:"backoff,omitempty"`
}

// decodeRuleRequest parses the body and maps it to a domain rule. Unknown fields
// are rejected so a misspelled operator is 400, not silently ignored. Structural
// validity (compilable regex, non-empty actions) is the service's job.
func decodeRuleRequest(r *http.Request) (*rules.Rule, error) {
	var body ruleRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		return nil, errors.New("invalid JSON body")
	}
	cond, err := toCondition(body.Condition)
	if err != nil {
		return nil, err
	}
	actions, err := toActions(body.Actions)
	if err != nil {
		return nil, err
	}
	return &rules.Rule{
		Name:      body.Name,
		Enabled:   body.Enabled,
		Condition: cond,
		Actions:   actions,
	}, nil
}

// toCondition maps a wire condition node to the domain tree. A node that sets
// nothing maps to an empty Condition, which rules.Compile rejects with a located
// error — keeping the kind-validation rules in one place.
func toCondition(d conditionDTO) (rules.Condition, error) {
	switch {
	case d.All != nil:
		subs, err := toConditions(d.All)
		return rules.Condition{All: subs}, err
	case d.Any != nil:
		subs, err := toConditions(d.Any)
		return rules.Condition{Any: subs}, err
	case d.Not != nil:
		inner, err := toCondition(*d.Not)
		if err != nil {
			return rules.Condition{}, err
		}
		return rules.Condition{Not: &inner}, nil
	case d.Match != nil:
		m, err := toMatch(*d.Match)
		if err != nil {
			return rules.Condition{}, err
		}
		return rules.Condition{Match: &m}, nil
	default:
		return toScalar(d)
	}
}

func toConditions(ds []conditionDTO) ([]rules.Condition, error) {
	out := make([]rules.Condition, len(ds))
	for i, d := range ds {
		c, err := toCondition(d)
		if err != nil {
			return nil, err
		}
		out[i] = c
	}
	return out, nil
}

// scalarOps maps each operator key to its rules.Op. exists is value-less; the
// rest carry a comparison value decoded from raw JSON.
var scalarOps = []struct {
	op  rules.Op
	get func(conditionDTO) json.RawMessage
}{
	{rules.OpEq, func(d conditionDTO) json.RawMessage { return d.Eq }},
	{rules.OpNe, func(d conditionDTO) json.RawMessage { return d.Ne }},
	{rules.OpIn, func(d conditionDTO) json.RawMessage { return d.In }},
	{rules.OpGt, func(d conditionDTO) json.RawMessage { return d.Gt }},
	{rules.OpGte, func(d conditionDTO) json.RawMessage { return d.Gte }},
	{rules.OpLt, func(d conditionDTO) json.RawMessage { return d.Lt }},
	{rules.OpLte, func(d conditionDTO) json.RawMessage { return d.Lte }},
	{rules.OpRegex, func(d conditionDTO) json.RawMessage { return d.Regex }},
	{rules.OpExists, func(d conditionDTO) json.RawMessage { return d.Exists }},
}

func toScalar(d conditionDTO) (rules.Condition, error) {
	c := rules.Condition{Field: rules.Field(d.Field)}
	for _, so := range scalarOps {
		raw := so.get(d)
		if raw == nil {
			continue
		}
		c.Op = so.op
		if so.op != rules.OpExists {
			v, err := decodeAny(raw)
			if err != nil {
				return rules.Condition{}, err
			}
			c.Value = v
		}
		return c, nil
	}
	// No operator set; Compile will report the precise reason.
	return c, nil
}

func toMatch(d matchDTO) (rules.Match, error) {
	m := rules.Match{Path: d.Path, Op: diff.Op(d.Op)}
	if d.Old != nil {
		v, err := decodeAny(d.Old)
		if err != nil {
			return rules.Match{}, err
		}
		m.Old, m.HasOld = v, true
	}
	if d.New != nil {
		v, err := decodeAny(d.New)
		if err != nil {
			return rules.Match{}, err
		}
		m.New, m.HasNew = v, true
	}
	return m, nil
}

func toActions(ds []actionDTO) ([]rules.Action, error) {
	out := make([]rules.Action, len(ds))
	for i, d := range ds {
		a := rules.Action{Type: rules.ActionType(d.Type)}
		switch d.Type {
		case string(rules.ActionWebhook):
			w := &rules.Webhook{URL: d.URL, SecretRef: d.SecretRef, TimeoutMS: d.TimeoutMS}
			if d.Retry != nil {
				w.Retry = rules.Retry{MaxAttempts: d.Retry.MaxAttempts, Backoff: d.Retry.Backoff}
			}
			a.Webhook = w
		case string(rules.ActionLog):
			a.Log = &rules.LogAction{Level: d.Level}
		}
		// An unknown type round-trips as-is and is rejected by the validator.
		out[i] = a
	}
	return out, nil
}

func decodeAny(raw json.RawMessage) (any, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, errors.New("invalid condition value")
	}
	return v, nil
}

// ruleResponse renders a domain rule to the wire shape. The condition is mapped
// back to the operator-as-key form so Create→Get round-trips correctly.
func ruleResponse(r *rules.Rule) map[string]any {
	out := map[string]any{
		"id":        r.ID,
		"name":      r.Name,
		"enabled":   r.Enabled,
		"version":   r.Version,
		"condition": conditionResponse(r.Condition),
		"actions":   actionsResponse(r.Actions),
	}
	if !r.UpdatedAt.IsZero() {
		out["updated_at"] = r.UpdatedAt
	}
	return out
}

func conditionResponse(c rules.Condition) map[string]any {
	switch {
	case c.All != nil:
		return map[string]any{"all": conditionsResponse(c.All)}
	case c.Any != nil:
		return map[string]any{"any": conditionsResponse(c.Any)}
	case c.Not != nil:
		return map[string]any{"not": conditionResponse(*c.Not)}
	case c.Match != nil:
		return map[string]any{"field": "changes", "match": matchResponse(*c.Match)}
	default:
		out := map[string]any{"field": string(c.Field)}
		if c.Op == rules.OpExists {
			out["exists"] = true
		} else if c.Op != "" {
			out[string(c.Op)] = c.Value
		}
		return out
	}
}

func conditionsResponse(cs []rules.Condition) []map[string]any {
	out := make([]map[string]any, len(cs))
	for i, c := range cs {
		out[i] = conditionResponse(c)
	}
	return out
}

func matchResponse(m rules.Match) map[string]any {
	out := map[string]any{"path": m.Path}
	if m.Op != "" {
		out["op"] = string(m.Op)
	}
	if m.HasOld {
		out["old"] = m.Old
	}
	if m.HasNew {
		out["new"] = m.New
	}
	return out
}

func actionsResponse(actions []rules.Action) []map[string]any {
	out := make([]map[string]any, len(actions))
	for i, a := range actions {
		m := map[string]any{"type": string(a.Type)}
		if a.Webhook != nil {
			m["url"] = a.Webhook.URL
			if a.Webhook.SecretRef != "" {
				m["secret_ref"] = a.Webhook.SecretRef
			}
			if a.Webhook.TimeoutMS > 0 {
				m["timeout_ms"] = a.Webhook.TimeoutMS
			}
			if a.Webhook.Retry != (rules.Retry{}) {
				m["retry"] = map[string]any{"max_attempts": a.Webhook.Retry.MaxAttempts, "backoff": a.Webhook.Retry.Backoff}
			}
		}
		if a.Log != nil {
			m["level"] = a.Log.Level
		}
		out[i] = m
	}
	return out
}
