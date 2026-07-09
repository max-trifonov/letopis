package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/rules"
	"github.com/max-trifonov/letopis/internal/service"
)

// fakeRuleRepo is an in-memory domain.RuleRepository so the rule acceptance
// tests drive the real RuleService (validation, audit-free, name conflicts)
// without a Mongo. It keys rules by id and tracks names per collection for the
// 409 path.
type fakeRuleRepo struct {
	byID  map[string]*rules.Rule
	names map[string]string // collection|name -> id
	seq   int
}

func newFakeRuleRepo() *fakeRuleRepo {
	return &fakeRuleRepo{byID: map[string]*rules.Rule{}, names: map[string]string{}}
}

func (f *fakeRuleRepo) nameKey(c, n string) string { return c + "|" + n }

func (f *fakeRuleRepo) Create(_ context.Context, collection string, r *rules.Rule) error {
	if _, ok := f.names[f.nameKey(collection, r.Name)]; ok {
		return domain.ErrRuleNameConflict
	}
	f.seq++
	r.ID = "rule_test" + string(rune('0'+f.seq))
	r.Version = 1
	cp := *r
	f.byID[r.ID] = &cp
	f.names[f.nameKey(collection, r.Name)] = r.ID
	return nil
}

func (f *fakeRuleRepo) Get(_ context.Context, _ string, id string) (*rules.Rule, error) {
	r, ok := f.byID[id]
	if !ok {
		return nil, domain.ErrNotFound
	}
	cp := *r
	return &cp, nil
}

func (f *fakeRuleRepo) List(_ context.Context, _ string) ([]rules.Rule, error) {
	out := []rules.Rule{}
	for _, r := range f.byID {
		out = append(out, *r)
	}
	return out, nil
}

func (f *fakeRuleRepo) Update(_ context.Context, _ string, r *rules.Rule) error {
	cur, ok := f.byID[r.ID]
	if !ok {
		return domain.ErrNotFound
	}
	r.Version = cur.Version + 1
	cp := *r
	f.byID[r.ID] = &cp
	return nil
}

func (f *fakeRuleRepo) Delete(_ context.Context, _ string, id string) error {
	if _, ok := f.byID[id]; !ok {
		return domain.ErrNotFound
	}
	delete(f.byID, id)
	return nil
}

func (f *fakeRuleRepo) ListAll(_ context.Context) (map[string][]rules.Rule, error) {
	return nil, nil
}

func ruleRouter(t *testing.T, repo domain.RuleRepository) http.Handler {
	t.Helper()
	svc := service.NewRuleService(repo, nil, nil, nil)
	return newRouter(health.NewRegistry(), adminResolver(t), nil, nil, nil, nil, nil, nil, nil, svc, nil)
}

const rulesPath = "/api/v1/collections/crm.deals/rules"

// sampleRuleBody is a valid rule covering combinators, a scalar leaf, a change
// match and both action kinds — the same shape as API §4.
const sampleRuleBody = `{
  "name": "alert",
  "enabled": true,
  "condition": {"all": [
    {"field": "op", "eq": "update"},
    {"field": "changes", "match": {"path": "status", "old": "active", "new": "cancelled"}},
    {"any": [{"field": "author_id", "in": ["42", "77"]}]}
  ]},
  "actions": [
    {"type": "webhook", "url": "https://crm.example.com/hooks/hm", "secret_ref": "whsec_1", "timeout_ms": 5000, "retry": {"max_attempts": 8, "backoff": "exponential"}},
    {"type": "log", "level": "warn"}
  ]
}`

func TestRuleCreateAndGet(t *testing.T) {
	h := ruleRouter(t, newFakeRuleRepo())

	rec := post(t, h, rulesPath, "Bearer key-admin", sampleRuleBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create status = %d, want 201; body=%s", rec.Code, rec.Body.String())
	}
	var created struct {
		Rule map[string]any `json:"rule"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("decode: %v", err)
	}
	id, _ := created.Rule["id"].(string)
	if id == "" || created.Rule["version"] != float64(1) {
		t.Fatalf("create did not assign id/version: %+v", created.Rule)
	}
	// The condition round-trips back to the operator-as-key form.
	cond, _ := created.Rule["condition"].(map[string]any)
	if _, ok := cond["all"]; !ok {
		t.Fatalf("condition lost its all combinator: %+v", cond)
	}

	rec = do(t, h, http.MethodGet, rulesPath+"/"+id, "Bearer key-admin")
	if rec.Code != http.StatusOK {
		t.Fatalf("get status = %d, want 200", rec.Code)
	}
}

func TestRuleListUpdateDelete(t *testing.T) {
	repo := newFakeRuleRepo()
	h := ruleRouter(t, repo)

	rec := post(t, h, rulesPath, "Bearer key-admin", sampleRuleBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("seed create = %d", rec.Code)
	}
	var created struct {
		Rule map[string]any `json:"rule"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &created)
	id := created.Rule["id"].(string)

	// List returns the one rule.
	rec = do(t, h, http.MethodGet, rulesPath, "Bearer key-admin")
	var listed struct {
		Rules []map[string]any `json:"rules"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &listed)
	if len(listed.Rules) != 1 {
		t.Fatalf("list = %d rules, want 1", len(listed.Rules))
	}

	// Update bumps the version.
	updated := strings.Replace(sampleRuleBody, `"enabled": true`, `"enabled": false`, 1)
	rec = put(t, h, rulesPath+"/"+id, "Bearer key-admin", updated)
	if rec.Code != http.StatusOK {
		t.Fatalf("update status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	var up struct {
		Rule map[string]any `json:"rule"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &up)
	if up.Rule["version"] != float64(2) || up.Rule["enabled"] != false {
		t.Fatalf("update body = %+v", up.Rule)
	}

	// Delete → 204, then Get → 404.
	rec = do(t, h, http.MethodDelete, rulesPath+"/"+id, "Bearer key-admin")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", rec.Code)
	}
	rec = do(t, h, http.MethodGet, rulesPath+"/"+id, "Bearer key-admin")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("get after delete = %d, want 404", rec.Code)
	}
}

func TestRuleDuplicateNameIs409(t *testing.T) {
	h := ruleRouter(t, newFakeRuleRepo())
	if rec := post(t, h, rulesPath, "Bearer key-admin", sampleRuleBody); rec.Code != http.StatusCreated {
		t.Fatalf("first create = %d", rec.Code)
	}
	rec := post(t, h, rulesPath, "Bearer key-admin", sampleRuleBody)
	if rec.Code != http.StatusConflict {
		t.Fatalf("duplicate name status = %d, want 409", rec.Code)
	}
}

// Invalid conditions/actions are rejected at save with 400 carrying the field.
func TestRuleInvalidIs400(t *testing.T) {
	h := ruleRouter(t, newFakeRuleRepo())
	cases := map[string]string{
		"bad regex":        `{"name":"r","condition":{"field":"source","regex":"("},"actions":[{"type":"log"}]}`,
		"unknown operator": `{"name":"r","condition":{"field":"op","approx":"u"},"actions":[{"type":"log"}]}`,
		"unknown field":    `{"name":"r","condition":{"field":"amount","eq":1},"actions":[{"type":"log"}]}`,
		"empty actions":    `{"name":"r","condition":{"field":"op","eq":"update"},"actions":[]}`,
		"webhook no url":   `{"name":"r","condition":{"field":"op","eq":"update"},"actions":[{"type":"webhook"}]}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rec := post(t, h, rulesPath, "Bearer key-admin", body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
			}
		})
	}
}

// An unknown field in the body is rejected by DisallowUnknownFields.
func TestRuleUnknownBodyFieldIs400(t *testing.T) {
	h := ruleRouter(t, newFakeRuleRepo())
	rec := post(t, h, rulesPath, "Bearer key-admin", `{"name":"r","oops":1,"condition":{"field":"op","eq":"u"},"actions":[{"type":"log"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestRuleUpdateUnknownIs404(t *testing.T) {
	h := ruleRouter(t, newFakeRuleRepo())
	rec := put(t, h, rulesPath+"/rule_missing", "Bearer key-admin", sampleRuleBody)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

// The admin scope is required (NFR-5.2): a read+write key is 403.
func TestRuleRequiresAdminScope(t *testing.T) {
	h := ruleRouter(t, newFakeRuleRepo())
	rec := post(t, h, rulesPath, "Bearer key-rw", sampleRuleBody)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}

// The key's collection mask is enforced (FR-7.2): an admin key scoped to crm.*
// may not manage rules under docs.*.
func TestRuleOutsideMaskIs403(t *testing.T) {
	repo := newFakeRuleRepo()
	h := ruleRouter(t, repo)
	rec := post(t, h, "/api/v1/collections/docs.secret/rules", "Bearer key-admin", sampleRuleBody)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
	if len(repo.byID) != 0 {
		t.Fatal("masked collection must not reach the repository")
	}
}

// TestRuleMappingRoundTrip checks the wire→domain→wire mapping directly: the
// operator-as-key shape, the change match (HasOld/HasNew) and actions survive a
// decode and re-encode.
func TestRuleMappingRoundTrip(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(sampleRuleBody))
	rule, err := decodeRuleRequest(req)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Compiling proves the decoded condition is well-formed.
	if _, err := rules.Compile(rule.Condition); err != nil {
		t.Fatalf("decoded condition does not compile: %v", err)
	}
	// The change-match leaf carried old/new (HasOld/HasNew set).
	matchLeaf := rule.Condition.All[1]
	if matchLeaf.Match == nil || !matchLeaf.Match.HasOld || !matchLeaf.Match.HasNew {
		t.Fatalf("match leaf lost old/new presence: %+v", matchLeaf.Match)
	}

	resp := ruleResponse(rule)
	cond := resp["condition"].(map[string]any)
	all := cond["all"].([]map[string]any)
	if all[0]["eq"] != "update" {
		t.Fatalf("scalar leaf did not round-trip to operator-as-key: %+v", all[0])
	}
	if len(resp["actions"].([]map[string]any)) != 2 {
		t.Fatalf("actions lost in response mapping: %+v", resp["actions"])
	}
}
