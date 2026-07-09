//go:build integration

package mongo

import (
	"context"
	"errors"
	"testing"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/rules"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// sampleRule builds a rule whose condition exercises every node kind, so the
// round-trip tests prove the nested condition survives a BSON round-trip.
func sampleRule(name string) *rules.Rule {
	return &rules.Rule{
		Name:    name,
		Enabled: true,
		Condition: rules.Condition{All: []rules.Condition{
			{Field: rules.FieldOp, Op: rules.OpEq, Value: "update"},
			{Field: rules.FieldAuthorID, Op: rules.OpIn, Value: []any{"42", "77"}},
			{Not: &rules.Condition{Field: rules.FieldSource, Op: rules.OpRegex, Value: "^test"}},
			{Match: &rules.Match{Path: "items.*.price", Op: diff.OpChange, New: float64(12), HasNew: true}},
		}},
		Actions: []rules.Action{
			{Type: rules.ActionWebhook, Webhook: &rules.Webhook{URL: "https://x/h", SecretRef: "whsec_1", TimeoutMS: 5000, Retry: rules.Retry{MaxAttempts: 8, Backoff: "exponential"}}},
			{Type: rules.ActionLog, Log: &rules.LogAction{Level: "warn"}},
		},
	}
}

func TestIntegrationRuleCreateGetRoundTrip(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewRuleRepo(conn)

	r := sampleRule("alert")
	if err := repo.Create(ctx, "crm.deals", r); err != nil {
		t.Fatalf("create: %v", err)
	}
	if r.ID == "" || r.Version != 1 {
		t.Fatalf("create did not assign id/version: %+v", r)
	}

	got, err := repo.Get(ctx, "crm.deals", r.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	// The condition must compile and behave identically after a BSON round-trip:
	// that is the real proof it survived, beyond a struct compare over `any`.
	p, err := rules.Compile(got.Condition)
	if err != nil {
		t.Fatalf("stored condition does not compile: %v", err)
	}
	ev := rules.EvalEvent{
		Op: "update", Author: "77", Source: "prod",
		Changes: []diff.Change{{Path: "items.0.price", Op: diff.OpChange, Old: float64(10), New: float64(12)}},
	}
	if !p.Eval(ev) {
		t.Error("round-tripped condition should match the sample event")
	}
	if len(got.Actions) != 2 || got.Actions[0].Webhook == nil || got.Actions[0].Webhook.SecretRef != "whsec_1" {
		t.Fatalf("actions lost in round-trip: %+v", got.Actions)
	}
}

// An empty combinator keeps its non-nil identity through storage, so its fixed
// truth value (empty any ⇒ false) is preserved rather than degrading into an
// "empty condition" compile error on reload (the nil-vs-empty contract).
func TestIntegrationRuleEmptyCombinatorRoundTrip(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewRuleRepo(conn)

	r := &rules.Rule{
		Name:      "empty-any",
		Condition: rules.Condition{Any: []rules.Condition{}},
		Actions:   []rules.Action{{Type: rules.ActionLog, Log: &rules.LogAction{Level: "info"}}},
	}
	if err := repo.Create(ctx, "crm.deals", r); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := repo.Get(ctx, "crm.deals", r.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Condition.Any == nil {
		t.Fatal("empty Any combinator did not survive the round-trip (became nil)")
	}
	p, err := rules.Compile(got.Condition)
	if err != nil {
		t.Fatalf("empty combinator should compile, not error: %v", err)
	}
	if p.Eval(rules.EvalEvent{Op: "update"}) {
		t.Error("empty Any should evaluate to false")
	}
}

func TestIntegrationRuleNameUnique(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewRuleRepo(conn)

	if err := repo.Create(ctx, "crm.deals", sampleRule("dup")); err != nil {
		t.Fatalf("first create: %v", err)
	}
	err := repo.Create(ctx, "crm.deals", sampleRule("dup"))
	if !errors.Is(err, domain.ErrRuleNameConflict) {
		t.Fatalf("duplicate name err = %v, want ErrRuleNameConflict", err)
	}
	// The same name under a different collection is fine — uniqueness is scoped.
	if err := repo.Create(ctx, "crm.leads", sampleRule("dup")); err != nil {
		t.Fatalf("same name, other collection: %v", err)
	}
}

func TestIntegrationRuleUpdateBumpsVersion(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewRuleRepo(conn)

	r := sampleRule("v")
	if err := repo.Create(ctx, "crm.deals", r); err != nil {
		t.Fatalf("create: %v", err)
	}

	r.Enabled = false
	r.Condition = rules.Condition{Field: rules.FieldOp, Op: rules.OpEq, Value: "delete"}
	if err := repo.Update(ctx, "crm.deals", r); err != nil {
		t.Fatalf("update: %v", err)
	}
	if r.Version != 2 {
		t.Fatalf("version = %d, want 2", r.Version)
	}

	got, err := repo.Get(ctx, "crm.deals", r.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Version != 2 || got.Enabled {
		t.Fatalf("updated body not persisted: %+v", got)
	}
	if got.Condition.Op != rules.OpEq || got.Condition.Value != "delete" {
		t.Fatalf("condition not replaced: %+v", got.Condition)
	}

	if err := repo.Update(ctx, "crm.deals", sampleRule("missing")); !errors.Is(err, domain.ErrNotFound) {
		// sampleRule has no id, so the filter matches nothing.
		t.Fatalf("update of unknown id err = %v, want ErrNotFound", err)
	}
}

func TestIntegrationRuleDelete(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewRuleRepo(conn)

	r := sampleRule("d")
	if err := repo.Create(ctx, "crm.deals", r); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := repo.Delete(ctx, "crm.deals", r.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := repo.Get(ctx, "crm.deals", r.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("get after delete err = %v, want ErrNotFound", err)
	}
	if err := repo.Delete(ctx, "crm.deals", r.ID); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("delete again err = %v, want ErrNotFound", err)
	}
}

func TestIntegrationRuleListAndListAll(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewRuleRepo(conn)

	for _, n := range []string{"a", "b"} {
		if err := repo.Create(ctx, "crm.deals", sampleRule(n)); err != nil {
			t.Fatalf("create %s: %v", n, err)
		}
	}
	if err := repo.Create(ctx, "crm.leads", sampleRule("c")); err != nil {
		t.Fatalf("create c: %v", err)
	}

	deals, err := repo.List(ctx, "crm.deals")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(deals) != 2 {
		t.Fatalf("list(crm.deals) = %d rules, want 2", len(deals))
	}

	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("listAll: %v", err)
	}
	if len(all["crm.deals"]) != 2 || len(all["crm.leads"]) != 1 {
		t.Fatalf("listAll grouping = %v", all)
	}
}

// Rules are isolated per tenant: a different tenant lands in a different
// database (ADR-010), so it sees none of the first tenant's rules.
func TestIntegrationRuleTenantIsolation(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewRuleRepo(conn)

	if err := repo.Create(ctx, "crm.deals", sampleRule("only-a")); err != nil {
		t.Fatalf("create: %v", err)
	}

	otherCtx := tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: "tenant-b"}})
	got, err := repo.List(otherCtx, "crm.deals")
	if err != nil {
		t.Fatalf("list for other tenant: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant-b sees %d rules, want 0", len(got))
	}
}

// Provisioning is idempotent: a fresh repo (cleared provisioned cache) ensures
// the same indexes against an existing collection without error.
func TestIntegrationRuleProvisionIdempotent(t *testing.T) {
	conn, ctx := testConn(t)

	if err := NewRuleRepo(conn).Create(ctx, "crm.deals", sampleRule("first")); err != nil {
		t.Fatalf("first repo create: %v", err)
	}
	// A second repo has an empty provisioned map, so it re-runs ensure against the
	// now-existing collection and indexes.
	if err := NewRuleRepo(conn).Create(ctx, "crm.deals", sampleRule("second")); err != nil {
		t.Fatalf("second repo create (idempotent provision): %v", err)
	}
}
