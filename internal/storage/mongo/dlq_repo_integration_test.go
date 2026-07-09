//go:build integration

package mongo

import (
	"errors"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

func sampleDeadLetter(id, ruleID string, at time.Time) domain.DeadLetter {
	return domain.DeadLetter{
		ID:         id,
		RuleID:     ruleID,
		Collection: "crm.deals",
		DeliveryID: "dlv_" + id,
		URL:        "https://recv.example.com/hook",
		SecretRef:  "whsec_1",
		Body:       []byte(`{"event":{"entity_id":"d-1"}}`),
		Attempts:   5,
		LastError:  "webhook returned status Internal Server Error",
		FailedAt:   at,
	}
}

func TestIntegrationDLQSaveListGet(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewDLQRepo(conn)

	base := time.Now().UTC().Truncate(time.Millisecond)
	// Save three for rule_a (different failed_at) and one for rule_b.
	for i, at := range []time.Time{base, base.Add(time.Second), base.Add(2 * time.Second)} {
		if err := repo.Save(ctx, sampleDeadLetter(string(rune('1'+i)), "rule_a", at)); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	if err := repo.Save(ctx, sampleDeadLetter("9", "rule_b", base)); err != nil {
		t.Fatalf("save rule_b: %v", err)
	}

	// List returns only rule_a, newest-first.
	got, err := repo.List(ctx, "rule_a", 10, nil)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("rule_a entries = %d, want 3", len(got))
	}
	if !got[0].FailedAt.After(got[1].FailedAt) || !got[1].FailedAt.After(got[2].FailedAt) {
		t.Fatalf("not newest-first: %v", []time.Time{got[0].FailedAt, got[1].FailedAt, got[2].FailedAt})
	}

	// Get round-trips the body and fields faithfully.
	one, err := repo.Get(ctx, got[0].ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if one.RuleID != "rule_a" || string(one.Body) != `{"event":{"entity_id":"d-1"}}` || one.Attempts != 5 {
		t.Fatalf("round-trip mismatch: %+v", one)
	}
}

func TestIntegrationDLQPagination(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewDLQRepo(conn)

	base := time.Now().UTC().Truncate(time.Millisecond)
	const n = 5
	for i := range n {
		at := base.Add(time.Duration(i) * time.Second)
		if err := repo.Save(ctx, sampleDeadLetter(string(rune('1'+i)), "rule_a", at)); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	// Page through two at a time using the failed_at+id cursor; expect all five in
	// strict newest-first order with no repeats or gaps.
	var seen []string
	var after *domain.DLQCursor
	for {
		page, err := repo.List(ctx, "rule_a", 2, after)
		if err != nil {
			t.Fatalf("list page: %v", err)
		}
		if len(page) == 0 {
			break
		}
		for _, dl := range page {
			seen = append(seen, dl.ID)
		}
		last := page[len(page)-1]
		after = &domain.DLQCursor{FailedAt: last.FailedAt, ID: last.ID}
		if len(page) < 2 {
			break
		}
	}
	if len(seen) != n {
		t.Fatalf("paged %d entries, want %d (%v)", len(seen), n, seen)
	}
	// No duplicates.
	uniq := map[string]struct{}{}
	for _, id := range seen {
		if _, dup := uniq[id]; dup {
			t.Fatalf("duplicate id across pages: %s", id)
		}
		uniq[id] = struct{}{}
	}
}

func TestIntegrationDLQDeleteAndCount(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewDLQRepo(conn)

	at := time.Now().UTC()
	_ = repo.Save(ctx, sampleDeadLetter("1", "rule_a", at))
	_ = repo.Save(ctx, sampleDeadLetter("2", "rule_a", at))

	cnt, err := repo.Count(ctx, "rule_a")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if cnt != 2 {
		t.Fatalf("count = %d, want 2", cnt)
	}

	if err := repo.Delete(ctx, "1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := repo.Delete(ctx, "1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("second delete err = %v, want ErrNotFound", err)
	}
	if cnt, _ := repo.Count(ctx, "rule_a"); cnt != 1 {
		t.Fatalf("count after delete = %d, want 1", cnt)
	}
}

func TestIntegrationDLQTenantIsolation(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewDLQRepo(conn)

	if err := repo.Save(ctx, sampleDeadLetter("1", "rule_a", time.Now().UTC())); err != nil {
		t.Fatalf("save: %v", err)
	}

	// A different tenant must not see the first tenant's dead letters.
	otherCtx := tenant.NewContext(ctx, tenant.Principal{Tenant: tenant.Tenant{ID: "other"}})
	got, err := repo.List(otherCtx, "rule_a", 10, nil)
	if err != nil {
		t.Fatalf("list other tenant: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("tenant isolation breached: other tenant saw %d entries", len(got))
	}
	if _, err := repo.Get(otherCtx, "1"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("other tenant Get err = %v, want ErrNotFound", err)
	}
}

func TestIntegrationDLQProvisionIdempotent(t *testing.T) {
	conn, ctx := testConn(t)
	repo := NewDLQRepo(conn)

	// Two saves trigger ensure twice; the second must be a no-op, not an error.
	if err := repo.Save(ctx, sampleDeadLetter("1", "rule_a", time.Now().UTC())); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if err := repo.Save(ctx, sampleDeadLetter("2", "rule_a", time.Now().UTC())); err != nil {
		t.Fatalf("second save: %v", err)
	}

	db, err := conn.DBFor(ctx)
	if err != nil {
		t.Fatalf("db: %v", err)
	}
	cur, err := db.Collection(DLQ).Indexes().List(ctx)
	if err != nil {
		t.Fatalf("list indexes: %v", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	found := false
	for cur.Next(ctx) {
		var idx struct {
			Name string `bson:"name"`
		}
		if err := cur.Decode(&idx); err != nil {
			t.Fatalf("decode index: %v", err)
		}
		if idx.Name == "dlq_rule_failed_at" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected index dlq_rule_failed_at to exist")
	}
}
