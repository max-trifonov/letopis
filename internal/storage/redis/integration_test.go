//go:build integration

// These tests need a real Redis and Docker; they run under
// `go test -tags integration ./...`. They cover the ticket lifecycle against a
// live backend: create/read, TTL-preserving updates, tenant isolation, and
// expiry surfacing as not-found.
package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

func testClient(t *testing.T) goredis.UniversalClient {
	t.Helper()
	ctx := context.Background()
	container, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		t.Fatalf("start redis container: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(container) })
	uri, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	opt, err := goredis.ParseURL(uri)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	rdb := goredis.NewClient(opt)
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func ctxFor(id string) context.Context {
	return tenant.NewContext(context.Background(), tenant.Principal{Tenant: tenant.Tenant{ID: id}})
}

func newTicket(id string) *domain.Ticket {
	now := time.Now().UTC()
	return &domain.Ticket{ID: id, Status: domain.TicketAccepted, EntityCollection: "crm.deals", EntityID: "d-1", CreatedAt: now, UpdatedAt: now}
}

func TestIntegrationTicketLifecycle(t *testing.T) {
	store := NewTicketStore(testClient(t), time.Hour)
	ctx := ctxFor("acme")

	if err := store.Create(ctx, newTicket("tkt_1")); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := store.Get(ctx, "tkt_1")
	if err != nil || got.Status != domain.TicketAccepted {
		t.Fatalf("get = %+v err=%v", got, err)
	}

	got.Status = domain.TicketStored
	if err := store.Save(ctx, got); err != nil {
		t.Fatalf("save: %v", err)
	}
	again, _ := store.Get(ctx, "tkt_1")
	if again.Status != domain.TicketStored {
		t.Fatalf("status after save = %s", again.Status)
	}
}

func TestIntegrationTicketTenantIsolation(t *testing.T) {
	store := NewTicketStore(testClient(t), time.Hour)
	if err := store.Create(ctxFor("acme"), newTicket("tkt_1")); err != nil {
		t.Fatalf("create: %v", err)
	}
	// A different tenant must not resolve acme's ticket id.
	if _, err := store.Get(ctxFor("globex"), "tkt_1"); !errors.Is(err, domain.ErrTicketNotFound) {
		t.Fatalf("cross-tenant get err = %v, want ErrTicketNotFound", err)
	}
}

func TestIntegrationTicketExpiry(t *testing.T) {
	store := NewTicketStore(testClient(t), 300*time.Millisecond)
	ctx := ctxFor("acme")
	if err := store.Create(ctx, newTicket("tkt_1")); err != nil {
		t.Fatalf("create: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := store.Get(ctx, "tkt_1"); !errors.Is(err, domain.ErrTicketNotFound) {
		t.Fatalf("expired get err = %v, want ErrTicketNotFound", err)
	}
	// Save on an expired ticket must not silently resurrect it.
	if err := store.Save(ctx, newTicket("tkt_1")); !errors.Is(err, domain.ErrTicketNotFound) {
		t.Fatalf("save after expiry err = %v, want ErrTicketNotFound", err)
	}
}

func TestIntegrationIdempotencyReserveAndReplay(t *testing.T) {
	store := NewIdempotencyStore(testClient(t), time.Hour)
	ctx := ctxFor("acme")
	key := "crm.deals|evt-1"
	rec := domain.IdempotencyRecord{TicketID: "tkt_1", StatusCode: 202}

	reserved, _, err := store.Reserve(ctx, key, rec)
	if err != nil || !reserved {
		t.Fatalf("first reserve = %v err=%v, want true", reserved, err)
	}
	// A repeat loses the claim and reads back the original record.
	reserved, existing, err := store.Reserve(ctx, key, domain.IdempotencyRecord{TicketID: "tkt_2", StatusCode: 202})
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if reserved || existing.TicketID != "tkt_1" {
		t.Fatalf("repeat must replay tkt_1: reserved=%v existing=%+v", reserved, existing)
	}
}

func TestIntegrationIdempotencyTenantIsolation(t *testing.T) {
	store := NewIdempotencyStore(testClient(t), time.Hour)
	key := "crm.deals|evt-1"
	rec := domain.IdempotencyRecord{TicketID: "tkt_1", StatusCode: 202}

	if reserved, _, _ := store.Reserve(ctxFor("acme"), key, rec); !reserved {
		t.Fatal("acme reserve must succeed")
	}
	// Another tenant's identical logical key is independent.
	reserved, _, err := store.Reserve(ctxFor("globex"), key, rec)
	if err != nil || !reserved {
		t.Fatalf("globex reserve = %v err=%v, want true (isolated namespace)", reserved, err)
	}
}

func TestIntegrationIdempotencyRelease(t *testing.T) {
	store := NewIdempotencyStore(testClient(t), time.Hour)
	ctx := ctxFor("acme")
	key := "crm.deals|evt-1"
	rec := domain.IdempotencyRecord{TicketID: "tkt_1", StatusCode: 202}

	_, _, _ = store.Reserve(ctx, key, rec)
	if err := store.Release(ctx, key); err != nil {
		t.Fatalf("release: %v", err)
	}
	// After release the key is free again.
	reserved, _, err := store.Reserve(ctx, key, rec)
	if err != nil || !reserved {
		t.Fatalf("reserve after release = %v err=%v, want true", reserved, err)
	}
}

func TestIntegrationIdempotencyExpiry(t *testing.T) {
	store := NewIdempotencyStore(testClient(t), 300*time.Millisecond)
	ctx := ctxFor("acme")
	key := "crm.deals|evt-1"
	rec := domain.IdempotencyRecord{TicketID: "tkt_1", StatusCode: 202}

	_, _, _ = store.Reserve(ctx, key, rec)
	time.Sleep(500 * time.Millisecond)
	// The dedup window lapsed: the key is claimable again (FR-1.6).
	reserved, _, err := store.Reserve(ctx, key, rec)
	if err != nil || !reserved {
		t.Fatalf("reserve after expiry = %v err=%v, want true", reserved, err)
	}
}

// Concurrent identical reservations against a live Redis: exactly one wins,
// proving the SET NX is the real arbiter (the first idempotency barrier).
func TestIntegrationIdempotencyReserveRace(t *testing.T) {
	store := NewIdempotencyStore(testClient(t), time.Hour)
	ctx := ctxFor("acme")
	key := "crm.deals|evt-1"
	rec := domain.IdempotencyRecord{TicketID: "tkt_1", StatusCode: 202}

	const n = 24
	var wins atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			if reserved, _, err := store.Reserve(ctx, key, rec); err == nil && reserved {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("reservation race winners = %d, want exactly 1", wins.Load())
	}
}
