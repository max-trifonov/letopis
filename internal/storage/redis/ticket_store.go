// Package redis holds the Redis-backed infrastructure adapters: the async-write
// ticket store and the receipt-dedup idempotency store. It implements domain
// ports; the service layer depends on the interfaces, not on this package.
package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// keyspace prefixes every ticket key; the scheme keeps the Redis namespace
// legible and lets a FLUSH-by-pattern target one tenant.
const keyspace = "letopis:tkt"

// DefaultTTL is the ticket lifetime when the config leaves it unset: a client
// has a day to poll a result before the ticket is reaped.
const DefaultTTL = 24 * time.Hour

// TicketStore persists tickets in Redis with a TTL. Tickets are namespaced by
// tenant database: the key embeds the tenant from ctx, so one tenant can never
// resolve another's ticket id — an out-of-scope GET is indistinguishable from
// "expired".
type TicketStore struct {
	rdb goredis.UniversalClient
	ttl time.Duration
}

func NewTicketStore(rdb goredis.UniversalClient, ttl time.Duration) *TicketStore {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &TicketStore{rdb: rdb, ttl: ttl}
}

var _ domain.TicketStore = (*TicketStore)(nil)

// ticketDoc is the Redis serialization of a ticket: a DTO separate from the
// domain model so the storage shape can evolve independently.
type ticketDoc struct {
	ID         string `json:"id"`
	Status     string `json:"status"`
	Collection string `json:"collection"`
	EntityID   string `json:"entity_id"`
	Error      string `json:"error,omitempty"`
	CreatedAt  int64  `json:"created_at"` // unix millis
	UpdatedAt  int64  `json:"updated_at"`
}

func (s *TicketStore) Create(ctx context.Context, t *domain.Ticket) error {
	key, err := ticketKey(ctx, t.ID)
	if err != nil {
		return err
	}
	b, err := json.Marshal(toDoc(t))
	if err != nil {
		return err
	}
	if err := s.rdb.Set(ctx, key, b, s.ttl).Err(); err != nil {
		return fmt.Errorf("redis: create ticket: %w", err)
	}
	return nil
}

func (s *TicketStore) Get(ctx context.Context, id string) (*domain.Ticket, error) {
	key, err := ticketKey(ctx, id)
	if err != nil {
		return nil, err
	}
	b, err := s.rdb.Get(ctx, key).Bytes()
	if errors.Is(err, goredis.Nil) {
		return nil, domain.ErrTicketNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("redis: get ticket: %w", err)
	}
	var d ticketDoc
	if err := json.Unmarshal(b, &d); err != nil {
		return nil, fmt.Errorf("redis: decode ticket: %w", err)
	}
	return fromDoc(d), nil
}

// Save overwrites an existing ticket, preserving its original TTL (KEEPTTL) so a
// status update does not extend the lifetime granted at creation. A ticket that
// expired between Get and Save is reported as not found rather than silently
// recreated without a TTL.
func (s *TicketStore) Save(ctx context.Context, t *domain.Ticket) error {
	key, err := ticketKey(ctx, t.ID)
	if err != nil {
		return err
	}
	b, err := json.Marshal(toDoc(t))
	if err != nil {
		return err
	}
	ok, err := s.rdb.SetXX(ctx, key, b, goredis.KeepTTL).Result()
	if err != nil {
		return fmt.Errorf("redis: save ticket: %w", err)
	}
	if !ok {
		return domain.ErrTicketNotFound
	}
	return nil
}

// ticketKey builds the tenant-scoped Redis key. The database name comes from the
// request principal, never from the ticket id, so ids are only resolvable within
// their own tenant.
func ticketKey(ctx context.Context, id string) (string, error) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return "", tenant.ErrNoKey
	}
	return keyspace + ":" + p.Tenant.DatabaseName() + ":" + id, nil
}

func toDoc(t *domain.Ticket) ticketDoc {
	return ticketDoc{
		ID:         t.ID,
		Status:     string(t.Status),
		Collection: t.EntityCollection,
		EntityID:   t.EntityID,
		Error:      t.Error,
		CreatedAt:  t.CreatedAt.UnixMilli(),
		UpdatedAt:  t.UpdatedAt.UnixMilli(),
	}
}

func fromDoc(d ticketDoc) *domain.Ticket {
	return &domain.Ticket{
		ID:               d.ID,
		Status:           domain.TicketStatus(d.Status),
		EntityCollection: d.Collection,
		EntityID:         d.EntityID,
		Error:            d.Error,
		CreatedAt:        time.UnixMilli(d.CreatedAt).UTC(),
		UpdatedAt:        time.UnixMilli(d.UpdatedAt).UTC(),
	}
}
