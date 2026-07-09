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

const idempKeyspace = "letopis:idem"

// reserveMaxTries bounds the SET NX / GET race: a key that expires between
// SET NX loss and GET retries rather than reporting an empty record.
const reserveMaxTries = 3

// IdempotencyStore is the Redis receipt-dedup barrier. Keys are namespaced by
// tenant database so one tenant's event_id never collides with another's.
type IdempotencyStore struct {
	rdb goredis.UniversalClient
	ttl time.Duration
}

func NewIdempotencyStore(rdb goredis.UniversalClient, ttl time.Duration) *IdempotencyStore {
	if ttl <= 0 {
		ttl = DefaultTTL
	}
	return &IdempotencyStore{rdb: rdb, ttl: ttl}
}

var _ domain.IdempotencyStore = (*IdempotencyStore)(nil)

type idempDoc struct {
	TicketID   string `json:"ticket_id"`
	StatusCode int    `json:"status_code"`
}

func (s *IdempotencyStore) Reserve(ctx context.Context, key string, rec domain.IdempotencyRecord) (bool, domain.IdempotencyRecord, error) {
	rkey, err := idempotencyKey(ctx, key)
	if err != nil {
		return false, domain.IdempotencyRecord{}, err
	}
	b, err := json.Marshal(idempDoc{TicketID: rec.TicketID, StatusCode: rec.StatusCode})
	if err != nil {
		return false, domain.IdempotencyRecord{}, err
	}
	for range reserveMaxTries {
		ok, err := s.rdb.SetNX(ctx, rkey, b, s.ttl).Result()
		if err != nil {
			return false, domain.IdempotencyRecord{}, fmt.Errorf("redis: reserve idempotency: %w", err)
		}
		if ok {
			return true, domain.IdempotencyRecord{}, nil
		}
		existing, err := s.rdb.Get(ctx, rkey).Bytes()
		if errors.Is(err, goredis.Nil) {
			continue // expired in the gap between SET NX loss and GET; retry
		}
		if err != nil {
			return false, domain.IdempotencyRecord{}, fmt.Errorf("redis: read idempotency: %w", err)
		}
		var d idempDoc
		if err := json.Unmarshal(existing, &d); err != nil {
			return false, domain.IdempotencyRecord{}, fmt.Errorf("redis: decode idempotency: %w", err)
		}
		return false, domain.IdempotencyRecord{TicketID: d.TicketID, StatusCode: d.StatusCode}, nil
	}
	// Never won SET NX and never found a value: proceed undeduped; the
	// storage {event_id} barrier is the backstop.
	return true, domain.IdempotencyRecord{}, nil
}

func (s *IdempotencyStore) Release(ctx context.Context, key string) error {
	rkey, err := idempotencyKey(ctx, key)
	if err != nil {
		return err
	}
	if err := s.rdb.Del(ctx, rkey).Err(); err != nil {
		return fmt.Errorf("redis: release idempotency: %w", err)
	}
	return nil
}

// idempotencyKey builds the tenant-scoped Redis key. The tenant comes from the
// principal in ctx, never from the client, so keys never cross tenant boundaries.
func idempotencyKey(ctx context.Context, logical string) (string, error) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return "", tenant.ErrNoKey
	}
	return idempKeyspace + ":" + p.Tenant.DatabaseName() + ":" + logical, nil
}
