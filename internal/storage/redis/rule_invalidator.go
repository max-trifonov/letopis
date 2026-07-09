package redis

import (
	"context"
	"strings"

	goredis "github.com/redis/go-redis/v9"

	"github.com/max-trifonov/letopis/internal/tenant"
)

// ruleInvalidateChannel carries rule-change signals for all tenants. Messages
// are "{tenant_db}|{collection}" — subscriber evicts that cache entry directly.
const ruleInvalidateChannel = "letopis:rules:invalidate"

// RuleInvalidator publishes rule-cache invalidations over Redis pub/sub and
// subscribes to them. Best-effort: a publish failure falls back to TTL expiry.
type RuleInvalidator struct {
	rdb goredis.UniversalClient
}

func NewRuleInvalidator(rdb goredis.UniversalClient) *RuleInvalidator {
	return &RuleInvalidator{rdb: rdb}
}

// InvalidateRules broadcasts that a collection's rules changed. The tenant comes
// from ctx, never a parameter. Publish errors are swallowed; TTL converges.
func (r *RuleInvalidator) InvalidateRules(ctx context.Context, collection string) {
	p, ok := tenant.FromContext(ctx)
	if !ok {
		return
	}
	msg := p.Tenant.DatabaseName() + "|" + collection
	_ = r.rdb.Publish(ctx, ruleInvalidateChannel, msg).Err()
}

func (r *RuleInvalidator) Subscribe(ctx context.Context, onInvalidate func(tenantDB, collection string)) error {
	sub := r.rdb.Subscribe(ctx, ruleInvalidateChannel)
	defer func() { _ = sub.Close() }()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			tenantDB, collection, ok := parseInvalidateMsg(msg.Payload)
			if !ok {
				continue
			}
			onInvalidate(tenantDB, collection)
		}
	}
}

// parseInvalidateMsg splits "{tenant_db}|{collection}". A malformed payload is
// ignored — a garbled message means TTL-bounded staleness, not a crash.
func parseInvalidateMsg(payload string) (tenantDB, collection string, ok bool) {
	i := strings.IndexByte(payload, '|')
	if i <= 0 || i == len(payload)-1 {
		return "", "", false
	}
	return payload[:i], payload[i+1:], true
}
