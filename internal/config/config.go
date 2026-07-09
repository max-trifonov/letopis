// Package config defines the instance configuration: a YAML file plus a small
// set of environment overrides. Dynamic state (tenants, collections, rules)
// lives in the database, not here.
package config

import (
	"fmt"
	"slices"

	"github.com/max-trifonov/letopis/internal/tenant"
)

// Role selects which parts of the service a process runs.
type Role string

const (
	RoleAPI    Role = "api"
	RoleWorker Role = "worker"
	RoleAll    Role = "all"
)

func (r Role) ServesAPI() bool  { return r == RoleAPI || r == RoleAll }
func (r Role) RunsWorker() bool { return r == RoleWorker || r == RoleAll }

type Config struct {
	Role        Role        `yaml:"role"`
	Server      Server      `yaml:"server"`
	MongoDB     MongoDB     `yaml:"mongodb"`
	Redis       Redis       `yaml:"redis"`
	Queue       Queue       `yaml:"queue"`
	Tickets     Tickets     `yaml:"tickets"`
	Idempotency Idempotency `yaml:"idempotency"`
	Rules       Rules       `yaml:"rules"`
	Webhooks    Webhooks    `yaml:"webhooks"`
	Log         Log         `yaml:"log"`
	Collections Collections `yaml:"collections"`
	// Tenants is the MVP bootstrap: tenants and their API keys. Until the admin
	// API lands this YAML is the only source.
	Tenants []Tenant `yaml:"tenants"`
}

type Collections struct {
	// AutoCreate lets a write to an unknown collection provision it on first
	// use. On by default for frictionless onboarding; turn off to require
	// explicit creation.
	AutoCreate bool `yaml:"auto_create"`
}

// Tenant is the YAML shape of one tenant. It is a config DTO mapped to the
// tenant domain via TenantSpecs — it never leaks inward.
type Tenant struct {
	ID       string         `yaml:"id"`
	Database TenantDatabase `yaml:"database"`
	Keys     []TenantKey    `yaml:"keys"`
}

type TenantDatabase struct {
	URI  string `yaml:"uri"`
	Name string `yaml:"name"`
}

type TenantKey struct {
	KeyHash     string   `yaml:"key_hash"`
	Key         string   `yaml:"key"`
	Scopes      []string `yaml:"scopes"`
	Collections []string `yaml:"collections"`
}

// TenantSpecs maps the config DTOs to the tenant package's input shape. Explicit
// mapping keeps the YAML form and the domain model decoupled.
func (c *Config) TenantSpecs() []tenant.Spec {
	specs := make([]tenant.Spec, 0, len(c.Tenants))
	for _, t := range c.Tenants {
		spec := tenant.Spec{ID: t.ID, DBURI: t.Database.URI, DBName: t.Database.Name}
		for _, k := range t.Keys {
			spec.Keys = append(spec.Keys, tenant.KeySpec{
				Hash:        k.KeyHash,
				Plaintext:   k.Key,
				Scopes:      k.Scopes,
				Collections: k.Collections,
			})
		}
		specs = append(specs, spec)
	}
	return specs
}

type Server struct {
	HTTP Listen `yaml:"http"`
	GRPC Listen `yaml:"grpc"`
}

type Listen struct {
	Addr string `yaml:"addr"`
	TLS  TLS    `yaml:"tls"`
}

type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// MongoDB holds the default cluster connection. Tenants may override it
// individually; those overrides live in the tenant config, not here.
type MongoDB struct {
	URI string `yaml:"uri"`
}

type Redis struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// Queue configures the event-pipeline backend. Driver values are kept as
// literals here so config does not depend on the infrastructure package.
type Queue struct {
	// Driver selects the implementation: "memory" (in-process, only valid for
	// role=all) or "redis-streams" (cross-process, the production default).
	Driver string `yaml:"driver"`
	// Shards is the number of streams events are partitioned across; one shard
	// is consumed by one worker goroutine to keep per-entity order. Changing it
	// on a live queue reshuffles the key→stream mapping and requires a drain.
	Shards int `yaml:"shards"`
	// StreamPrefix and ConsumerGroup name the Redis Streams resources; ignored
	// by the memory driver.
	StreamPrefix  string `yaml:"stream_prefix"`
	ConsumerGroup string `yaml:"consumer_group"`
	// BatchSize and BatchLingerMS tune the fast-path batcher: flush at this many
	// accumulated events or after this many milliseconds, whichever comes first.
	// Zero falls back to the built-in defaults (500 / 50).
	BatchSize     int `yaml:"batch_size"`
	BatchLingerMS int `yaml:"batch_linger_ms"`
	// MaxDepth trips backpressure: an async accept is refused with 429+Retry-After
	// once the queue's sampled depth reaches this many messages. Also how a Mongo
	// outage surfaces: the worker stops acking, the backlog climbs, and the
	// threshold produces a clean 429 instead of an unbounded queue.
	// 0 disables backpressure (depth is still sampled for the gauge).
	MaxDepth int `yaml:"max_depth"`
	// RetryAfterSeconds is the Retry-After hint returned on a backpressure 429.
	// Zero falls back to the built-in default.
	RetryAfterSeconds int `yaml:"retry_after_seconds"`
}

type Tickets struct {
	// TTLHours is how long a ticket is retained before it is reaped; a client
	// has this window to poll a result. Zero falls back to 24h.
	TTLHours int `yaml:"ttl_hours"`
}

type Idempotency struct {
	// TTLHours is the dedup window: how long a seen event_id/Idempotency-Key is
	// remembered so a repeat replays the original accept. Zero falls back to 24h.
	TTLHours int `yaml:"ttl_hours"`
}

type Rules struct {
	// CacheTTLSeconds is the fallback lifetime of a collection's compiled-rule
	// cache: how long a rule change can take to become visible when Redis pub/sub
	// invalidation is unavailable. With pub/sub working, edits take effect
	// immediately and this only bounds the worst case. Zero falls back to 30s.
	CacheTTLSeconds int `yaml:"cache_ttl_seconds"`
}

// Webhooks configures the webhook delivery pipeline. The delivery stream is a
// second queue on the same backend as ingest; the secrets map resolves a rule's
// secret_ref to its HMAC signing secret, so the secret lives in instance config,
// not in the rules database.
type Webhooks struct {
	// DefaultTimeoutMS bounds a single delivery attempt when the rule does not set
	// its own timeout. Zero falls back to the built-in default (5000ms).
	DefaultTimeoutMS int `yaml:"default_timeout_ms"`
	// MaxAttempts caps delivery tries before a webhook goes to the DLQ when the
	// rule does not set its own. Zero falls back to the built-in default (5).
	MaxAttempts int `yaml:"max_attempts"`
	// Backoff tunes the exponential retry schedule shared by all deliveries.
	Backoff Backoff `yaml:"backoff"`
	// Secrets maps an opaque secret_ref to the HMAC signing secret. A rule
	// references a key here; an unknown ref sends the delivery straight to the DLQ
	// without retries — it is a configuration error, not a transient failure.
	Secrets map[string]string `yaml:"secrets"`
	// DeliveryShards is the number of streams deliveries are partitioned across.
	// Unlike ingest, deliveries are not ordered between themselves, so this only
	// tunes parallelism. Zero falls back to the ingest queue's shard count.
	DeliveryShards int `yaml:"delivery_shards"`
	// SSRF controls which outbound webhook targets are permitted. By default all
	// private/loopback/link-local/metadata addresses are blocked.
	SSRF WebhookSSRF `yaml:"ssrf"`
}

type WebhookSSRF struct {
	// Allow is a list of CIDRs always permitted, even if they are private (e.g.
	// ["192.168.100.0/24"] for a webhook receiver on an internal network).
	Allow []string `yaml:"allow"`
	// Deny is a list of extra CIDRs to block in addition to the built-in denylist.
	Deny []string `yaml:"deny"`
	// AllowPrivate disables the built-in RFC1918/loopback/link-local denylist
	// entirely. Use only in trusted environments; the default (false) is safe.
	AllowPrivate bool `yaml:"allow_private"`
	// AllowHTTP permits http:// webhook targets. Default false; set true only in
	// dev/test environments where TLS is not available.
	AllowHTTP bool `yaml:"allow_http"`
}

// Backoff is the exponential-with-jitter retry schedule: the nth attempt waits
// min(base*2^n, max) with full jitter applied.
type Backoff struct {
	BaseMS int `yaml:"base_ms"`
	MaxMS  int `yaml:"max_ms"`
}

type Log struct {
	Level  string `yaml:"level"`  // debug | info | warn | error
	Format string `yaml:"format"` // json | text
}

func Default() *Config {
	return &Config{
		Role: RoleAll,
		Server: Server{
			HTTP: Listen{Addr: ":8080"},
			GRPC: Listen{Addr: ":9090"},
		},
		MongoDB:     MongoDB{URI: "mongodb://localhost:27017"},
		Redis:       Redis{Addr: "localhost:6379"},
		Queue:       Queue{Driver: "redis-streams", Shards: 16, StreamPrefix: "letopis:ingest", ConsumerGroup: "workers"},
		Log:         Log{Level: "info", Format: "json"},
		Collections: Collections{AutoCreate: true},
	}
}

func (c *Config) Validate() error {
	switch c.Role {
	case RoleAPI, RoleWorker, RoleAll:
	default:
		return fmt.Errorf("role: %q is not one of api, worker, all", c.Role)
	}
	if !slices.Contains([]string{"debug", "info", "warn", "error"}, c.Log.Level) {
		return fmt.Errorf("log.level: %q is not one of debug, info, warn, error", c.Log.Level)
	}
	if !slices.Contains([]string{"json", "text"}, c.Log.Format) {
		return fmt.Errorf("log.format: %q is not one of json, text", c.Log.Format)
	}
	if c.Role.ServesAPI() {
		for name, l := range map[string]Listen{"server.http": c.Server.HTTP, "server.grpc": c.Server.GRPC} {
			if l.Addr == "" {
				return fmt.Errorf("%s.addr: required for role %q", name, c.Role)
			}
			if l.TLS.Enabled && (l.TLS.CertFile == "" || l.TLS.KeyFile == "") {
				return fmt.Errorf("%s.tls: cert_file and key_file are required when enabled", name)
			}
		}
	}
	if c.Rules.CacheTTLSeconds < 0 {
		return fmt.Errorf("rules.cache_ttl_seconds: must be >= 0 (0 = default), got %d", c.Rules.CacheTTLSeconds)
	}
	if err := c.Webhooks.validate(); err != nil {
		return err
	}
	return c.Queue.validate(c.Role, c.Redis)
}

// validate checks the webhook delivery block. All knobs accept zero as "use the
// built-in default"; only negative values are rejected, since a negative timeout
// or attempt count cannot encode a deliverable.
func (w Webhooks) validate() error {
	for name, v := range map[string]int{
		"webhooks.default_timeout_ms": w.DefaultTimeoutMS,
		"webhooks.max_attempts":       w.MaxAttempts,
		"webhooks.backoff.base_ms":    w.Backoff.BaseMS,
		"webhooks.backoff.max_ms":     w.Backoff.MaxMS,
		"webhooks.delivery_shards":    w.DeliveryShards,
	} {
		if v < 0 {
			return fmt.Errorf("%s: must be >= 0 (0 = default), got %d", name, v)
		}
	}
	return nil
}

// validate checks the queue block against the process role and Redis settings.
// The memory driver shares nothing between processes, so it is only meaningful
// when one process runs both api and worker; redis-streams needs a Redis endpoint.
func (q Queue) validate(role Role, redis Redis) error {
	if q.Shards < 1 {
		return fmt.Errorf("queue.shards: must be >= 1, got %d", q.Shards)
	}
	if q.BatchSize < 0 {
		return fmt.Errorf("queue.batch_size: must be >= 0 (0 = default), got %d", q.BatchSize)
	}
	if q.BatchLingerMS < 0 {
		return fmt.Errorf("queue.batch_linger_ms: must be >= 0 (0 = default), got %d", q.BatchLingerMS)
	}
	if q.MaxDepth < 0 {
		return fmt.Errorf("queue.max_depth: must be >= 0 (0 = disabled), got %d", q.MaxDepth)
	}
	if q.RetryAfterSeconds < 0 {
		return fmt.Errorf("queue.retry_after_seconds: must be >= 0 (0 = default), got %d", q.RetryAfterSeconds)
	}
	switch q.Driver {
	case "memory":
		if role != RoleAll {
			return fmt.Errorf("queue.driver: memory is only valid with role=all; split api/worker roles need redis-streams")
		}
	case "redis-streams":
		if redis.Addr == "" {
			return fmt.Errorf("queue.driver: redis-streams requires redis.addr")
		}
		if q.StreamPrefix == "" || q.ConsumerGroup == "" {
			return fmt.Errorf("queue: stream_prefix and consumer_group are required for redis-streams")
		}
	default:
		return fmt.Errorf("queue.driver: %q is not one of memory, redis-streams", q.Driver)
	}
	return nil
}
