package domain

import (
	"context"
	"fmt"
	"time"
)

// Reliability, ordering and lifecycle enums for per-collection config.
// All enums are validated and stored; those not yet enforced (retention, plugins)
// are stored now so configs written today survive future activations without migration.
type (
	ReliabilityMode string
	FirstEventOp    string
	OrderingMode    string
	RetentionType   string
	FailMode        string
)

const (
	ReliabilityStrict  ReliabilityMode = "strict"
	ReliabilityDurable ReliabilityMode = "durable"
	ReliabilityFast    ReliabilityMode = "fast"

	FirstEventCreate FirstEventOp = "create"
	FirstEventUpdate FirstEventOp = "update"

	OrderingReceived OrderingMode = "received"
	OrderingSource   OrderingMode = "source" // reserved, not honoured before 1.0

	RetentionForever  RetentionType = "forever"
	RetentionDays     RetentionType = "days"
	RetentionVersions RetentionType = "versions"

	FailOpen   FailMode = "open"
	FailClosed FailMode = "closed"
)

// ValidReliabilityMode reports whether m is one of the three known modes.
// The empty string is not valid here — callers that allow "unset" check it before calling.
func ValidReliabilityMode(m ReliabilityMode) bool {
	switch m {
	case ReliabilityStrict, ReliabilityDurable, ReliabilityFast:
		return true
	default:
		return false
	}
}

func ValidFirstEventOp(op FirstEventOp) bool {
	switch op {
	case FirstEventCreate, FirstEventUpdate:
		return true
	default:
		return false
	}
}

// ValidOrderingMode accepts source even though it's not enforced before 1.0,
// so a config written today survives its activation without migration.
func ValidOrderingMode(m OrderingMode) bool {
	switch m {
	case OrderingReceived, OrderingSource:
		return true
	default:
		return false
	}
}

func ValidRetentionType(t RetentionType) bool {
	switch t {
	case RetentionForever, RetentionDays, RetentionVersions:
		return true
	default:
		return false
	}
}

func ValidFailMode(m FailMode) bool {
	switch m {
	case FailOpen, FailClosed:
		return true
	default:
		return false
	}
}

// ConfigError marks an invalid CollectionConfig field.
// Transport maps it to 400 and surfaces Field so a client can fix the request.
type ConfigError struct {
	Field  string
	Reason string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("collection config: %s: %s", e.Field, e.Reason)
}

// Validate checks a config submitted through the admin API. Rejects any *set* enum
// field with an unknown value and any negative numeric field. An empty enum is
// allowed — WithDefaults fills it. Does not reject an explicit zero numeric field:
// "unset" and "zero" are indistinguishable at the domain level, so positive-value
// enforcement of an explicit zero happens at the transport boundary.
func (cfg CollectionConfig) Validate() error {
	if cfg.ReliabilityMode != "" && !ValidReliabilityMode(cfg.ReliabilityMode) {
		return &ConfigError{Field: "reliability_mode", Reason: "unknown mode"}
	}
	if cfg.FirstEventOp != "" && !ValidFirstEventOp(cfg.FirstEventOp) {
		return &ConfigError{Field: "first_event_op", Reason: "unknown op"}
	}
	if cfg.Ordering.Mode != "" && !ValidOrderingMode(cfg.Ordering.Mode) {
		return &ConfigError{Field: "ordering.mode", Reason: "unknown mode"}
	}
	if cfg.Retention.Type != "" && !ValidRetentionType(cfg.Retention.Type) {
		return &ConfigError{Field: "retention.type", Reason: "unknown type"}
	}
	if cfg.SnapshotInterval < 0 {
		return &ConfigError{Field: "snapshot_interval", Reason: "must be positive"}
	}
	if cfg.MaxEventSizeBytes < 0 {
		return &ConfigError{Field: "max_event_size_bytes", Reason: "must be positive"}
	}
	for name, p := range cfg.Plugins {
		if p.FailMode != "" && !ValidFailMode(p.FailMode) {
			return &ConfigError{Field: "plugins." + name + ".fail_mode", Reason: "unknown fail mode"}
		}
	}
	return nil
}

// Defaults applied to unset config fields. Kept in one place so wire layer
// and storage agree on what an omitted field means.
const (
	DefaultSnapshotInterval    = 100
	DefaultMaxEventSizeBytes   = 1 << 20 // 1 MiB
	DefaultReorderWindowMillis = 0
)

// Retention controls how much history is kept; stored but not yet enforced.
type Retention struct {
	Type RetentionType
	Days int // when Type == days
	Keep int // when Type == versions
}

// Ordering selects version ordering. source mode is reserved; only received is enforced.
type Ordering struct {
	Mode            OrderingMode
	ReorderWindowMS int
}

type PluginConfig struct {
	Enabled  bool
	FailMode FailMode
	Params   map[string]any
}

// CollectionConfig is the per-collection behaviour record. Name is the logical
// collection name (e.g. "crm.deals").
type CollectionConfig struct {
	Name              string
	ReliabilityMode   ReliabilityMode
	SnapshotInterval  int
	Retention         Retention
	MaxEventSizeBytes int64
	FirstEventOp      FirstEventOp
	Ordering          Ordering
	ArrayKeys         map[string]string
	Plugins           map[string]PluginConfig
}

// WithDefaults returns a copy of cfg with every unset field filled from built-in
// defaults. Pure, so it can be unit-tested and applied identically on read and on auto-create.
func WithDefaults(cfg CollectionConfig) CollectionConfig {
	out := cfg
	if out.ReliabilityMode == "" {
		// durable is the default: async with at-least-once guarantee.
		// A collection opts into strict (synchronous) or fast (batched, best-effort) explicitly.
		out.ReliabilityMode = ReliabilityDurable
	}
	if out.FirstEventOp == "" {
		out.FirstEventOp = FirstEventCreate
	}
	if out.Ordering.Mode == "" {
		out.Ordering.Mode = OrderingReceived
	}
	if out.SnapshotInterval == 0 {
		out.SnapshotInterval = DefaultSnapshotInterval
	}
	if out.Retention.Type == "" {
		out.Retention.Type = RetentionForever
	}
	if out.MaxEventSizeBytes == 0 {
		out.MaxEventSizeBytes = DefaultMaxEventSizeBytes
	}
	return out
}

// CollectionStats are cheap per-collection counters: entities (one cur_* document each),
// an estimated event count, and the newest event time. Events uses an estimated count
// to avoid scanning large ev_* collections. LastEventAt is zero when no events exist.
type CollectionStats struct {
	Entities    int64
	Events      int64
	LastEventAt time.Time
}

// CollectionSummary is one entry in the collection listing: logical name, basic
// statistics, and effective config (defaults applied).
type CollectionSummary struct {
	Name   string
	Stats  CollectionStats
	Config CollectionConfig
}

// StatsRepository serves the collection listing. ListCollections reports a tenant's
// logical collection names — the union of stored configs and physical ev_* namespaces,
// so auto-created collections appear even without an explicit config. Tenant database
// comes from ctx.
type StatsRepository interface {
	ListCollections(ctx context.Context) ([]string, error)
	Stats(ctx context.Context, collection string) (CollectionStats, error)
}

// CollectionRepository persists per-collection config and provisions physical collections.
// Tenant database is taken from ctx.
type CollectionRepository interface {
	GetConfig(ctx context.Context, collection string) (*CollectionConfig, error)
	SaveConfig(ctx context.Context, cfg *CollectionConfig) error
	// EnsurePhysical creates ev_*/cur_* collections and their indexes idempotently.
	EnsurePhysical(ctx context.Context, collection string) error
}
