package mongo

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/domain"
)

// namespaceExists is the MongoDB error code for "collection already exists".
// CreateCollection is otherwise not idempotent, so we treat this as success.
const namespaceExists = 48

type CollectionRepo struct {
	conn *ConnManager
}

func NewCollectionRepo(conn *ConnManager) *CollectionRepo { return &CollectionRepo{conn: conn} }

var _ domain.CollectionRepository = (*CollectionRepo)(nil)

func (r *CollectionRepo) GetConfig(ctx context.Context, collection string) (*domain.CollectionConfig, error) {
	if err := validateLogical(collection); err != nil {
		return nil, err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	var d collectionConfigDoc
	if err := db.Collection(CollectionsConfig).FindOne(ctx, bson.D{{Key: "_id", Value: collection}}).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return fromConfigDoc(d), nil
}

func (r *CollectionRepo) SaveConfig(ctx context.Context, cfg *domain.CollectionConfig) error {
	if err := validateLogical(cfg.Name); err != nil {
		return err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	_, err = db.Collection(CollectionsConfig).ReplaceOne(
		ctx,
		bson.D{{Key: "_id", Value: cfg.Name}},
		toConfigDoc(cfg),
		options.Replace().SetUpsert(true),
	)
	return err
}

func (r *CollectionRepo) EnsurePhysical(ctx context.Context, collection string) error {
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	for _, name := range []string{eventsCollection(collection), currentCollection(collection), snapCollection(collection)} {
		if err := db.CreateCollection(ctx, name); err != nil && !isNamespaceExists(err) {
			return err
		}
	}
	if err := ensureEventIndexes(ctx, db, collection); err != nil {
		return err
	}
	return ensureSnapshotIndexes(ctx, db, collection)
}

func isNamespaceExists(err error) bool {
	if ce, ok := errors.AsType[mongo.CommandError](err); ok {
		return ce.Code == namespaceExists
	}
	return false
}

type collectionConfigDoc struct {
	ID                string               `bson:"_id"`
	ReliabilityMode   string               `bson:"reliability_mode"`
	SnapshotInterval  int                  `bson:"snapshot_interval"`
	Retention         retentionDoc         `bson:"retention"`
	MaxEventSizeBytes int64                `bson:"max_event_size_bytes"`
	FirstEventOp      string               `bson:"first_event_op"`
	Ordering          orderingDoc          `bson:"ordering"`
	ArrayKeys         map[string]string    `bson:"array_keys,omitempty"`
	Plugins           map[string]pluginDoc `bson:"plugins,omitempty"`
}

type retentionDoc struct {
	Type string `bson:"type"`
	Days int    `bson:"days,omitempty"`
	Keep int    `bson:"keep,omitempty"`
}

type orderingDoc struct {
	Mode            string `bson:"mode"`
	ReorderWindowMS int    `bson:"reorder_window_ms,omitempty"`
}

type pluginDoc struct {
	Enabled  bool           `bson:"enabled"`
	FailMode string         `bson:"fail_mode,omitempty"`
	Params   map[string]any `bson:"params,omitempty"`
}

func toConfigDoc(cfg *domain.CollectionConfig) collectionConfigDoc {
	d := collectionConfigDoc{
		ID:                cfg.Name,
		ReliabilityMode:   string(cfg.ReliabilityMode),
		SnapshotInterval:  cfg.SnapshotInterval,
		Retention:         retentionDoc{Type: string(cfg.Retention.Type), Days: cfg.Retention.Days, Keep: cfg.Retention.Keep},
		MaxEventSizeBytes: cfg.MaxEventSizeBytes,
		FirstEventOp:      string(cfg.FirstEventOp),
		Ordering:          orderingDoc{Mode: string(cfg.Ordering.Mode), ReorderWindowMS: cfg.Ordering.ReorderWindowMS},
		ArrayKeys:         cfg.ArrayKeys,
	}
	if len(cfg.Plugins) > 0 {
		d.Plugins = make(map[string]pluginDoc, len(cfg.Plugins))
		for k, p := range cfg.Plugins {
			d.Plugins[k] = pluginDoc{Enabled: p.Enabled, FailMode: string(p.FailMode), Params: p.Params}
		}
	}
	return d
}

func fromConfigDoc(d collectionConfigDoc) *domain.CollectionConfig {
	cfg := &domain.CollectionConfig{
		Name:              d.ID,
		ReliabilityMode:   domain.ReliabilityMode(d.ReliabilityMode),
		SnapshotInterval:  d.SnapshotInterval,
		Retention:         domain.Retention{Type: domain.RetentionType(d.Retention.Type), Days: d.Retention.Days, Keep: d.Retention.Keep},
		MaxEventSizeBytes: d.MaxEventSizeBytes,
		FirstEventOp:      domain.FirstEventOp(d.FirstEventOp),
		Ordering:          domain.Ordering{Mode: domain.OrderingMode(d.Ordering.Mode), ReorderWindowMS: d.Ordering.ReorderWindowMS},
		ArrayKeys:         d.ArrayKeys,
	}
	if len(d.Plugins) > 0 {
		cfg.Plugins = make(map[string]domain.PluginConfig, len(d.Plugins))
		for k, p := range d.Plugins {
			cfg.Plugins[k] = domain.PluginConfig{Enabled: p.Enabled, FailMode: domain.FailMode(p.FailMode), Params: p.Params}
		}
	}
	return cfg
}
