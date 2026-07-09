package mongo

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/domain"
)

// idxSnapVersion is the unique {eid:1,v:-1} snapshot index. The descending v
// serves double duty: enforces one snapshot per {entity,version} and answers
// Nearest (v ≤ target lookup) with a one-document index scan.
const idxSnapVersion = "uniq_snap_eid_v"

type SnapshotRepo struct {
	conn *ConnManager
}

func NewSnapshotRepo(conn *ConnManager) *SnapshotRepo { return &SnapshotRepo{conn: conn} }

var _ domain.SnapshotRepository = (*SnapshotRepo)(nil)

func (r *SnapshotRepo) Save(ctx context.Context, collection string, snap *domain.Snapshot) error {
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	coll := db.Collection(snapCollection(collection))

	// Upsert on {eid,v}: the builder is best-effort and may re-run, so a
	// re-snap replaces rather than collides with the unique index.
	_, err = coll.ReplaceOne(
		ctx,
		bson.D{{Key: "eid", Value: snap.EntityID}, {Key: "v", Value: snap.Version}},
		toSnapDoc(snap),
		options.Replace().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("mongo: save snapshot %s/%s v%d: %w", collection, snap.EntityID, snap.Version, err)
	}
	return nil
}

func (r *SnapshotRepo) Nearest(ctx context.Context, collection, entityID string, maxVersion int64) (*domain.Snapshot, error) {
	if err := validateLogical(collection); err != nil {
		return nil, err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	coll := db.Collection(snapCollection(collection))

	// Highest version at or below the target: a descending-version sort over the
	// {eid,v} index, first document wins.
	opts := options.FindOne().SetSort(bson.D{{Key: "v", Value: -1}})
	filter := bson.D{
		{Key: "eid", Value: entityID},
		{Key: "v", Value: bson.D{{Key: "$lte", Value: maxVersion}}},
	}
	var d snapDoc
	if err := coll.FindOne(ctx, filter, opts).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return fromSnapDoc(d), nil
}

// ensureSnapshotIndexes creates the sn_* unique {eid:1,v:-1} index. Like the
// event indexes, creation is idempotent.
func ensureSnapshotIndexes(ctx context.Context, db *mongo.Database, logical string) error {
	coll := db.Collection(snapCollection(logical))
	_, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "eid", Value: 1}, {Key: "v", Value: -1}},
		Options: options.Index().SetUnique(true).SetName(idxSnapVersion),
	})
	return err
}
