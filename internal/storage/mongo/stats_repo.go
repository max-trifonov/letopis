package mongo

import (
	"context"
	"errors"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/domain"
)

// StatsRepo implements domain.StatsRepository over the per-tenant database. The
// counters it returns are deliberately cheap — no full scans.
type StatsRepo struct {
	conn *ConnManager
}

func NewStatsRepo(conn *ConnManager) *StatsRepo { return &StatsRepo{conn: conn} }

var _ domain.StatsRepository = (*StatsRepo)(nil)

// ListCollections returns the tenant's logical collection names: the union of
// physical ev_* namespaces (so auto-created collections appear) and stored
// _collections configs (so a collection configured but not yet written to also
// appears). The result is sorted for a stable listing.
func (r *StatsRepo) ListCollections(ctx context.Context) ([]string, error) {
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}

	physical, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}
	set := map[string]struct{}{}
	for _, name := range physical {
		if isUserEventCollection(name) {
			set[logicalFromEvents(name)] = struct{}{}
		}
	}

	// Configs may exist before any event is written (SaveConfig does not provision
	// the physical collections), so fold in the _collections ids too. Only the _id
	// is needed.
	cur, err := db.Collection(CollectionsConfig).Find(ctx, bson.D{}, options.Find().SetProjection(bson.D{{Key: "_id", Value: 1}}))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		var d struct {
			ID string `bson:"_id"`
		}
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		set[d.ID] = struct{}{}
	}
	if err := cur.Err(); err != nil {
		return nil, err
	}

	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

// Stats returns the cheap counters for one collection: entity count from cur_*
// (one doc per entity), estimated event count from ev_* metadata, and newest
// event time. A non-existent or empty collection yields zeros, not an error.
func (r *StatsRepo) Stats(ctx context.Context, collection string) (domain.CollectionStats, error) {
	if err := validateLogical(collection); err != nil {
		return domain.CollectionStats{}, err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return domain.CollectionStats{}, err
	}

	entities, err := db.Collection(currentCollection(collection)).CountDocuments(ctx, bson.D{})
	if err != nil {
		return domain.CollectionStats{}, err
	}

	events, err := db.Collection(eventsCollection(collection)).EstimatedDocumentCount(ctx)
	if err != nil {
		return domain.CollectionStats{}, err
	}

	lastAt, err := lastEventTime(ctx, db.Collection(eventsCollection(collection)))
	if err != nil {
		return domain.CollectionStats{}, err
	}

	return domain.CollectionStats{Entities: entities, Events: events, LastEventAt: lastAt}, nil
}

// lastEventTime returns the newest stored event time in the collection. It reads
// the last document in natural (insertion) order rather than sorting on ts_st:
// ts_st is assigned at append from the wall clock and events are append-only, so
// the last-inserted document carries the latest activity time. Reading it via a
// reverse $natural scan limited to one document is O(1) and needs no dedicated
// ts_st index, avoiding a blocking in-memory sort on a large ev_*.
func lastEventTime(ctx context.Context, coll *mongo.Collection) (time.Time, error) {
	opts := options.FindOne().
		SetSort(bson.D{{Key: "$natural", Value: -1}}).
		SetProjection(bson.D{{Key: "ts_st", Value: 1}})
	var d struct {
		TSStored time.Time `bson:"ts_st"`
	}
	switch err := coll.FindOne(ctx, bson.D{}, opts).Decode(&d); {
	case err == nil:
		return d.TSStored, nil
	case errors.Is(err, mongo.ErrNoDocuments):
		return time.Time{}, nil
	default:
		return time.Time{}, err
	}
}
