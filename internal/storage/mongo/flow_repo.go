package mongo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/domain"
)

// FlowRepo implements domain.FlowStore over the per-tenant ev__flow collection
// and a fan-out over ev_* for cross-collection flow queries.
type FlowRepo struct {
	conn *ConnManager

	// provisioned guards the lazy index creation once per tenant database.
	// ev__flow is a system collection with no auto-create config, so the write
	// path provisions it itself.
	provisioned sync.Map // tenant db name -> struct{}
}

func NewFlowRepo(conn *ConnManager) *FlowRepo { return &FlowRepo{conn: conn} }

var _ domain.FlowStore = (*FlowRepo)(nil)

func (r *FlowRepo) AppendActivity(ctx context.Context, a *domain.Activity) error {
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	if err := r.ensure(ctx, db); err != nil {
		return err
	}
	if a.TSStored.IsZero() {
		a.TSStored = time.Now().UTC()
	}
	if _, err := db.Collection(FlowActivities).InsertOne(ctx, toActivityDoc(a)); err != nil {
		return fmt.Errorf("mongo: append activity %s: %w", a.ActivityID, err)
	}
	return nil
}

func (r *FlowRepo) FlowActivities(ctx context.Context, flowID string) ([]domain.Activity, error) {
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetSort(bson.D{{Key: "ts_rcv", Value: 1}})
	cur, err := db.Collection(FlowActivities).Find(ctx, bson.D{{Key: "flow", Value: flowID}}, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()

	var out []domain.Activity
	for cur.Next(ctx) {
		var d activityDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, fromActivityDoc(d))
	}
	return out, cur.Err()
}

// FlowEvents sweeps the tenant's user ev_* collections for events tagged with
// the flow. The collection count is in the tens, so a serial fan-out backed by
// the sparse {flow.f,ts_rcv} index is acceptable.
func (r *FlowRepo) FlowEvents(ctx context.Context, flowID string) ([]domain.FlowEvent, error) {
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	names, err := db.ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, err
	}

	opts := options.Find().SetSort(bson.D{{Key: "ts_rcv", Value: 1}})
	var out []domain.FlowEvent
	for _, name := range names {
		if !isUserEventCollection(name) {
			continue
		}
		logical := logicalFromEvents(name)
		cur, err := db.Collection(name).Find(ctx, bson.D{{Key: "flow.f", Value: flowID}}, opts)
		if err != nil {
			return nil, err
		}
		for cur.Next(ctx) {
			var d eventDoc
			if err := cur.Decode(&d); err != nil {
				_ = cur.Close(ctx)
				return nil, err
			}
			ev, err := fromEventDoc(d)
			if err != nil {
				_ = cur.Close(ctx)
				return nil, err
			}
			out = append(out, domain.FlowEvent{Collection: logical, Event: *ev})
		}
		if err := cur.Err(); err != nil {
			_ = cur.Close(ctx)
			return nil, err
		}
		_ = cur.Close(ctx)
	}
	return out, nil
}

// ensure creates ev__flow and its indexes once per tenant database. Both
// operations are idempotent, so a lost race only repeats harmless work.
func (r *FlowRepo) ensure(ctx context.Context, db *mongo.Database) error {
	if _, done := r.provisioned.Load(db.Name()); done {
		return nil
	}
	if err := db.CreateCollection(ctx, FlowActivities); err != nil && !isNamespaceExists(err) {
		return err
	}
	if err := ensureFlowIndexes(ctx, db); err != nil {
		return err
	}
	r.provisioned.Store(db.Name(), struct{}{})
	return nil
}

// ensureFlowIndexes creates the ev__flow indexes: flow lookup in received order,
// the entity→flows reverse index, and a unique sparse aid for idempotency.
func ensureFlowIndexes(ctx context.Context, db *mongo.Database) error {
	coll := db.Collection(FlowActivities)
	_, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "flow", Value: 1}, {Key: "ts_rcv", Value: 1}}},
		{Keys: bson.D{{Key: "refs.c", Value: 1}, {Key: "refs.eid", Value: 1}, {Key: "ts_rcv", Value: -1}}},
		{
			Keys:    bson.D{{Key: "aid", Value: 1}},
			Options: options.Index().SetUnique(true).SetSparse(true).SetName("uniq_aid"),
		},
	})
	return err
}
