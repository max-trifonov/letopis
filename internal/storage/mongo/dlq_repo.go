package mongo

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/domain"
)

// DLQRepo implements domain.DLQRepository over the per-tenant _dlq collection.
// One collection holds every rule's dead letters; rule_id and collection live in
// the document so List is a filtered read. Provisioning is lazy and idempotent.
type DLQRepo struct {
	conn *ConnManager

	provisioned sync.Map // tenant db name -> struct{}
}

func NewDLQRepo(conn *ConnManager) *DLQRepo { return &DLQRepo{conn: conn} }

var _ domain.DLQRepository = (*DLQRepo)(nil)

func (r *DLQRepo) Save(ctx context.Context, dl domain.DeadLetter) error {
	db, err := r.ready(ctx)
	if err != nil {
		return err
	}
	if dl.ID == "" {
		dl.ID = domain.NewDeadLetterID()
	}
	if dl.FailedAt.IsZero() {
		dl.FailedAt = time.Now().UTC()
	}
	if _, err := db.Collection(DLQ).InsertOne(ctx, toDLQDoc(dl)); err != nil {
		return fmt.Errorf("mongo: save dead letter %s: %w", dl.ID, err)
	}
	return nil
}

// List returns a rule's dead letters newest-first (failed_at desc, _id desc as a
// stable tiebreak), at most limit, starting strictly after the cursor. The
// {rule_id, failed_at} index serves the filter and the primary sort.
func (r *DLQRepo) List(ctx context.Context, ruleID string, limit int, after *domain.DLQCursor) ([]domain.DeadLetter, error) {
	db, err := r.ready(ctx)
	if err != nil {
		return nil, err
	}
	filter := bson.D{{Key: "rule_id", Value: ruleID}}
	if after != nil {
		// Strictly-after in (failed_at desc, _id desc) order: an earlier failed_at,
		// or the same failed_at with a smaller id.
		filter = append(filter, bson.E{Key: "$or", Value: bson.A{
			bson.D{{Key: "failed_at", Value: bson.D{{Key: "$lt", Value: after.FailedAt}}}},
			bson.D{
				{Key: "failed_at", Value: after.FailedAt},
				{Key: "_id", Value: bson.D{{Key: "$lt", Value: after.ID}}},
			},
		}})
	}
	opts := options.Find().SetSort(bson.D{{Key: "failed_at", Value: -1}, {Key: "_id", Value: -1}})
	if limit > 0 {
		opts.SetLimit(int64(limit))
	}
	cur, err := db.Collection(DLQ).Find(ctx, filter, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()

	out := []domain.DeadLetter{}
	for cur.Next(ctx) {
		var d dlqDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, fromDLQDoc(d))
	}
	return out, cur.Err()
}

func (r *DLQRepo) Get(ctx context.Context, id string) (*domain.DeadLetter, error) {
	db, err := r.ready(ctx)
	if err != nil {
		return nil, err
	}
	var d dlqDoc
	if err := db.Collection(DLQ).FindOne(ctx, bson.D{{Key: "_id", Value: id}}).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	dl := fromDLQDoc(d)
	return &dl, nil
}

func (r *DLQRepo) Delete(ctx context.Context, id string) error {
	db, err := r.ready(ctx)
	if err != nil {
		return err
	}
	res, err := db.Collection(DLQ).DeleteOne(ctx, bson.D{{Key: "_id", Value: id}})
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return domain.ErrNotFound
	}
	return nil
}

// Count returns how many dead letters a rule has. An empty ruleID counts
// the whole tenant. It uses the {rule_id,...} index prefix, so it is cheap.
func (r *DLQRepo) Count(ctx context.Context, ruleID string) (int64, error) {
	db, err := r.ready(ctx)
	if err != nil {
		return 0, err
	}
	filter := bson.D{}
	if ruleID != "" {
		filter = bson.D{{Key: "rule_id", Value: ruleID}}
	}
	return db.Collection(DLQ).CountDocuments(ctx, filter)
}

func (r *DLQRepo) ready(ctx context.Context) (*mongo.Database, error) {
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.ensure(ctx, db); err != nil {
		return nil, err
	}
	return db, nil
}

func (r *DLQRepo) ensure(ctx context.Context, db *mongo.Database) error {
	if _, done := r.provisioned.Load(db.Name()); done {
		return nil
	}
	if err := db.CreateCollection(ctx, DLQ); err != nil && !isNamespaceExists(err) {
		return err
	}
	// {rule_id, failed_at desc} serves the per-rule newest-first listing and Count;
	// the trailing _id keeps the cursor tiebreak on the index.
	if _, err := db.Collection(DLQ).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "rule_id", Value: 1}, {Key: "failed_at", Value: -1}, {Key: "_id", Value: -1}},
		Options: options.Index().SetName("dlq_rule_failed_at"),
	}); err != nil {
		return err
	}
	r.provisioned.Store(db.Name(), struct{}{})
	return nil
}

// dlqDoc is the BSON shape of a dead letter. Body is stored as raw bytes so the
// exact signed payload round-trips for a faithful redeliver.
type dlqDoc struct {
	ID         string    `bson:"_id"`
	RuleID     string    `bson:"rule_id"`
	Collection string    `bson:"collection"`
	DeliveryID string    `bson:"delivery_id"`
	URL        string    `bson:"url"`
	SecretRef  string    `bson:"secret_ref,omitempty"`
	Body       []byte    `bson:"body"`
	Attempts   int       `bson:"attempts"`
	LastError  string    `bson:"last_error,omitempty"`
	FailedAt   time.Time `bson:"failed_at"`
}

func toDLQDoc(dl domain.DeadLetter) dlqDoc {
	return dlqDoc{
		ID:         dl.ID,
		RuleID:     dl.RuleID,
		Collection: dl.Collection,
		DeliveryID: dl.DeliveryID,
		URL:        dl.URL,
		SecretRef:  dl.SecretRef,
		Body:       dl.Body,
		Attempts:   dl.Attempts,
		LastError:  dl.LastError,
		FailedAt:   dl.FailedAt,
	}
}

func fromDLQDoc(d dlqDoc) domain.DeadLetter {
	return domain.DeadLetter{
		ID:         d.ID,
		RuleID:     d.RuleID,
		Collection: d.Collection,
		DeliveryID: d.DeliveryID,
		URL:        d.URL,
		SecretRef:  d.SecretRef,
		Body:       d.Body,
		Attempts:   d.Attempts,
		LastError:  d.LastError,
		FailedAt:   d.FailedAt,
	}
}
