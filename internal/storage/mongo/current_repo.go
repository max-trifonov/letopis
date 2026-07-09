package mongo

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/domain"
)

// CurrentRepo implements domain.CurrentRepository over the cur_* collections.
// The document _id is the entity id; it stores only the latest version,
// separate from the append-only history.
type CurrentRepo struct {
	conn *ConnManager
}

func NewCurrentRepo(conn *ConnManager) *CurrentRepo { return &CurrentRepo{conn: conn} }

var _ domain.CurrentRepository = (*CurrentRepo)(nil)

func (r *CurrentRepo) Get(ctx context.Context, collection, entityID string) (*domain.CurrentState, error) {
	if err := validateLogical(collection); err != nil {
		return nil, err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	coll := db.Collection(currentCollection(collection))

	var d currentDoc
	if err := coll.FindOne(ctx, bson.D{{Key: "_id", Value: entityID}}).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return fromCurrentDoc(d), nil
}

func (r *CurrentRepo) Upsert(ctx context.Context, collection string, st *domain.CurrentState) error {
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	coll := db.Collection(currentCollection(collection))

	doc := toCurrentDoc(st)
	_, err = coll.ReplaceOne(
		ctx,
		bson.D{{Key: "_id", Value: st.EntityID}},
		doc,
		options.Replace().SetUpsert(true),
	)
	return err
}
