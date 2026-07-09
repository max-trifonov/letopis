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

// SystemAuditRepo implements domain.AuditStore over the per-tenant ev__system
// collection — append-only, one document per action, same pattern as the entity
// event collections.
type SystemAuditRepo struct {
	conn *ConnManager

	// provisioned guards the lazy index creation so it runs once per tenant
	// database per process, not on every audit write (mirrors FlowRepo).
	provisioned sync.Map // tenant db name -> struct{}
}

func NewSystemAuditRepo(conn *ConnManager) *SystemAuditRepo { return &SystemAuditRepo{conn: conn} }

var _ domain.AuditStore = (*SystemAuditRepo)(nil)

func (r *SystemAuditRepo) Record(ctx context.Context, e domain.AuditEvent) error {
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	if err := r.ensure(ctx, db); err != nil {
		return err
	}
	if e.TS.IsZero() {
		e.TS = time.Now().UTC()
	}
	if _, err := db.Collection(SystemEvents).InsertOne(ctx, toAuditDoc(e)); err != nil {
		return fmt.Errorf("mongo: record audit %s: %w", e.Action, err)
	}
	return nil
}

// ensure creates ev__system and its lookup index once per tenant database. Both
// operations are idempotent, so a lost race only repeats harmless work.
func (r *SystemAuditRepo) ensure(ctx context.Context, db *mongo.Database) error {
	if _, done := r.provisioned.Load(db.Name()); done {
		return nil
	}
	if err := db.CreateCollection(ctx, SystemEvents); err != nil && !isNamespaceExists(err) {
		return err
	}
	// One index serves both "what happened to this collection, newest first" and
	// the tenant-wide reverse-chronological scan (a prefix of it).
	if _, err := db.Collection(SystemEvents).Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "collection", Value: 1}, {Key: "ts", Value: -1}},
		Options: options.Index().SetName("audit_collection_ts"),
	}); err != nil {
		return err
	}
	r.provisioned.Store(db.Name(), struct{}{})
	return nil
}

type auditDoc struct {
	ID         string         `bson:"_id"`
	Action     string         `bson:"action"`
	Collection string         `bson:"collection"`
	Actor      string         `bson:"actor"`
	TS         time.Time      `bson:"ts"`
	Details    map[string]any `bson:"details,omitempty"`
}

func toAuditDoc(e domain.AuditEvent) auditDoc {
	return auditDoc{
		ID:         e.ID,
		Action:     e.Action,
		Collection: e.Collection,
		Actor:      e.Actor,
		TS:         e.TS,
		Details:    e.Details,
	}
}
