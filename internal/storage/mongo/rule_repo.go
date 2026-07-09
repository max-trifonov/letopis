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

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/rules"
)

// idxRuleName is the unique {collection,name} index. A clash surfaces as
// domain.ErrRuleNameConflict so the CRUD layer can answer 409.
const idxRuleName = "uniq_rule_collection_name"

// RuleRepo implements domain.RuleRepository over the per-tenant _rules
// collection. One collection holds every rule; the logical collection name
// lives in the document so List/ListAll are filtered reads and the cache warms
// with a single ListAll. The raw rules.Rule is stored; compilation happens on
// cache load.
type RuleRepo struct {
	conn *ConnManager

	// provisioned guards the lazy index creation so it runs once per tenant
	// database per process (mirrors FlowRepo/SystemAuditRepo). _rules is a system
	// collection with no auto-create config to hang provisioning off.
	provisioned sync.Map // tenant db name -> struct{}
}

func NewRuleRepo(conn *ConnManager) *RuleRepo { return &RuleRepo{conn: conn} }

var _ domain.RuleRepository = (*RuleRepo)(nil)

func (r *RuleRepo) Create(ctx context.Context, collection string, rule *rules.Rule) error {
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.ready(ctx)
	if err != nil {
		return err
	}
	if rule.ID == "" {
		rule.ID = domain.NewRuleID()
	}
	rule.Version = 1
	rule.UpdatedAt = time.Now().UTC()
	if _, err := db.Collection(Rules).InsertOne(ctx, toRuleDoc(collection, rule)); err != nil {
		if isDupKeyOn(err, idxRuleName) {
			return domain.ErrRuleNameConflict
		}
		return fmt.Errorf("mongo: create rule %s/%s: %w", collection, rule.Name, err)
	}
	return nil
}

func (r *RuleRepo) Get(ctx context.Context, collection, id string) (*rules.Rule, error) {
	if err := validateLogical(collection); err != nil {
		return nil, err
	}
	db, err := r.ready(ctx)
	if err != nil {
		return nil, err
	}
	var d ruleDoc
	filter := bson.D{{Key: "_id", Value: id}, {Key: "collection", Value: collection}}
	if err := db.Collection(Rules).FindOne(ctx, filter).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return fromRuleDoc(d), nil
}

func (r *RuleRepo) List(ctx context.Context, collection string) ([]rules.Rule, error) {
	if err := validateLogical(collection); err != nil {
		return nil, err
	}
	db, err := r.ready(ctx)
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	cur, err := db.Collection(Rules).Find(ctx, bson.D{{Key: "collection", Value: collection}}, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()

	out := []rules.Rule{}
	for cur.Next(ctx) {
		var d ruleDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out = append(out, *fromRuleDoc(d))
	}
	return out, cur.Err()
}

func (r *RuleRepo) Update(ctx context.Context, collection string, rule *rules.Rule) error {
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.ready(ctx)
	if err != nil {
		return err
	}
	rule.UpdatedAt = time.Now().UTC()
	// Bump the version atomically and replace the body in one round-trip:
	// $set the new fields, $inc the version; FindOneAndUpdate returns the new doc
	// so the caller and audit see the version that took effect.
	filter := bson.D{{Key: "_id", Value: rule.ID}, {Key: "collection", Value: collection}}
	update := bson.D{
		{Key: "$set", Value: ruleBodyDoc(rule)},
		{Key: "$inc", Value: bson.D{{Key: "version", Value: int64(1)}}},
	}
	opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
	var d ruleDoc
	if err := db.Collection(Rules).FindOneAndUpdate(ctx, filter, update, opts).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return domain.ErrNotFound
		}
		if isDupKeyOn(err, idxRuleName) {
			return domain.ErrRuleNameConflict
		}
		return fmt.Errorf("mongo: update rule %s/%s: %w", collection, rule.ID, err)
	}
	rule.Version = d.Version
	return nil
}

func (r *RuleRepo) Delete(ctx context.Context, collection, id string) error {
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.ready(ctx)
	if err != nil {
		return err
	}
	filter := bson.D{{Key: "_id", Value: id}, {Key: "collection", Value: collection}}
	res, err := db.Collection(Rules).DeleteOne(ctx, filter)
	if err != nil {
		return err
	}
	if res.DeletedCount == 0 {
		return domain.ErrNotFound
	}
	return nil
}

func (r *RuleRepo) ListAll(ctx context.Context) (map[string][]rules.Rule, error) {
	db, err := r.ready(ctx)
	if err != nil {
		return nil, err
	}
	opts := options.Find().SetSort(bson.D{{Key: "collection", Value: 1}, {Key: "_id", Value: 1}})
	cur, err := db.Collection(Rules).Find(ctx, bson.D{}, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()

	out := map[string][]rules.Rule{}
	for cur.Next(ctx) {
		var d ruleDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		out[d.Collection] = append(out[d.Collection], *fromRuleDoc(d))
	}
	return out, cur.Err()
}

func (r *RuleRepo) ready(ctx context.Context) (*mongo.Database, error) {
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	if err := r.ensure(ctx, db); err != nil {
		return nil, err
	}
	return db, nil
}

func (r *RuleRepo) ensure(ctx context.Context, db *mongo.Database) error {
	if _, done := r.provisioned.Load(db.Name()); done {
		return nil
	}
	if err := db.CreateCollection(ctx, Rules); err != nil && !isNamespaceExists(err) {
		return err
	}
	// {collection,_id} serves Get and the per-collection List; the unique
	// {collection,name} enforces name uniqueness within a collection.
	if _, err := db.Collection(Rules).Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "collection", Value: 1}, {Key: "_id", Value: 1}}},
		{
			Keys:    bson.D{{Key: "collection", Value: 1}, {Key: "name", Value: 1}},
			Options: options.Index().SetUnique(true).SetName(idxRuleName),
		},
	}); err != nil {
		return err
	}
	r.provisioned.Store(db.Name(), struct{}{})
	return nil
}

// ruleDoc is the BSON shape of a stored rule. Condition and actions are nested
// sub-documents, consistent with how changes/flow embed in ev_*.
type ruleDoc struct {
	ID         string      `bson:"_id"`
	Collection string      `bson:"collection"`
	Name       string      `bson:"name"`
	Enabled    bool        `bson:"enabled"`
	Condition  condDoc     `bson:"condition"`
	Actions    []actionDoc `bson:"actions"`
	Version    int64       `bson:"version"`
	UpdatedAt  time.Time   `bson:"updated_at"`
}

type condDoc struct {
	// All/Any deliberately omit `omitempty`: an empty combinator (all ⇒ true,
	// any ⇒ false) must round-trip distinct from an absent one, so a nil slice
	// must marshal to BSON null and an empty slice to [], not both vanish.
	All   []condDoc `bson:"all"`
	Any   []condDoc `bson:"any"`
	Not   *condDoc  `bson:"not,omitempty"`
	Field string    `bson:"field,omitempty"`
	Op    string    `bson:"op,omitempty"`
	Value any       `bson:"value,omitempty"`
	Match *matchDoc `bson:"match,omitempty"`
}

type matchDoc struct {
	Path   string `bson:"path"`
	Op     string `bson:"op,omitempty"`
	Old    any    `bson:"old,omitempty"`
	New    any    `bson:"new,omitempty"`
	HasOld bool   `bson:"has_old,omitempty"`
	HasNew bool   `bson:"has_new,omitempty"`
}

type actionDoc struct {
	Type    string      `bson:"type"`
	Webhook *webhookDoc `bson:"webhook,omitempty"`
	Log     *logDoc     `bson:"log,omitempty"`
}

type webhookDoc struct {
	URL       string `bson:"url"`
	SecretRef string `bson:"secret_ref,omitempty"`
	TimeoutMS int    `bson:"timeout_ms,omitempty"`
	Retry     struct {
		MaxAttempts int    `bson:"max_attempts,omitempty"`
		Backoff     string `bson:"backoff,omitempty"`
	} `bson:"retry,omitempty"`
}

type logDoc struct {
	Level string `bson:"level,omitempty"`
}

func toRuleDoc(collection string, r *rules.Rule) ruleDoc {
	return ruleDoc{
		ID:         r.ID,
		Collection: collection,
		Name:       r.Name,
		Enabled:    r.Enabled,
		Condition:  toCondDoc(r.Condition),
		Actions:    toActionDocs(r.Actions),
		Version:    r.Version,
		UpdatedAt:  r.UpdatedAt,
	}
}

// ruleBodyDoc is the $set payload for Update: only the mutable body fields.
// _id/collection are immutable, and version is $inc-ed separately so setting it
// here would race.
func ruleBodyDoc(r *rules.Rule) bson.D {
	return bson.D{
		{Key: "name", Value: r.Name},
		{Key: "enabled", Value: r.Enabled},
		{Key: "condition", Value: toCondDoc(r.Condition)},
		{Key: "actions", Value: toActionDocs(r.Actions)},
		{Key: "updated_at", Value: r.UpdatedAt},
	}
}

func toCondDoc(c rules.Condition) condDoc {
	d := condDoc{
		Field: string(c.Field),
		Op:    string(c.Op),
		Value: c.Value,
	}
	// make (not append-from-nil) so an empty combinator keeps its non-nil identity
	// through marshaling — nil and empty slice must stay distinct.
	if c.All != nil {
		d.All = make([]condDoc, len(c.All))
		for i, sub := range c.All {
			d.All[i] = toCondDoc(sub)
		}
	}
	if c.Any != nil {
		d.Any = make([]condDoc, len(c.Any))
		for i, sub := range c.Any {
			d.Any[i] = toCondDoc(sub)
		}
	}
	if c.Not != nil {
		n := toCondDoc(*c.Not)
		d.Not = &n
	}
	if c.Match != nil {
		d.Match = &matchDoc{
			Path:   c.Match.Path,
			Op:     string(c.Match.Op),
			Old:    c.Match.Old,
			New:    c.Match.New,
			HasOld: c.Match.HasOld,
			HasNew: c.Match.HasNew,
		}
	}
	return d
}

func fromCondDoc(d condDoc) rules.Condition {
	c := rules.Condition{
		Field: rules.Field(d.Field),
		Op:    rules.Op(d.Op),
		Value: canonicalJSON(d.Value),
	}
	// Only attach All/Any when present; nil and empty slice must stay distinct
	// for the empty-combinator identity (nil all ⇒ true, nil any ⇒ false).
	if d.All != nil {
		c.All = make([]rules.Condition, len(d.All))
		for i, sub := range d.All {
			c.All[i] = fromCondDoc(sub)
		}
	}
	if d.Any != nil {
		c.Any = make([]rules.Condition, len(d.Any))
		for i, sub := range d.Any {
			c.Any[i] = fromCondDoc(sub)
		}
	}
	if d.Not != nil {
		n := fromCondDoc(*d.Not)
		c.Not = &n
	}
	if d.Match != nil {
		c.Match = &rules.Match{
			Path:   d.Match.Path,
			Op:     diff.Op(d.Match.Op),
			Old:    canonicalJSON(d.Match.Old),
			New:    canonicalJSON(d.Match.New),
			HasOld: d.Match.HasOld,
			HasNew: d.Match.HasNew,
		}
	}
	return c
}

func toActionDocs(actions []rules.Action) []actionDoc {
	out := make([]actionDoc, len(actions))
	for i, a := range actions {
		ad := actionDoc{Type: string(a.Type)}
		if a.Webhook != nil {
			w := &webhookDoc{URL: a.Webhook.URL, SecretRef: a.Webhook.SecretRef, TimeoutMS: a.Webhook.TimeoutMS}
			w.Retry.MaxAttempts = a.Webhook.Retry.MaxAttempts
			w.Retry.Backoff = a.Webhook.Retry.Backoff
			ad.Webhook = w
		}
		if a.Log != nil {
			ad.Log = &logDoc{Level: a.Log.Level}
		}
		out[i] = ad
	}
	return out
}

func fromActionDocs(docs []actionDoc) []rules.Action {
	out := make([]rules.Action, len(docs))
	for i, d := range docs {
		a := rules.Action{Type: rules.ActionType(d.Type)}
		if d.Webhook != nil {
			a.Webhook = &rules.Webhook{
				URL:       d.Webhook.URL,
				SecretRef: d.Webhook.SecretRef,
				TimeoutMS: d.Webhook.TimeoutMS,
				Retry:     rules.Retry{MaxAttempts: d.Webhook.Retry.MaxAttempts, Backoff: d.Webhook.Retry.Backoff},
			}
		}
		if d.Log != nil {
			a.Log = &rules.LogAction{Level: d.Log.Level}
		}
		out[i] = a
	}
	return out
}

func fromRuleDoc(d ruleDoc) *rules.Rule {
	return &rules.Rule{
		ID:        d.ID,
		Name:      d.Name,
		Enabled:   d.Enabled,
		Condition: fromCondDoc(d.Condition),
		Actions:   fromActionDocs(d.Actions),
		Version:   d.Version,
		UpdatedAt: d.UpdatedAt,
	}
}
