package mongo

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/max-trifonov/letopis/internal/domain"
)

// maxAppendRetries bounds the read-tail/insert loop. Under contention each
// loser of the {eid,v} race re-reads and tries the next version; the bound
// is a safety net against pathological starvation, not an expected path.
const maxAppendRetries = 16

// Index names are explicit so we can tell which unique index a duplicate-key
// error came from: version index clash means "retry"; event_id clash means
// idempotency, which the ingest layer turns into a no-op.
const (
	idxEventVersion = "uniq_eid_v"
	idxEventID      = "uniq_event_id"
)

type EventRepo struct {
	conn *ConnManager
}

func NewEventRepo(conn *ConnManager) *EventRepo { return &EventRepo{conn: conn} }

var _ domain.EventRepository = (*EventRepo)(nil)

func (r *EventRepo) AppendEvent(ctx context.Context, collection string, ev *domain.Event) error {
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	coll := db.Collection(eventsCollection(collection))

	for range maxAppendRetries {
		last, err := lastVersion(ctx, coll, ev.EntityID)
		if err != nil {
			return err
		}
		ev.Version = last + 1
		if ev.TSStored.IsZero() {
			ev.TSStored = time.Now().UTC()
		}
		if _, err := coll.InsertOne(ctx, toEventDoc(ev)); err != nil {
			if isDupKeyOn(err, idxEventVersion) {
				continue // concurrent writer claimed this version; re-read the tail
			}
			if isDupKeyOn(err, idxEventID) {
				// Storage idempotency barrier: the original write landed, fold into no-op.
				return domain.ErrDuplicateEvent
			}
			return fmt.Errorf("mongo: append %s/%s: %w", collection, ev.EntityID, err)
		}
		return nil
	}
	return domain.ErrVersionConflict
}

// AppendEvents inserts a pre-versioned batch with a single InsertMany. Versions
// are assigned by the caller (fast batcher); a {eid,v} collision means
// misconfiguration and is rejected loudly by the unique index.
func (r *EventRepo) AppendEvents(ctx context.Context, collection string, evs []*domain.Event) error {
	if len(evs) == 0 {
		return nil
	}
	if err := validateLogical(collection); err != nil {
		return err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return err
	}
	coll := db.Collection(eventsCollection(collection))

	now := time.Now().UTC()
	docs := make([]any, len(evs))
	for i, ev := range evs {
		if ev.TSStored.IsZero() {
			ev.TSStored = now
		}
		docs[i] = toEventDoc(ev)
	}
	if _, err := coll.InsertMany(ctx, docs); err != nil {
		return fmt.Errorf("mongo: append batch %s (%d events): %w", collection, len(evs), err)
	}
	return nil
}

func (r *EventRepo) LastEvent(ctx context.Context, collection, entityID string) (*domain.Event, error) {
	if err := validateLogical(collection); err != nil {
		return nil, err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	coll := db.Collection(eventsCollection(collection))

	opts := options.FindOne().SetSort(bson.D{{Key: "v", Value: -1}})
	var d eventDoc
	if err := coll.FindOne(ctx, bson.D{{Key: "eid", Value: entityID}}, opts).Decode(&d); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return fromEventDoc(d)
}

func (r *EventRepo) ListEvents(ctx context.Context, collection string, f domain.EventFilter) ([]domain.Event, error) {
	if err := validateLogical(collection); err != nil {
		return nil, err
	}
	db, err := r.conn.DBFor(ctx)
	if err != nil {
		return nil, err
	}
	coll := db.Collection(eventsCollection(collection))

	cur, err := coll.Find(ctx, historyFilter(f), historyOptions(f))
	if err != nil {
		return nil, err
	}
	defer func() { _ = cur.Close(ctx) }()

	var out []domain.Event
	for cur.Next(ctx) {
		var d eventDoc
		if err := cur.Decode(&d); err != nil {
			return nil, err
		}
		ev, err := fromEventDoc(d)
		if err != nil {
			return nil, err
		}
		out = append(out, *ev)
	}
	return out, cur.Err()
}

// sortField maps an ordering choice to the stored document key. Version is the
// canonical arrival order; the timestamps are display orderings.
func sortField(o domain.OrderField) string {
	switch o {
	case domain.OrderSource:
		return "ts_src"
	case domain.OrderReceived:
		return "ts_rcv"
	default:
		return "v"
	}
}

// historyFilter builds the query from an EventFilter. The received-time window
// uses ts_rcv (the canonical ordering time), and the cursor predicate is a
// compound (sort-field, version) comparison so paging is stable even when the
// time field repeats across events.
func historyFilter(f domain.EventFilter) bson.D {
	field := sortField(f.OrderBy)
	filter := bson.D{}
	if f.EntityID != "" {
		filter = append(filter, bson.E{Key: "eid", Value: f.EntityID})
	}
	if f.Op != "" {
		filter = append(filter, bson.E{Key: "op", Value: string(f.Op)})
	}
	if f.Author != "" {
		filter = append(filter, bson.E{Key: "author", Value: f.Author})
	}
	if f.Source != "" {
		filter = append(filter, bson.E{Key: "src", Value: f.Source})
	}
	if window := timeWindow(f.From, f.To); window != nil {
		filter = append(filter, bson.E{Key: "ts_rcv", Value: window})
	}
	if f.Path != "" {
		// Match an event whose change list touches this exact path or one
		// nested under it ("amount" also matches "amount.cents", "items[..]").
		pat := "^" + regexp.QuoteMeta(f.Path) + `($|\.|\[)`
		filter = append(filter, bson.E{Key: "changes.p", Value: bson.D{{Key: "$regex", Value: pat}}})
	}
	if f.After != nil {
		filter = append(filter, cursorPredicate(field, f.Order, *f.After)...)
	}
	if f.MaxVersion > 0 {
		// Always filter on version regardless of display ordering — reconstruction
		// replays in version order, not by timestamp.
		filter = append(filter, bson.E{Key: "v", Value: bson.D{{Key: "$lte", Value: f.MaxVersion}}})
	}
	return filter
}

func timeWindow(from, to time.Time) bson.D {
	w := bson.D{}
	if !from.IsZero() {
		w = append(w, bson.E{Key: "$gte", Value: from})
	}
	if !to.IsZero() {
		w = append(w, bson.E{Key: "$lte", Value: to})
	}
	if len(w) == 0 {
		return nil
	}
	return w
}

// cursorPredicate restricts results to those after the cursor in sort order.
// For version ordering a single comparison suffices; for a time ordering we
// need the (ts, v) lexicographic comparison so equal timestamps do not drop or
// duplicate rows across pages.
func cursorPredicate(field string, order domain.SortOrder, pos domain.Position) bson.D {
	cmp := "$gt"
	if order == domain.OrderDesc {
		cmp = "$lt"
	}
	if field == "v" {
		return bson.D{{Key: "v", Value: bson.D{{Key: cmp, Value: pos.Version}}}}
	}
	return bson.D{{Key: "$or", Value: bson.A{
		bson.D{{Key: field, Value: bson.D{{Key: cmp, Value: pos.TS}}}},
		bson.D{{Key: field, Value: pos.TS}, {Key: "v", Value: bson.D{{Key: cmp, Value: pos.Version}}}},
	}}}
}

func historyOptions(f domain.EventFilter) *options.FindOptionsBuilder {
	field := sortField(f.OrderBy)
	dir := 1
	if f.Order == domain.OrderDesc {
		dir = -1
	}
	sort := bson.D{{Key: field, Value: dir}}
	if field != "v" {
		sort = append(sort, bson.E{Key: "v", Value: dir}) // stable tie-breaker
	}
	opts := options.Find().SetSort(sort)
	if f.Limit > 0 {
		opts.SetLimit(int64(f.Limit))
	}
	return opts
}

// lastVersion returns the highest version stored for the entity, or 0 when
// it has no history. Backed by the {eid,v} index, so it is an index scan of
// one document.
func lastVersion(ctx context.Context, coll *mongo.Collection, entityID string) (int64, error) {
	opts := options.FindOne().
		SetSort(bson.D{{Key: "v", Value: -1}}).
		SetProjection(bson.D{{Key: "v", Value: 1}})
	var d struct {
		V int64 `bson:"v"`
	}
	switch err := coll.FindOne(ctx, bson.D{{Key: "eid", Value: entityID}}, opts).Decode(&d); {
	case err == nil:
		return d.V, nil
	case errors.Is(err, mongo.ErrNoDocuments):
		return 0, nil
	default:
		return 0, err
	}
}

// ensureEventIndexes creates the ev_* indexes. Creation is idempotent: MongoDB
// ignores a request for an existing index with the same spec.
func ensureEventIndexes(ctx context.Context, db *mongo.Database, logical string) error {
	coll := db.Collection(eventsCollection(logical))
	_, err := coll.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "eid", Value: 1}, {Key: "v", Value: 1}},
			Options: options.Index().SetUnique(true).SetName(idxEventVersion),
		},
		{Keys: bson.D{{Key: "eid", Value: 1}, {Key: "ts_st", Value: -1}}},
		{
			Keys:    bson.D{{Key: "author", Value: 1}, {Key: "ts_st", Value: -1}},
			Options: options.Index().SetSparse(true),
		},
		{
			Keys:    bson.D{{Key: "event_id", Value: 1}},
			Options: options.Index().SetUnique(true).SetSparse(true).SetName(idxEventID),
		},
		{
			Keys:    bson.D{{Key: "flow.f", Value: 1}, {Key: "ts_rcv", Value: 1}},
			Options: options.Index().SetSparse(true),
		},
	})
	return err
}

// isDupKeyOn reports whether err is a duplicate-key error (code 11000) on the
// named index. The server embeds the index name in the error message, letting
// the caller distinguish a version race from an idempotency clash.
func isDupKeyOn(err error, indexName string) bool {
	match := func(code int32, msg string) bool {
		return code == 11000 && (indexName == "" || strings.Contains(msg, indexName))
	}
	if we, ok := errors.AsType[mongo.WriteException](err); ok {
		for _, e := range we.WriteErrors {
			if match(int32(e.Code), e.Message) {
				return true
			}
		}
	}
	if ce, ok := errors.AsType[mongo.CommandError](err); ok {
		return match(ce.Code, ce.Message)
	}
	return false
}
