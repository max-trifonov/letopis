package service

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/plugin"
	"github.com/max-trifonov/letopis/internal/rules"
	"github.com/max-trifonov/letopis/internal/tenant"
)

// actionHost resolves a rule-action type to the plugin that handles it.
// *plugin.Host satisfies it; nil disables plugin-action dispatch.
type actionHost interface {
	Action(typ string) (plugin.ActionPlugin, bool)
}

// DeliveryRequest is handed to the DeliveryPublisher when a webhook action fires.
type DeliveryRequest struct {
	Collection string
	Event      domain.Event
	RuleID     string
	RuleName   string
	Webhook    rules.Webhook
}

// DeliveryPublisher is the port the engine hands a fired webhook to. Publishing
// must be a queue append, not an HTTP call; nil disables webhooks.
type DeliveryPublisher interface {
	Publish(ctx context.Context, req DeliveryRequest) error
}

// RuleMetrics counts rule firings. Labels must be low-cardinality — action kind,
// not collection or entity. Nil disables instrumentation.
type RuleMetrics interface {
	IncRuleMatch(tenant, action string)
}

// defaultRuleCacheTTL is the fallback visibility lag when Redis pub/sub
// invalidation is unavailable: short enough to pick up edits promptly,
// long enough to avoid a per-event repository read.
const defaultRuleCacheTTL = 30 * time.Second

// RulesEngine evaluates rules post-store and fires their actions; it never fails
// the write it follows — all errors are logged and swallowed. Compiled rules are
// cached per (tenant, collection), lazily warmed, and evicted by Invalidate or
// on TTL expiry.
type RulesEngine struct {
	repo    domain.RuleRepository
	pub     DeliveryPublisher // optional; nil disables webhook delivery
	actions actionHost        // optional; nil disables plugin-action dispatch
	metrics RuleMetrics       // optional; nil disables match counting
	log     *slog.Logger
	ttl     time.Duration
	now     func() time.Time

	mu    sync.RWMutex
	cache map[ruleCacheKey]*ruleCacheEntry
}

// RulesEngineOption configures a RulesEngine at construction.
type RulesEngineOption func(*RulesEngine)

// WithDeliveryPublisher wires the webhook delivery handoff; without it webhook
// actions are dropped with a warning.
func WithDeliveryPublisher(p DeliveryPublisher) RulesEngineOption {
	return func(e *RulesEngine) { e.pub = p }
}

// WithRuleMetrics wires the match counter.
func WithRuleMetrics(m RuleMetrics) RulesEngineOption {
	return func(e *RulesEngine) { e.metrics = m }
}

// WithActionDispatcher wires the action-plugin host so non-built-in action types
// (anything other than log/webhook) are dispatched to the matching plugin. A nil
// host leaves unknown actions logged and skipped.
func WithActionDispatcher(h actionHost) RulesEngineOption {
	return func(e *RulesEngine) { e.actions = h }
}

// WithRuleCacheTTL sets the fallback cache lifetime; a non-positive value keeps
// the default.
func WithRuleCacheTTL(d time.Duration) RulesEngineOption {
	return func(e *RulesEngine) {
		if d > 0 {
			e.ttl = d
		}
	}
}

func NewRulesEngine(repo domain.RuleRepository, log *slog.Logger, opts ...RulesEngineOption) *RulesEngine {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	e := &RulesEngine{
		repo:  repo,
		log:   log.With("component", "rules-engine"),
		ttl:   defaultRuleCacheTTL,
		now:   time.Now,
		cache: map[ruleCacheKey]*ruleCacheEntry{},
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

type ruleCacheKey struct {
	tenant     string // tenant database name — the isolation boundary
	collection string
}

// ruleCacheEntry holds a collection's enabled rules already compiled, plus the
// instant the entry should be re-read as a TTL fallback.
type ruleCacheEntry struct {
	rules     []compiledRule
	expiresAt time.Time
}

// compiledRule is one enabled rule ready to evaluate: its predicate tree
// (compiled once) and the actions to fire when it holds.
type compiledRule struct {
	id      string
	name    string
	pred    rules.Predicate
	actions []rules.Action
}

// SetDeliveryPublisher wires the publisher after construction. The delivery queue
// is built later in app wiring than the engine, hence this two-phase init.
// Must be called before any Evaluate; a nil engine is a no-op.
func (e *RulesEngine) SetDeliveryPublisher(p DeliveryPublisher) {
	if e == nil {
		return
	}
	e.pub = p
}

// Evaluate runs the collection's rules against a committed event, best-effort.
// Errors are logged and swallowed; a nil engine is a no-op. Under durable reclaim
// a webhook may fire more than once — receivers deduplicate on X-HM-Delivery.
func (e *RulesEngine) Evaluate(ctx context.Context, collection string, ev domain.Event) {
	if e == nil {
		return
	}
	p, ok := tenant.FromContext(ctx)
	if !ok {
		// Missing principal is a wiring error; don't panic post-store.
		e.log.Warn("rules evaluation skipped: no tenant on context", "collection", collection)
		return
	}
	key := ruleCacheKey{tenant: p.Tenant.DatabaseName(), collection: collection}
	compiled := e.rulesFor(ctx, key)
	if len(compiled) == 0 {
		return
	}

	ee := toEvalEvent(ev)
	for _, r := range compiled {
		if !r.pred.Eval(ee) {
			continue
		}
		for _, a := range r.actions {
			e.fire(ctx, p.Tenant.ID, collection, ev, r, a)
		}
	}
}

// Invalidate drops the cached compiled rules for a (tenantDB, collection) pair.
// A nil engine is a no-op.
func (e *RulesEngine) Invalidate(tenantDB, collection string) {
	if e == nil {
		return
	}
	e.mu.Lock()
	delete(e.cache, ruleCacheKey{tenant: tenantDB, collection: collection})
	e.mu.Unlock()
}

// rulesFor returns the compiled enabled rules for the key, loading from the
// repository on a cache miss or TTL expiry. A load error degrades to "no rules"
// rather than failing the committed write.
func (e *RulesEngine) rulesFor(ctx context.Context, key ruleCacheKey) []compiledRule {
	e.mu.RLock()
	ent, ok := e.cache[key]
	e.mu.RUnlock()
	if ok && e.now().Before(ent.expiresAt) {
		return ent.rules
	}

	list, err := e.repo.List(ctx, key.collection)
	if err != nil {
		e.log.Warn("rules cache load failed; skipping evaluation for this event", "collection", key.collection, "err", err)
		return nil
	}
	compiled := e.compileEnabled(key.collection, list)

	e.mu.Lock()
	e.cache[key] = &ruleCacheEntry{rules: compiled, expiresAt: e.now().Add(e.ttl)}
	e.mu.Unlock()
	return compiled
}

// compileEnabled compiles the enabled rules of a collection. A stored rule was
// validated on write, so a compile failure here is unexpected drift — logged and
// dropped rather than aborting the whole collection's evaluation.
func (e *RulesEngine) compileEnabled(collection string, list []rules.Rule) []compiledRule {
	out := make([]compiledRule, 0, len(list))
	for i := range list {
		r := list[i]
		if !r.Enabled {
			continue
		}
		pred, err := rules.Compile(r.Condition)
		if err != nil {
			e.log.Error("stored rule failed to compile; skipping", "collection", collection, "rule_id", r.ID, "err", err)
			continue
		}
		out = append(out, compiledRule{id: r.ID, name: r.Name, pred: pred, actions: r.Actions})
	}
	return out
}

// fire runs one action of a matched rule. All failures are logged and swallowed.
func (e *RulesEngine) fire(ctx context.Context, tenantID, collection string, ev domain.Event, r compiledRule, a rules.Action) {
	switch a.Type {
	case rules.ActionLog:
		e.fireLog(collection, ev, r, a)
		e.count(tenantID, string(rules.ActionLog))
	case rules.ActionWebhook:
		e.fireWebhook(ctx, collection, ev, r, a)
		e.count(tenantID, string(rules.ActionWebhook))
	default:
		e.firePlugin(ctx, tenantID, collection, ev, r, a)
	}
}

// firePlugin dispatches a non-built-in action type to its plugin, best-effort.
// An action type with no registered plugin is unexpected drift (a stored rule was
// validated), logged and skipped.
func (e *RulesEngine) firePlugin(ctx context.Context, tenantID, collection string, ev domain.Event, r compiledRule, a rules.Action) {
	ap, ok := e.actionFor(string(a.Type))
	if !ok {
		e.log.Error("unknown action type on matched rule; skipping", "collection", collection, "rule_id", r.id, "type", a.Type)
		return
	}
	if err := ap.Execute(ctx, ev, a.Params); err != nil {
		e.log.Warn("action plugin failed; dropping", "collection", collection, "rule_id", r.id, "type", a.Type, "err", err)
	}
	e.count(tenantID, string(a.Type))
}

// actionFor resolves a plugin for the action type, guarding the optional host.
func (e *RulesEngine) actionFor(typ string) (plugin.ActionPlugin, bool) {
	if e.actions == nil {
		return nil, false
	}
	return e.actions.Action(typ)
}

// fireLog emits a structured alert at the action's level. Level is optional;
// an unset or unknown value defaults to info.
func (e *RulesEngine) fireLog(collection string, ev domain.Event, r compiledRule, a rules.Action) {
	level := slog.LevelInfo
	if a.Log != nil {
		level = parseLogLevel(a.Log.Level)
	}
	e.log.Log(context.Background(), level, "rule matched",
		"action", "log",
		"collection", collection,
		"rule_id", r.id,
		"rule_name", r.name,
		"entity_id", ev.EntityID,
		"version", ev.Version,
		"op", string(ev.Op),
	)
}

// fireWebhook hands the matched event to the delivery publisher. Without a wired
// publisher the action is dropped with a warning rather than silently lost.
func (e *RulesEngine) fireWebhook(ctx context.Context, collection string, ev domain.Event, r compiledRule, a rules.Action) {
	if a.Webhook == nil {
		e.log.Error("webhook action without configuration; skipping", "collection", collection, "rule_id", r.id)
		return
	}
	if e.pub == nil {
		e.log.Warn("webhook action fired but no delivery publisher is wired; dropping", "collection", collection, "rule_id", r.id)
		return
	}
	req := DeliveryRequest{
		Collection: collection,
		Event:      ev,
		RuleID:     r.id,
		RuleName:   r.name,
		Webhook:    *a.Webhook,
	}
	if err := e.pub.Publish(ctx, req); err != nil {
		e.log.Warn("publishing webhook delivery failed; dropping", "collection", collection, "rule_id", r.id, "err", err)
	}
}

func (e *RulesEngine) count(tenantID, action string) {
	if e.metrics != nil {
		e.metrics.IncRuleMatch(tenantID, action)
	}
}

// toEvalEvent maps a domain event to the engine's evaluation input; Changes are
// shared, not copied.
func toEvalEvent(ev domain.Event) rules.EvalEvent {
	return rules.EvalEvent{
		Op:       string(ev.Op),
		EntityID: ev.EntityID,
		Author:   ev.Author,
		Source:   ev.Source,
		Changes:  ev.Changes,
	}
}

// parseLogLevel maps a rule's log level string to a slog.Level, defaulting to
// info for an empty or unrecognized value.
func parseLogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
