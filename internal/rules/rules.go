// Package rules is the rules engine's pure core: the rule model, the compiler
// that turns a condition tree into predicates, and the evaluator that runs them
// against an event. No I/O — no knowledge of MongoDB, Redis or HTTP.
//
// The split between Compile (done once on save: regex compilation, structural checks)
// and Predicate.Eval (done per event: a cheap tree walk) means the hot path never
// interprets the condition JSON.
package rules

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
)

// Op is a leaf comparison operator.
type Op string

const (
	OpEq     Op = "eq"     // field equals the value
	OpNe     Op = "ne"     // field differs from the value
	OpIn     Op = "in"     // field is one of a list of values
	OpGt     Op = "gt"     // numeric: field > value
	OpGte    Op = "gte"    // numeric: field >= value
	OpLt     Op = "lt"     // numeric: field < value
	OpLte    Op = "lte"    // numeric: field <= value
	OpRegex  Op = "regex"  // field (a string) matches the pattern
	OpExists Op = "exists" // field is present (non-empty)
)

// Field names the scalar event attribute a leaf predicate reads.
type Field string

const (
	FieldOp       Field = "op"        // entity-level op: create/update/delete
	FieldEntityID Field = "entity_id" // the entity's id
	FieldAuthorID Field = "author_id" // the change's author
	FieldSource   Field = "source"    // the source system
)

func validField(f Field) bool {
	switch f {
	case FieldOp, FieldEntityID, FieldAuthorID, FieldSource:
		return true
	default:
		return false
	}
}

// Condition is one node of a rule's condition tree. A node is exactly one of:
// an All/Any/Not combinator, a Match leaf (over changes), or a scalar leaf
// (Field+Op+Value). Compile rejects a node that sets more than one kind or none at all.
// Empty combinators have fixed identity: an empty All is true, an empty Any is false —
// so a nil vs empty slice distinction is meaningful and preserved through storage/transport.
type Condition struct {
	All []Condition // every sub-condition must hold (empty ⇒ true)
	Any []Condition // at least one must hold (empty ⇒ false)
	Not *Condition  // negation of a single sub-condition

	// Scalar leaf: compare event Field with Value using Op.
	Field Field
	Op    Op
	Value any

	// Match leaf: hold when at least one element of changes matches.
	Match *Match
}

// Match is a leaf that holds when at least one element of the event's changes
// matches all of its set criteria. Path is a glob over the diff path with '*'
// matching one segment ("items.*.price" matches "items.0.price" but not "items.0.qty").
// An unset criterion is a wildcard, so {Path:"status"} matches any change at that path.
type Match struct {
	Path string
	Op   diff.Op // optional; empty matches any kind
	Old  any     // optional; nil is a wildcard, not "match null"
	New  any     // optional; nil is a wildcard, not "match null"

	// HasOld/HasNew distinguish "criterion omitted" (wildcard) from "match the JSON
	// value null". The wire and storage layers set these when the key is present in
	// the payload; an in-code Match{Path:"x"} leaves them false.
	HasOld bool
	HasNew bool
}

// ActionType is the kind of an action.
type ActionType string

const (
	ActionWebhook ActionType = "webhook"
	ActionLog     ActionType = "log"
)

// Action is one reaction a rule fires. Type selects which of Webhook/Log carries
// the built-in payload; an unrecognised Type is dispatched to an action plugin
// which reads its configuration from the opaque Params blob.
type Action struct {
	Type    ActionType
	Webhook *Webhook
	Log     *LogAction
	// Params is raw, plugin-defined config for a non-built-in action type.
	// The core doesn't interpret it; handed verbatim to the plugin's Execute.
	Params json.RawMessage
}

// Webhook is the configuration of a webhook action. SecretRef is an opaque handle
// the dispatcher resolves to a signing secret; the rule never stores the secret itself.
type Webhook struct {
	URL       string
	SecretRef string
	TimeoutMS int
	Retry     Retry
}

// Retry tunes webhook redelivery. MaxAttempts caps tries before the delivery goes to the DLQ.
type Retry struct {
	MaxAttempts int
	Backoff     string
}

type LogAction struct {
	Level string
}

// Rule is a stored rule. Version is monotonic — storage bumps it on every change —
// so a cache can tell a stale compiled rule from a current one.
type Rule struct {
	ID        string
	Name      string
	Enabled   bool
	Condition Condition
	Actions   []Action
	Version   int64
	UpdatedAt time.Time
}

// EvalEvent is the neutral view of an event a compiled rule is evaluated against.
// The mapping from domain.Event is the caller's job. Changes reuses diff.Change
// directly, so the hook hands its slice over without copying.
type EvalEvent struct {
	Op       string // entity op: create/update/delete
	EntityID string
	Author   string
	Source   string
	Changes  []diff.Change
}

// Predicate is a compiled condition: a tree walked once per event. Eval is pure
// and must not allocate on the hot path beyond what a recursive walk needs.
type Predicate interface {
	Eval(EvalEvent) bool
}

// RuleError marks an invalid rule, condition or action. Transport maps it to 400
// and surfaces Field so a client can fix the offending part.
type RuleError struct {
	Field  string
	Reason string
}

func (e *RuleError) Error() string {
	return fmt.Sprintf("rule: %s: %s", e.Field, e.Reason)
}
