package rules

import (
	"testing"

	"github.com/max-trifonov/letopis/internal/diff"
)

// scalar builds a scalar-leaf condition for terse table rows.
func scalar(f Field, op Op, v any) Condition { return Condition{Field: f, Op: op, Value: v} }

// evalOK compiles c and evaluates it against e, failing the test on a compile
// error (the condition under test is expected to be valid).
func evalOK(t *testing.T, c Condition, e EvalEvent) bool {
	t.Helper()
	p, err := Compile(c)
	if err != nil {
		t.Fatalf("Compile(%+v): unexpected error %v", c, err)
	}
	return p.Eval(e)
}

func TestScalarOperators(t *testing.T) {
	ev := EvalEvent{Op: "update", EntityID: "42", Author: "77", Source: "crm"}

	tests := []struct {
		name string
		cond Condition
		want bool
	}{
		{"eq match", scalar(FieldOp, OpEq, "update"), true},
		{"eq miss", scalar(FieldOp, OpEq, "create"), false},
		{"ne match", scalar(FieldOp, OpNe, "create"), true},
		{"ne miss", scalar(FieldOp, OpNe, "update"), false},
		{"in match", scalar(FieldAuthorID, OpIn, []any{"1", "77"}), true},
		{"in miss", scalar(FieldAuthorID, OpIn, []any{"1", "2"}), false},
		{"gt numeric match", scalar(FieldEntityID, OpGt, float64(10)), true},
		{"gt numeric miss", scalar(FieldEntityID, OpGt, float64(100)), false},
		{"gte boundary", scalar(FieldEntityID, OpGte, float64(42)), true},
		{"lt match", scalar(FieldEntityID, OpLt, float64(50)), true},
		{"lte boundary", scalar(FieldEntityID, OpLte, float64(42)), true},
		{"regex match", scalar(FieldSource, OpRegex, "^cr"), true},
		{"regex miss", scalar(FieldSource, OpRegex, "^doc"), false},
		{"exists present", scalar(FieldSource, OpExists, nil), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := evalOK(t, tt.cond, ev); got != tt.want {
				t.Errorf("Eval = %v, want %v", got, tt.want)
			}
		})
	}
}

// A missing field: exists is false, ne holds against any concrete value, eq/in
// miss, and a numeric comparison is a clean miss rather than a panic.
func TestScalarMissingField(t *testing.T) {
	empty := EvalEvent{Op: "update"} // Source unset
	cases := []struct {
		cond Condition
		want bool
	}{
		{scalar(FieldSource, OpExists, nil), false},
		{scalar(FieldSource, OpEq, "crm"), false},
		{scalar(FieldSource, OpNe, "crm"), true},
		{scalar(FieldSource, OpIn, []any{"crm"}), false},
		{scalar(FieldSource, OpGt, float64(1)), false},
	}
	for _, tc := range cases {
		if got := evalOK(t, tc.cond, empty); got != tc.want {
			t.Errorf("%+v Eval = %v, want %v", tc.cond, got, tc.want)
		}
	}
}

// A non-numeric field under an ordering operator is incomparable → false, and
// must not panic (type-safety, Ш0 edge case).
func TestNumericComparisonAcrossTypes(t *testing.T) {
	ev := EvalEvent{Source: "crm"} // a word, not a number
	if evalOK(t, scalar(FieldSource, OpGt, float64(5)), ev) {
		t.Error("gt against a non-numeric field should be false")
	}
}

func TestCombinators(t *testing.T) {
	ev := EvalEvent{Op: "update", Author: "77"}

	all := Condition{All: []Condition{
		scalar(FieldOp, OpEq, "update"),
		scalar(FieldAuthorID, OpIn, []any{"42", "77"}),
	}}
	if !evalOK(t, all, ev) {
		t.Error("all of two satisfied clauses should hold")
	}

	allMiss := Condition{All: []Condition{
		scalar(FieldOp, OpEq, "update"),
		scalar(FieldAuthorID, OpEq, "1"),
	}}
	if evalOK(t, allMiss, ev) {
		t.Error("all with a failing clause should not hold")
	}

	anyC := Condition{Any: []Condition{
		scalar(FieldOp, OpEq, "create"),
		scalar(FieldAuthorID, OpEq, "77"),
	}}
	if !evalOK(t, anyC, ev) {
		t.Error("any with one satisfied clause should hold")
	}

	not := Condition{Not: &Condition{Field: FieldOp, Op: OpEq, Value: "create"}}
	if !evalOK(t, not, ev) {
		t.Error("not of a false clause should hold")
	}

	// Nested: all[ op=update, not(author in [1,2]) ].
	nested := Condition{All: []Condition{
		scalar(FieldOp, OpEq, "update"),
		{Not: &Condition{Field: FieldAuthorID, Op: OpIn, Value: []any{"1", "2"}}},
	}}
	if !evalOK(t, nested, ev) {
		t.Error("nested all/not should hold")
	}
}

// Empty combinators take their conventional identity (Ш0 decision, documented).
func TestEmptyCombinatorIdentity(t *testing.T) {
	if !evalOK(t, Condition{All: []Condition{}}, EvalEvent{}) {
		t.Error("empty all should be true")
	}
	if evalOK(t, Condition{Any: []Condition{}}, EvalEvent{}) {
		t.Error("empty any should be false")
	}
}

func TestMatchChanges(t *testing.T) {
	ev := EvalEvent{
		Op: "update",
		Changes: []diff.Change{
			{Path: "status", Op: diff.OpChange, Old: "active", New: "cancelled"},
			{Path: "items.0.price", Op: diff.OpChange, Old: float64(10), New: float64(12)},
			{Path: "items.0.qty", Op: diff.OpChange, Old: float64(1), New: float64(2)},
		},
	}

	tests := []struct {
		name  string
		match Match
		want  bool
	}{
		{"exact path", Match{Path: "status"}, true},
		{"old+new equality", Match{Path: "status", Old: "active", HasOld: true, New: "cancelled", HasNew: true}, true},
		{"old mismatch", Match{Path: "status", Old: "draft", HasOld: true}, false},
		{"glob matches index", Match{Path: "items.*.price"}, true},
		{"glob excludes sibling", Match{Path: "items.*.cost"}, false},
		{"glob wrong leaf", Match{Path: "items.*.qty"}, true}, // qty does change here
		{"op filter change", Match{Path: "status", Op: diff.OpChange}, true},
		{"op filter add misses", Match{Path: "status", Op: diff.OpAdd}, false},
		{"absent path", Match{Path: "missing"}, false},
		{"new equality numeric", Match{Path: "items.*.price", New: float64(12), HasNew: true}, true},
		{"new equality numeric miss", Match{Path: "items.*.price", New: float64(99), HasNew: true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cond := Condition{Match: &tt.match}
			if got := evalOK(t, cond, ev); got != tt.want {
				t.Errorf("Eval = %v, want %v", got, tt.want)
			}
		})
	}
}

// The whole-value change carries the empty path; an empty glob matches only it.
func TestMatchWholeValuePath(t *testing.T) {
	ev := EvalEvent{Changes: []diff.Change{{Path: "", Op: diff.OpChange, Old: 1, New: 2}}}
	if !evalOK(t, Condition{Match: &Match{Path: ""}}, ev) {
		t.Error("empty glob should match the empty (whole-value) path")
	}
	if evalOK(t, Condition{Match: &Match{Path: "status"}}, ev) {
		t.Error("a named glob should not match the empty path")
	}
}

// nil Old/New are wildcards, not "equals JSON null": HasOld/HasNew gate the
// comparison, so an in-code Match{Path} matches regardless of the change values.
func TestMatchNilIsWildcardNotNull(t *testing.T) {
	ev := EvalEvent{Changes: []diff.Change{{Path: "x", Op: diff.OpAdd, New: "v"}}}
	if !evalOK(t, Condition{Match: &Match{Path: "x"}}, ev) {
		t.Error("unset Old/New should be a wildcard")
	}
	// Explicitly requiring New == nil should not match a non-nil New.
	if evalOK(t, Condition{Match: &Match{Path: "x", New: nil, HasNew: true}}, ev) {
		t.Error("HasNew with nil should require an actual null, not match 'v'")
	}
}

func TestRegexNonStringFieldNeverMatches(t *testing.T) {
	// regex only applies to string fields; against an empty (absent) field it is
	// a miss, never a panic.
	if evalOK(t, scalar(FieldSource, OpRegex, ".*"), EvalEvent{}) {
		t.Error("regex against an absent field should not match")
	}
}
