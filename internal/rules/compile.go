package rules

import (
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/max-trifonov/letopis/internal/diff"
)

// Compile turns a condition into a tree of predicates, performing every expensive
// or fallible step once: regex compilation, numeric coercion, and structural validation.
// The returned Predicate's Eval is then a cheap pure walk. A malformed condition
// returns a *RuleError naming the offending field.
func Compile(c Condition) (Predicate, error) {
	return compile(c, "condition")
}

// compile recurses the condition tree. path tracks where we are for error messages
// ("condition.all[1].op"), so a client can locate a bad clause.
func compile(c Condition, path string) (Predicate, error) {
	switch kind, err := classify(c, path); {
	case err != nil:
		return nil, err
	case kind == kindAll:
		return compileList(c.All, path+".all", func(ps []Predicate) Predicate { return allPred(ps) })
	case kind == kindAny:
		return compileList(c.Any, path+".any", func(ps []Predicate) Predicate { return anyPred(ps) })
	case kind == kindNot:
		inner, err := compile(*c.Not, path+".not")
		if err != nil {
			return nil, err
		}
		return notPred{inner}, nil
	case kind == kindMatch:
		return compileMatch(*c.Match, path+".match")
	default: // kindScalar
		return compileScalar(c, path)
	}
}

type condKind int

const (
	kindScalar condKind = iota
	kindAll
	kindAny
	kindNot
	kindMatch
)

// classify determines which single kind a node is. Rejects an ambiguous node
// (more than one kind set) or an empty one. Nil vs empty slice matters: an empty
// All/Any is a valid combinator with a fixed truth value, so we test for non-nil,
// not for length.
func classify(c Condition, path string) (condKind, error) {
	var kinds []condKind
	if c.All != nil {
		kinds = append(kinds, kindAll)
	}
	if c.Any != nil {
		kinds = append(kinds, kindAny)
	}
	if c.Not != nil {
		kinds = append(kinds, kindNot)
	}
	if c.Match != nil {
		kinds = append(kinds, kindMatch)
	}
	if c.Field != "" || c.Op != "" {
		kinds = append(kinds, kindScalar)
	}
	switch len(kinds) {
	case 0:
		return 0, &RuleError{Field: path, Reason: "empty condition: set one of all/any/not/match or field+op"}
	case 1:
		return kinds[0], nil
	default:
		return 0, &RuleError{Field: path, Reason: "ambiguous condition: set exactly one of all/any/not/match or a scalar field"}
	}
}

func compileList(cs []Condition, path string, build func([]Predicate) Predicate) (Predicate, error) {
	ps := make([]Predicate, len(cs))
	for i, c := range cs {
		p, err := compile(c, path+"["+strconv.Itoa(i)+"]")
		if err != nil {
			return nil, err
		}
		ps[i] = p
	}
	return build(ps), nil
}

// compileScalar validates a scalar leaf and precompiles its comparator. regex
// is compiled here so a bad pattern is a save-time error, not a per-event panic.
func compileScalar(c Condition, path string) (Predicate, error) {
	if !validField(c.Field) {
		return nil, &RuleError{Field: path + ".field", Reason: "unknown field " + string(c.Field)}
	}
	switch c.Op {
	case OpEq, OpNe:
		return scalarPred{field: c.Field, op: c.Op, str: toStr(c.Value)}, nil
	case OpIn:
		list, ok := c.Value.([]any)
		if !ok {
			return nil, &RuleError{Field: path + ".in", Reason: "value must be a list"}
		}
		set := make(map[string]struct{}, len(list))
		for _, v := range list {
			set[toStr(v)] = struct{}{}
		}
		return scalarPred{field: c.Field, op: OpIn, set: set}, nil
	case OpGt, OpGte, OpLt, OpLte:
		n, ok := toFloat(c.Value)
		if !ok {
			return nil, &RuleError{Field: path + "." + string(c.Op), Reason: "value must be numeric"}
		}
		return scalarPred{field: c.Field, op: c.Op, num: n, numOK: true}, nil
	case OpRegex:
		s, ok := c.Value.(string)
		if !ok {
			return nil, &RuleError{Field: path + ".regex", Reason: "value must be a string"}
		}
		re, err := regexp.Compile(s)
		if err != nil {
			return nil, &RuleError{Field: path + ".regex", Reason: "invalid pattern: " + err.Error()}
		}
		return scalarPred{field: c.Field, op: OpRegex, re: re}, nil
	case OpExists:
		return scalarPred{field: c.Field, op: OpExists}, nil
	default:
		return nil, &RuleError{Field: path + ".op", Reason: "unknown operator " + string(c.Op)}
	}
}

// compileMatch validates the change-match leaf: the glob path is split into
// segments once (an empty segment is a malformed glob).
func compileMatch(m Match, path string) (Predicate, error) {
	segs, err := compileGlob(m.Path, path+".path")
	if err != nil {
		return nil, err
	}
	if m.Op != "" && m.Op != diff.OpAdd && m.Op != diff.OpChange && m.Op != diff.OpRemove {
		return nil, &RuleError{Field: path + ".op", Reason: "unknown change op " + string(m.Op)}
	}
	return matchPred{segs: segs, op: m.Op, old: m.Old, hasOld: m.HasOld, new: m.New, hasNew: m.HasNew}, nil
}

// compileGlob splits a glob path into segments and rejects an empty segment
// (a leading/trailing/doubled dot), which would otherwise match nothing in a way
// the author didn't intend. An empty path is allowed: it addresses the whole-value change.
func compileGlob(glob, path string) ([]string, error) {
	if glob == "" {
		return []string{}, nil
	}
	segs := strings.Split(glob, ".")
	if slices.Contains(segs, "") {
		return nil, &RuleError{Field: path, Reason: "empty path segment in glob " + strconv.Quote(glob)}
	}
	return segs, nil
}

// allPred holds when every child holds. An empty allPred is true.
type allPred []Predicate

func (ps allPred) Eval(e EvalEvent) bool {
	for _, p := range ps {
		if !p.Eval(e) {
			return false
		}
	}
	return true
}

// anyPred holds when at least one child holds. An empty anyPred is false.
type anyPred []Predicate

func (ps anyPred) Eval(e EvalEvent) bool {
	for _, p := range ps {
		if p.Eval(e) {
			return true
		}
	}
	return false
}

type notPred struct{ inner Predicate }

func (p notPred) Eval(e EvalEvent) bool { return !p.inner.Eval(e) }

// scalarPred is a compiled scalar-field comparison. Exactly one of str/set/re
// or num is meaningful, selected by op.
type scalarPred struct {
	field Field
	op    Op
	str   string              // eq/ne
	set   map[string]struct{} // in
	num   float64             // gt/gte/lt/lte
	numOK bool
	re    *regexp.Regexp // regex
}

func (p scalarPred) Eval(e EvalEvent) bool {
	val, present := scalarField(e, p.field)
	switch p.op {
	case OpExists:
		return present
	case OpEq:
		return present && val == p.str
	case OpNe:
		// A missing field differs from any concrete value.
		return val != p.str
	case OpIn:
		if !present {
			return false
		}
		_, ok := p.set[val]
		return ok
	case OpGt, OpGte, OpLt, OpLte:
		n, ok := toFloat(val)
		if !ok || !p.numOK {
			return false // incomparable types: clean miss, no panic
		}
		return compareNum(p.op, n, p.num)
	case OpRegex:
		return present && p.re.MatchString(val)
	default:
		return false
	}
}

// scalarField resolves a field name to its event value and reports presence.
// The empty string counts as absent — these attributes are always-present strings
// in practice, so "" only occurs when the source omitted one.
func scalarField(e EvalEvent, f Field) (string, bool) {
	var v string
	switch f {
	case FieldOp:
		v = e.Op
	case FieldEntityID:
		v = e.EntityID
	case FieldAuthorID:
		v = e.Author
	case FieldSource:
		v = e.Source
	}
	return v, v != ""
}

// matchPred holds when at least one change matches every set criterion.
// segs is the precompiled glob; an unset criterion is a wildcard.
type matchPred struct {
	segs   []string
	op     diff.Op
	old    any
	hasOld bool
	new    any
	hasNew bool
}

func (p matchPred) Eval(e EvalEvent) bool {
	for _, ch := range e.Changes {
		if p.op != "" && ch.Op != p.op {
			continue
		}
		if !globMatch(p.segs, ch.Path) {
			continue
		}
		if p.hasOld && !jsonEqual(ch.Old, p.old) {
			continue
		}
		if p.hasNew && !jsonEqual(ch.New, p.new) {
			continue
		}
		return true
	}
	return false
}

// globMatch reports whether a precompiled glob matches a concrete diff path.
// Matching is segment-wise on '.', with '*' matching exactly one segment, so glob
// and path must have the same segment count. The empty glob matches only the empty path.
func globMatch(segs []string, path string) bool {
	if path == "" {
		return len(segs) == 0
	}
	parts := strings.Split(path, ".")
	if len(parts) != len(segs) {
		return false
	}
	for i, s := range segs {
		if s != "*" && s != parts[i] {
			return false
		}
	}
	return true
}

// jsonEqual reports deep equality over the JSON value space.
// reflect.DeepEqual is exact for the types encoding/json yields.
func jsonEqual(a, b any) bool { return reflect.DeepEqual(a, b) }

func compareNum(op Op, a, b float64) bool {
	switch op {
	case OpGt:
		return a > b
	case OpGte:
		return a >= b
	case OpLt:
		return a < b
	case OpLte:
		return a <= b
	default:
		return false
	}
}

// toStr renders a scalar JSON value to its string form for eq/ne/in comparison.
// A non-scalar value stringifies to "" and won't match any concrete field.
func toStr(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case bool:
		return strconv.FormatBool(t)
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return ""
	}
}

// toFloat coerces a JSON value to float64 for ordering operators.
// Reports false for anything non-numeric so a comparison across incompatible
// types is a clean miss rather than a panic.
func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	default:
		return 0, false
	}
}
