// Package diff computes and applies structural diffs over JSON values.
//
// Pure domain logic — operates on the neutral JSON representation produced by
// encoding/json (map[string]any, []any, string, float64, bool, nil) with no
// dependency on BSON, HTTP or storage. Mapping to transport or storage shapes
// happens at those boundaries, not here.
//
// The diff format keeps the old value (unlike RFC 6902) because "changed X to Y"
// is half the audit value. Export to RFC 6902 is available via ToJSONPatch.
package diff

import (
	"errors"
	"reflect"
	"sort"
	"strconv"
)

// Op is the kind of a single change.
type Op string

const (
	OpAdd    Op = "add"    // field/element absent in the source; New holds the value
	OpChange Op = "change" // value differed; both Old and New are set
	OpRemove Op = "remove" // field/element dropped; Old is preserved
)

// Change is one structural edit between two JSON values.
//
// Path is dot-notation: nested object keys are joined by '.', array elements
// are addressed by numeric index ("items.2"), and elements of a keyed array by
// a bracket selector ("items[sku=ABC]"). The empty path denotes the whole value.
// Old is meaningful for OpChange/OpRemove; New for OpAdd/OpChange.
type Change struct {
	Path string
	Op   Op
	Old  any
	New  any
}

// Options tunes how Diff treats arrays.
//
// ArrayKeys maps a dot-notation array path to the name of the element field
// used as a stable identity. Matching elements by key (rather than position)
// keeps diffs meaningful when a source reorders a collection. Arrays without
// an entry are diffed by index.
type Options struct {
	ArrayKeys map[string]string
}

// Typed errors returned by Apply and ToJSONPatch.
var (
	ErrPathNotFound           = errors.New("diff: path not found")
	ErrTypeMismatch           = errors.New("diff: type mismatch at path")
	ErrUnknownOp              = errors.New("diff: unknown operation")
	ErrKeyedPathNotExportable = errors.New("diff: keyed-array path is not expressible as a JSON Pointer")
)

// Diff returns the changes that turn a into b. The result is deterministic:
// object keys are visited in sorted order, and Apply(a, Diff(a, b)) == b for
// any JSON values a and b.
func Diff(a, b any, opts Options) []Change {
	var out []Change
	diffValue("", a, b, opts, &out)
	return out
}

func diffValue(path string, a, b any, opts Options, out *[]Change) {
	if jsonEqual(a, b) {
		return
	}

	am, aIsMap := a.(map[string]any)
	bm, bIsMap := b.(map[string]any)
	if aIsMap && bIsMap {
		diffMap(path, am, bm, opts, out)
		return
	}

	aa, aIsArr := a.([]any)
	ba, bIsArr := b.([]any)
	if aIsArr && bIsArr {
		if kf, ok := opts.ArrayKeys[path]; ok && keyable(aa, kf) && keyable(ba, kf) {
			diffKeyedArray(path, kf, aa, ba, opts, out)
			return
		}
		diffIndexArray(path, aa, ba, opts, out)
		return
	}

	// Scalars, a type change, or null↔composite: a single change at this path.
	// We deliberately don't recurse across a type boundary — the old and new
	// shapes are unrelated, so element-level edits would be noise.
	*out = append(*out, Change{Path: path, Op: OpChange, Old: a, New: b})
}

func diffMap(path string, a, b map[string]any, opts Options, out *[]Change) {
	for _, k := range sortedUnion(a, b) {
		av, aok := a[k]
		bv, bok := b[k]
		cp := childPath(path, k)
		switch {
		case aok && bok:
			diffValue(cp, av, bv, opts, out)
		case bok:
			// A new key carries its whole subtree as one add; we don't decompose
			// it into per-leaf operations.
			*out = append(*out, Change{Path: cp, Op: OpAdd, New: bv})
		default:
			*out = append(*out, Change{Path: cp, Op: OpRemove, Old: av})
		}
	}
}

// diffIndexArray compares element-wise by position. Tail additions are emitted
// ascending and tail removals descending so that a straight sequential Apply
// never addresses a shifted index.
func diffIndexArray(path string, a, b []any, opts Options, out *[]Change) {
	n := min(len(a), len(b))
	for i := range n {
		diffValue(indexPath(path, i), a[i], b[i], opts, out)
	}
	for i := n; i < len(b); i++ {
		*out = append(*out, Change{Path: indexPath(path, i), Op: OpAdd, New: b[i]})
	}
	for i := len(a) - 1; i >= n; i-- {
		*out = append(*out, Change{Path: indexPath(path, i), Op: OpRemove, Old: a[i]})
	}
}

// diffKeyedArray matches elements by their key field, so reordering alone produces
// no operations and content edits are reported against a stable identity.
// A pure reorder is not reproduced by Apply — an accepted limitation of keyed mode,
// which exists precisely to ignore order.
func diffKeyedArray(path, keyField string, a, b []any, opts Options, out *[]Change) {
	bByKey := make(map[string]any, len(b))
	for _, el := range b {
		ks, _ := keyValString(el.(map[string]any)[keyField])
		bByKey[ks] = el
	}
	aKeys := make(map[string]struct{}, len(a))
	for _, el := range a {
		ks, _ := keyValString(el.(map[string]any)[keyField])
		aKeys[ks] = struct{}{}
		kp := keyedPath(path, keyField, ks)
		if bel, ok := bByKey[ks]; ok {
			diffValue(kp, el, bel, opts, out)
		} else {
			*out = append(*out, Change{Path: kp, Op: OpRemove, Old: el})
		}
	}
	for _, el := range b {
		ks, _ := keyValString(el.(map[string]any)[keyField])
		if _, ok := aKeys[ks]; !ok {
			*out = append(*out, Change{Path: keyedPath(path, keyField, ks), Op: OpAdd, New: el})
		}
	}
}

// jsonEqual reports deep equality over the JSON value space. reflect.DeepEqual
// is exact for the types encoding/json yields, which is the only input this
// package contracts to handle.
func jsonEqual(a, b any) bool { return reflect.DeepEqual(a, b) }

func sortedUnion(a, b map[string]any) []string {
	seen := make(map[string]struct{}, len(a)+len(b))
	keys := make([]string, 0, len(a)+len(b))
	for _, m := range []map[string]any{a, b} {
		for k := range m {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				keys = append(keys, k)
			}
		}
	}
	sort.Strings(keys)
	return keys
}

// keyable reports whether every element is an object carrying a scalar, unique
// value under keyField. When it doesn't hold, Diff falls back to index mode
// rather than guessing at identity.
func keyable(arr []any, keyField string) bool {
	seen := make(map[string]struct{}, len(arr))
	for _, el := range arr {
		m, ok := el.(map[string]any)
		if !ok {
			return false
		}
		ks, ok := keyValString(m[keyField])
		if !ok {
			return false
		}
		if _, dup := seen[ks]; dup {
			return false
		}
		seen[ks] = struct{}{}
	}
	return true
}

// keyValString renders a scalar key value to its path form. Non-scalar values
// report false. Distinct scalar types that share a textual form (string "1" vs
// number 1) collide; keyed arrays are expected to use a consistent scalar key.
func keyValString(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), true
	case int:
		return strconv.Itoa(t), true
	case int64:
		return strconv.FormatInt(t, 10), true
	default:
		return "", false
	}
}

func childPath(path, key string) string {
	if path == "" {
		return key
	}
	return path + "." + key
}

func indexPath(path string, i int) string {
	return childPath(path, strconv.Itoa(i))
}

func keyedPath(path, keyField, keyVal string) string {
	return path + "[" + keyField + "=" + keyVal + "]"
}
