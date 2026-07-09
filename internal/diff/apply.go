package diff

import (
	"fmt"
	"strings"
)

// Apply returns the value obtained by applying changes to state in order.
// The input is never mutated: Apply works on a deep copy. Everything it needs
// is encoded in each change's path, so (unlike Diff) it takes no Options.
//
// Changes are expected in the order Diff emits them; an out-of-order sequence
// touching a shifted array index yields ErrPathNotFound rather than silent corruption.
func Apply(state any, changes []Change) (any, error) {
	result := deepCopy(state)
	for _, ch := range changes {
		segs, err := parsePath(ch.Path)
		if err != nil {
			return nil, err
		}
		if len(segs) == 0 {
			// The empty path targets the whole value; only a replacement makes sense
			// there (Diff never emits a root add/remove).
			if ch.Op != OpChange {
				return nil, fmt.Errorf("%w: root path supports change only, got %q", ErrUnknownOp, ch.Op)
			}
			result = ch.New
			continue
		}
		if result, err = applyStep(result, segs, ch); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// applyStep walks one path segment and either descends or performs the change at
// the leaf. Array operations return a possibly-new slice header, so each caller
// writes the result back into its parent.
func applyStep(node any, segs []pathSeg, ch Change) (any, error) {
	s := segs[0]
	last := len(segs) == 1

	switch s.kind {
	case segKey:
		m, ok := node.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("%w: %q expects an object", ErrTypeMismatch, ch.Path)
		}
		if last {
			switch ch.Op {
			case OpAdd, OpChange:
				m[s.name] = ch.New
			case OpRemove:
				if _, present := m[s.name]; !present {
					return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
				}
				delete(m, s.name)
			default:
				return nil, fmt.Errorf("%w: %q", ErrUnknownOp, ch.Op)
			}
			return node, nil
		}
		child, present := m[s.name]
		if !present {
			return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
		}
		nv, err := applyStep(child, segs[1:], ch)
		if err != nil {
			return nil, err
		}
		m[s.name] = nv
		return node, nil

	case segIndex:
		arr, ok := node.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: %q expects an array", ErrTypeMismatch, ch.Path)
		}
		i := s.index
		if last {
			switch ch.Op {
			case OpAdd:
				if i < 0 || i > len(arr) {
					return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
				}
				arr = append(arr, nil)
				copy(arr[i+1:], arr[i:])
				arr[i] = ch.New
				return arr, nil
			case OpChange:
				if i < 0 || i >= len(arr) {
					return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
				}
				arr[i] = ch.New
				return arr, nil
			case OpRemove:
				if i < 0 || i >= len(arr) {
					return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
				}
				return append(arr[:i], arr[i+1:]...), nil
			default:
				return nil, fmt.Errorf("%w: %q", ErrUnknownOp, ch.Op)
			}
		}
		if i < 0 || i >= len(arr) {
			return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
		}
		nv, err := applyStep(arr[i], segs[1:], ch)
		if err != nil {
			return nil, err
		}
		arr[i] = nv
		return arr, nil

	case segKeyed:
		arr, ok := node.([]any)
		if !ok {
			return nil, fmt.Errorf("%w: %q expects an array", ErrTypeMismatch, ch.Path)
		}
		idx := indexOfKey(arr, s.keyField, s.keyVal)
		if last {
			switch ch.Op {
			case OpAdd:
				// Added elements carry their own key inside New; appending preserves
				// the relative order Diff reported them in.
				return append(arr, ch.New), nil
			case OpChange:
				if idx < 0 {
					return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
				}
				arr[idx] = ch.New
				return arr, nil
			case OpRemove:
				if idx < 0 {
					return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
				}
				return append(arr[:idx], arr[idx+1:]...), nil
			default:
				return nil, fmt.Errorf("%w: %q", ErrUnknownOp, ch.Op)
			}
		}
		if idx < 0 {
			return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
		}
		nv, err := applyStep(arr[idx], segs[1:], ch)
		if err != nil {
			return nil, err
		}
		arr[idx] = nv
		return arr, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrPathNotFound, ch.Path)
}

func indexOfKey(arr []any, keyField, keyVal string) int {
	for j, el := range arr {
		if m, ok := el.(map[string]any); ok {
			if ks, ok := keyValString(m[keyField]); ok && ks == keyVal {
				return j
			}
		}
	}
	return -1
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, vv := range t {
			m[k] = deepCopy(vv)
		}
		return m
	case []any:
		s := make([]any, len(t))
		for i, vv := range t {
			s[i] = deepCopy(vv)
		}
		return s
	default:
		// Scalars are immutable JSON leaves; sharing them is safe.
		return v
	}
}

type segKind int

const (
	segKey segKind = iota
	segIndex
	segKeyed
)

type pathSeg struct {
	kind     segKind
	name     string // segKey
	index    int    // segIndex
	keyField string // segKeyed
	keyVal   string // segKeyed
}

// parsePath splits a dot-notation path into navigation segments. A bracket selector
// ("items[sku=ABC]") expands into the array's key segment followed by a keyed segment.
// A purely numeric segment is an array index; everything else is an object key.
// Numeric-looking object keys are not supported — an accepted ambiguity of dot-notation.
func parsePath(path string) ([]pathSeg, error) {
	if path == "" {
		return nil, nil
	}
	var segs []pathSeg
	for raw := range strings.SplitSeq(path, ".") {
		name, sel, keyed := strings.Cut(raw, "[")
		if keyed {
			field, val, ok := strings.Cut(strings.TrimSuffix(sel, "]"), "=")
			if !ok {
				return nil, fmt.Errorf("%w: malformed selector in %q", ErrPathNotFound, path)
			}
			if name != "" {
				segs = append(segs, keyOrIndex(name))
			}
			segs = append(segs, pathSeg{kind: segKeyed, keyField: field, keyVal: val})
			continue
		}
		segs = append(segs, keyOrIndex(raw))
	}
	return segs, nil
}

func keyOrIndex(seg string) pathSeg {
	if i, ok := atoiIndex(seg); ok {
		return pathSeg{kind: segIndex, index: i}
	}
	return pathSeg{kind: segKey, name: seg}
}

func atoiIndex(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			return 0, false
		}
		n = n*10 + int(r-'0')
	}
	return n, true
}
