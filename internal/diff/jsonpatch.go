package diff

import (
	"encoding/json"
	"fmt"
	"strings"
)

// JSONPatchOp is one RFC 6902 operation. OpChange maps to "replace" and the
// old value is dropped; a remove op carries no value.
type JSONPatchOp struct {
	Op    string
	Path  string // RFC 6901 JSON Pointer
	Value any
}

// MarshalJSON emits the RFC 6902 object. The value member is included for
// add/replace (even when it is JSON null) and omitted for remove.
func (o JSONPatchOp) MarshalJSON() ([]byte, error) {
	m := map[string]any{"op": o.Op, "path": o.Path}
	if o.Op != "remove" {
		m["value"] = o.Value
	}
	return json.Marshal(m)
}

// ToJSONPatch converts a native diff to an RFC 6902 patch for the read-API's
// format=json-patch option. Keyed-array paths have no JSON Pointer form —
// a pointer addresses elements by index, not by key — so a diff containing one
// yields ErrKeyedPathNotExportable; use the native format for such collections.
func ToJSONPatch(changes []Change) ([]JSONPatchOp, error) {
	out := make([]JSONPatchOp, 0, len(changes))
	for _, ch := range changes {
		ptr, err := pointerFromPath(ch.Path)
		if err != nil {
			return nil, err
		}
		switch ch.Op {
		case OpAdd:
			out = append(out, JSONPatchOp{Op: "add", Path: ptr, Value: ch.New})
		case OpChange:
			out = append(out, JSONPatchOp{Op: "replace", Path: ptr, Value: ch.New})
		case OpRemove:
			out = append(out, JSONPatchOp{Op: "remove", Path: ptr})
		default:
			return nil, fmt.Errorf("%w: %q", ErrUnknownOp, ch.Op)
		}
	}
	return out, nil
}

func pointerFromPath(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	var b strings.Builder
	for seg := range strings.SplitSeq(path, ".") {
		if strings.ContainsRune(seg, '[') {
			return "", ErrKeyedPathNotExportable
		}
		b.WriteByte('/')
		b.WriteString(escapePointer(seg))
	}
	return b.String(), nil
}

// escapePointer applies RFC 6901 token escaping: '~' → "~0", '/' → "~1".
func escapePointer(seg string) string {
	seg = strings.ReplaceAll(seg, "~", "~0")
	return strings.ReplaceAll(seg, "/", "~1")
}
