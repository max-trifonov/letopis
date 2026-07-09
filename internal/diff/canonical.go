package diff

import (
	"bytes"
	"encoding/json"
)

// Canonicalize returns a deterministic byte encoding of a JSON value, suitable
// as the input to a hash. encoding/json emits object keys in sorted order at every
// level, so two JSON-equal values produce identical bytes regardless of how they were built.
//
// Only present fields contribute: an absent optional field adds nothing, so introducing
// a new optional field later doesn't change the canonical form of values that omit it,
// and existing hash-chains keep verifying. A field present as null is distinct from absent.
func Canonicalize(v any) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	// Keep '<', '>', '&' literal: the output feeds a hash, not an HTML page,
	// and escaping would only obscure it.
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
