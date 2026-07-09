package diff

import (
	"testing"
)

func TestCanonicalizeStableUnderKeyOrder(t *testing.T) {
	// Two objects with the same content built in different insertion orders
	// must canonicalize identically; the recursion sorts nested keys too.
	x := map[string]any{"b": 2.0, "a": map[string]any{"z": 1.0, "y": 2.0}}
	y := map[string]any{"a": map[string]any{"y": 2.0, "z": 1.0}, "b": 2.0}

	cx, err := Canonicalize(x)
	if err != nil {
		t.Fatal(err)
	}
	cy, err := Canonicalize(y)
	if err != nil {
		t.Fatal(err)
	}
	if string(cx) != string(cy) {
		t.Errorf("canonical forms differ:\n %s\n %s", cx, cy)
	}
	if want := `{"a":{"y":2,"z":1},"b":2}`; string(cx) != want {
		t.Errorf("canonical = %s, want %s", cx, want)
	}
}

// TestCanonicalizeTolerantToOptionalFields encodes FR-8.5: adding an optional
// field (the flow block here) leaves the canonical form of values that omit it
// unchanged, so old hash-chains keep verifying.
func TestCanonicalizeTolerantToOptionalFields(t *testing.T) {
	withoutFlow := map[string]any{"entity": "order-1", "state": "paid"}
	reference, err := Canonicalize(withoutFlow)
	if err != nil {
		t.Fatal(err)
	}

	// A sibling event that carries the optional block hashes differently — the
	// guarantee is about absence, not about ignoring present fields.
	withFlow := map[string]any{"entity": "order-1", "state": "paid", "flow": map[string]any{"flow_id": "f1"}}
	got, err := Canonicalize(withFlow)
	if err != nil {
		t.Fatal(err)
	}
	if string(reference) == string(got) {
		t.Error("present optional field should change the canonical form")
	}

	// Re-canonicalizing the field-less value still matches its earlier form.
	again, err := Canonicalize(map[string]any{"state": "paid", "entity": "order-1"})
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(reference) {
		t.Errorf("canonical form not stable: %s vs %s", again, reference)
	}
}

func TestCanonicalizeNullDistinctFromAbsent(t *testing.T) {
	present, err := Canonicalize(map[string]any{"a": nil})
	if err != nil {
		t.Fatal(err)
	}
	absent, err := Canonicalize(map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if string(present) == string(absent) {
		t.Error("null-valued field must canonicalize differently from an absent one")
	}
}
