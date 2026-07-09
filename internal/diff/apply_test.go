package diff

import (
	"errors"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
)

// TestDiffApplyRoundtrip is the core property of the engine (ADR-004,
// development-workflow §3): Apply(a, Diff(a, b)) == b for arbitrary JSON
// values. The generator covers nested objects, arrays, type changes, null and
// scalars; keyed arrays are excluded here because keyed mode intentionally
// ignores pure reordering (covered by table tests instead).
func TestDiffApplyRoundtrip(t *testing.T) {
	prop := func(a, b jsonValue) bool {
		changes := Diff(a.v, b.v, Options{})
		got, err := Apply(a.v, changes)
		if err != nil {
			t.Logf("apply error for a=%#v b=%#v: %v", a.v, b.v, err)
			return false
		}
		if !reflect.DeepEqual(got, b.v) {
			t.Logf("roundtrip mismatch\n  a=%#v\n  b=%#v\n got=%#v\n diff=%#v", a.v, b.v, got, changes)
			return false
		}
		return true
	}
	cfg := &quick.Config{MaxCount: 3000, Rand: rand.New(rand.NewSource(1))}
	if err := quick.Check(prop, cfg); err != nil {
		t.Fatal(err)
	}
}

// TestApplyDoesNotMutateInput guards the deep-copy contract: callers reuse the
// source value after Apply (e.g. ingest derives a diff and keeps the snapshot).
func TestApplyDoesNotMutateInput(t *testing.T) {
	a := mustUnmarshal(t, `{"items":[1,2],"meta":{"v":1}}`)
	before := mustUnmarshal(t, `{"items":[1,2],"meta":{"v":1}}`)
	b := mustUnmarshal(t, `{"items":[1,9,3],"meta":{"v":2}}`)

	if _, err := Apply(a, Diff(a, b, Options{})); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, before) {
		t.Errorf("Apply mutated its input: got %#v, want %#v", a, before)
	}
}

func TestApplyRoundtripKeyedArrays(t *testing.T) {
	opts := Options{ArrayKeys: map[string]string{"items": "sku"}}
	// b keeps the order of surviving keys and appends new ones, which keyed
	// mode reproduces exactly.
	a := mustUnmarshal(t, `{"items":[{"sku":"A","qty":1},{"sku":"B","qty":2}]}`)
	b := mustUnmarshal(t, `{"items":[{"sku":"A","qty":5},{"sku":"C","qty":9}]}`)

	got, err := Apply(a, Diff(a, b, opts))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, b) {
		t.Errorf("keyed roundtrip\n got: %#v\nwant: %#v", got, b)
	}
}

func TestApplyErrors(t *testing.T) {
	tests := []struct {
		name    string
		state   string
		change  Change
		wantErr error
	}{
		{
			name:    "remove missing key",
			state:   `{"a":1}`,
			change:  Change{Path: "b", Op: OpRemove},
			wantErr: ErrPathNotFound,
		},
		{
			name:    "descend into object expecting array",
			state:   `{"a":{"b":1}}`,
			change:  Change{Path: "a.0", Op: OpChange, New: 1.0},
			wantErr: ErrTypeMismatch,
		},
		{
			name:    "index out of range",
			state:   `{"a":[1]}`,
			change:  Change{Path: "a.5", Op: OpChange, New: 1.0},
			wantErr: ErrPathNotFound,
		},
		{
			name:    "unknown op",
			state:   `{"a":1}`,
			change:  Change{Path: "a", Op: Op("bogus"), New: 2.0},
			wantErr: ErrUnknownOp,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Apply(mustUnmarshal(t, tc.state), []Change{tc.change})
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("got err %v, want %v", err, tc.wantErr)
			}
		})
	}
}
