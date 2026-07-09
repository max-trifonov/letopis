package diff

import (
	"reflect"
	"testing"
)

// Edge-case semantics fixed against ADR-004 and architecture §4, exercised by
// the tables below:
//   - a field present as null differs from an absent field (remove vs nothing);
//   - a type change (object↔array, scalar↔composite, null↔composite) is one
//     change at that path, not a recursive descent;
//   - empty arrays/objects are handled as ordinary containers;
//   - object keys are visited in sorted order for deterministic output.

// jdec turns a JSON literal into the neutral representation the engine works
// on, matching what encoding/json hands the rest of the system.
func jdec(t *testing.T, s string) any {
	t.Helper()
	return mustUnmarshal(t, s)
}

func TestDiff(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		opts Options
		want []Change
	}{
		{
			name: "identical yields no changes",
			a:    `{"a":1,"b":[1,2]}`,
			b:    `{"a":1,"b":[1,2]}`,
			want: nil,
		},
		{
			name: "nested scalar change keeps old and new",
			a:    `{"user":{"name":"ann","age":30}}`,
			b:    `{"user":{"name":"ann","age":31}}`,
			want: []Change{{Path: "user.age", Op: OpChange, Old: 30.0, New: 31.0}},
		},
		{
			name: "add and remove keys, sorted order",
			a:    `{"keep":1,"drop":2}`,
			b:    `{"keep":1,"add":3}`,
			want: []Change{
				{Path: "add", Op: OpAdd, New: 3.0},
				{Path: "drop", Op: OpRemove, Old: 2.0},
			},
		},
		{
			name: "null present differs from absent: change",
			a:    `{"a":null}`,
			b:    `{"a":1}`,
			want: []Change{{Path: "a", Op: OpChange, Old: nil, New: 1.0}},
		},
		{
			name: "remove a null-valued field",
			a:    `{"a":null}`,
			b:    `{}`,
			want: []Change{{Path: "a", Op: OpRemove, Old: nil}},
		},
		{
			name: "type change object to array is one change",
			a:    `{"x":{"k":1}}`,
			b:    `{"x":[1,2]}`,
			want: []Change{{Path: "x", Op: OpChange, Old: map[string]any{"k": 1.0}, New: []any{1.0, 2.0}}},
		},
		{
			name: "array element change by index",
			a:    `{"items":[1,2,3]}`,
			b:    `{"items":[1,9,3]}`,
			want: []Change{{Path: "items.1", Op: OpChange, Old: 2.0, New: 9.0}},
		},
		{
			name: "array grows: additions ascending at the tail",
			a:    `{"items":[1]}`,
			b:    `{"items":[1,2,3]}`,
			want: []Change{
				{Path: "items.1", Op: OpAdd, New: 2.0},
				{Path: "items.2", Op: OpAdd, New: 3.0},
			},
		},
		{
			name: "array shrinks: removals descending at the tail",
			a:    `{"items":[1,2,3,4]}`,
			b:    `{"items":[1,2]}`,
			want: []Change{
				{Path: "items.3", Op: OpRemove, Old: 4.0},
				{Path: "items.2", Op: OpRemove, Old: 3.0},
			},
		},
		{
			name: "root scalar change has empty path",
			a:    `1`,
			b:    `2`,
			want: []Change{{Path: "", Op: OpChange, Old: 1.0, New: 2.0}},
		},
		{
			name: "keyed array: change addressed by key, order ignored",
			a:    `{"items":[{"sku":"A","qty":1},{"sku":"B","qty":2}]}`,
			b:    `{"items":[{"sku":"B","qty":2},{"sku":"A","qty":5}]}`,
			opts: Options{ArrayKeys: map[string]string{"items": "sku"}},
			want: []Change{{Path: "items[sku=A].qty", Op: OpChange, Old: 1.0, New: 5.0}},
		},
		{
			name: "keyed array: add and remove by key",
			a:    `{"items":[{"sku":"A"},{"sku":"B"}]}`,
			b:    `{"items":[{"sku":"A"},{"sku":"C"}]}`,
			opts: Options{ArrayKeys: map[string]string{"items": "sku"}},
			want: []Change{
				{Path: "items[sku=B]", Op: OpRemove, Old: map[string]any{"sku": "B"}},
				{Path: "items[sku=C]", Op: OpAdd, New: map[string]any{"sku": "C"}},
			},
		},
		{
			name: "keyed config falls back to index when an element lacks the key",
			a:    `{"items":[{"sku":"A"},{"noksu":1}]}`,
			b:    `{"items":[{"sku":"A"},{"noksu":2}]}`,
			opts: Options{ArrayKeys: map[string]string{"items": "sku"}},
			want: []Change{{Path: "items.1.noksu", Op: OpChange, Old: 1.0, New: 2.0}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Diff(jdec(t, tc.a), jdec(t, tc.b), tc.opts)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Diff mismatch\n got: %#v\nwant: %#v", got, tc.want)
			}
		})
	}
}
