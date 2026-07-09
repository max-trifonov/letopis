package diff

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestToJSONPatchGolden(t *testing.T) {
	a := mustUnmarshal(t, `{"a/b":1,"items":[1,2,3],"drop":true}`)
	b := mustUnmarshal(t, `{"a/b":2,"items":[1,2],"add":"x"}`)

	patch, err := ToJSONPatch(Diff(a, b, Options{}))
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}

	// "change" maps to RFC 6902 "replace" (the old value is dropped), the slash
	// in the key "a/b" is escaped to "~1", and the tail removal keeps its
	// numeric pointer.
	want := `[{"op":"replace","path":"/a~1b","value":2},{"op":"add","path":"/add","value":"x"},{"op":"remove","path":"/drop"},{"op":"remove","path":"/items/2"}]`
	if string(got) != want {
		t.Errorf("json patch mismatch\n got: %s\nwant: %s", got, want)
	}
}

func TestToJSONPatchAddNullPreservesValue(t *testing.T) {
	patch, err := ToJSONPatch([]Change{{Path: "a", Op: OpAdd, New: nil}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := json.Marshal(patch)
	if err != nil {
		t.Fatal(err)
	}
	if want := `[{"op":"add","path":"/a","value":null}]`; string(got) != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestToJSONPatchRejectsKeyedPath(t *testing.T) {
	_, err := ToJSONPatch([]Change{{Path: "items[sku=A].qty", Op: OpChange, New: 1.0}})
	if !errors.Is(err, ErrKeyedPathNotExportable) {
		t.Errorf("got %v, want ErrKeyedPathNotExportable", err)
	}
}
