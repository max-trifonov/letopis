package diff

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
)

func mustUnmarshal(t *testing.T, s string) any {
	t.Helper()
	var v any
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("unmarshal %q: %v", s, err)
	}
	return v
}

// jsonValue wraps a randomly generated JSON value so testing/quick can drive
// the roundtrip property without an external generator library — the package
// deliberately stays stdlib-only (DoD).
type jsonValue struct{ v any }

func (jsonValue) Generate(r *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(jsonValue{randValue(r, 4)})
}

// randValue builds a JSON tree up to the given depth. Keys are drawn from a
// tiny alphabet so two independently generated values overlap often, which is
// what exercises change/add/remove together rather than wholesale replacement.
func randValue(r *rand.Rand, depth int) any {
	if depth <= 0 {
		return randScalar(r)
	}
	switch r.Intn(7) {
	case 0, 1, 2:
		return randScalar(r)
	case 3, 4:
		n := r.Intn(4)
		m := make(map[string]any, n)
		for range n {
			m[randKey(r)] = randValue(r, depth-1)
		}
		return m
	default:
		n := r.Intn(4)
		s := make([]any, n)
		for i := range n {
			s[i] = randValue(r, depth-1)
		}
		return s
	}
}

func randScalar(r *rand.Rand) any {
	switch r.Intn(5) {
	case 0:
		return nil
	case 1:
		return r.Intn(2) == 0
	case 2:
		// Small integral float64 values keep equality exact and mirror what
		// encoding/json produces for whole numbers.
		return float64(r.Intn(7) - 3)
	case 3:
		return randKey(r)
	default:
		return [...]string{"", "x", "letopis"}[r.Intn(3)]
	}
}

func randKey(r *rand.Rand) string {
	return [...]string{"a", "b", "c"}[r.Intn(3)]
}
