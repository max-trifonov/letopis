package diff

import (
	"encoding/json"
	"testing"
)

// A document shaped like a typical CRM record: a handful of scalar fields, a
// nested object and a small line-item array — the common ingest payload.
const benchDoc = `{
  "id": "order-4821",
  "status": "processing",
  "total": 1299.5,
  "customer": {"id": "cust-77", "name": "Acme Corp", "tier": "gold"},
  "items": [
    {"sku": "A-1", "qty": 2, "price": 199.0},
    {"sku": "B-7", "qty": 1, "price": 901.5},
    {"sku": "C-3", "qty": 4, "price": 49.75}
  ],
  "tags": ["priority", "export"]
}`

func benchPair(b *testing.B) (before, after any) {
	b.Helper()
	if err := json.Unmarshal([]byte(benchDoc), &before); err != nil {
		b.Fatal(err)
	}
	if err := json.Unmarshal([]byte(benchDoc), &after); err != nil {
		b.Fatal(err)
	}
	// A realistic edit: one scalar, one nested scalar, one line-item price.
	m := after.(map[string]any)
	m["status"] = "shipped"
	m["customer"].(map[string]any)["tier"] = "platinum"
	m["items"].([]any)[1].(map[string]any)["price"] = 950.0
	return before, after
}

func BenchmarkDiff(b *testing.B) {
	before, after := benchPair(b)
	b.ReportAllocs()
	for b.Loop() {
		_ = Diff(before, after, Options{})
	}
}

func BenchmarkApply(b *testing.B) {
	before, after := benchPair(b)
	changes := Diff(before, after, Options{})
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Apply(before, changes); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCanonicalize(b *testing.B) {
	before, _ := benchPair(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Canonicalize(before); err != nil {
			b.Fatal(err)
		}
	}
}
