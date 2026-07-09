package server

import (
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

func TestCursorRoundTripVersion(t *testing.T) {
	want := domain.Position{Version: 42}
	got, err := decodeCursor(encodeCursor(want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Version != want.Version || !got.TS.IsZero() {
		t.Fatalf("round-trip lost data: %+v", got)
	}
}

func TestCursorRoundTripWithTime(t *testing.T) {
	want := domain.Position{Version: 7, TS: time.Date(2026, 6, 13, 10, 0, 0, 0, time.UTC)}
	got, err := decodeCursor(encodeCursor(want))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Version != want.Version || !got.TS.Equal(want.TS) {
		t.Fatalf("round-trip lost data: %+v", got)
	}
}

// Two events sharing a timestamp must produce distinct cursors: the version
// tie-breaker is what keeps time-ordered pagination free of dupes/gaps.
func TestCursorDistinctOnEqualTimestamps(t *testing.T) {
	ts := time.Unix(1000, 0).UTC()
	a := encodeCursor(domain.Position{Version: 1, TS: ts})
	b := encodeCursor(domain.Position{Version: 2, TS: ts})
	if a == b {
		t.Fatal("equal timestamps produced identical cursors")
	}
}

func TestCursorRejectsGarbage(t *testing.T) {
	if _, err := decodeCursor("@@@not-base64@@@"); err == nil {
		t.Fatal("expected error on malformed cursor")
	}
	if _, err := decodeCursor("bm90anNvbg"); err == nil { // base64 of "notjson"
		t.Fatal("expected error on non-JSON cursor payload")
	}
}
