package worker

import (
	"strconv"
	"testing"
	"time"
)

func TestLagSeconds(t *testing.T) {
	now := time.Unix(100, 0)
	enq := now.Add(-2 * time.Second)

	secs, ok := lagSeconds(strconv.FormatInt(enq.UnixNano(), 10), now)
	if !ok || secs < 1.9 || secs > 2.1 {
		t.Fatalf("lagSeconds = %v ok=%v, want ~2", secs, ok)
	}
}

func TestLagSecondsMissingOrBad(t *testing.T) {
	if _, ok := lagSeconds("", time.Now()); ok {
		t.Fatal("empty stamp should report ok=false")
	}
	if _, ok := lagSeconds("not-a-number", time.Now()); ok {
		t.Fatal("unparseable stamp should report ok=false")
	}
}

// A future enqueue stamp (clock skew) clamps to 0 rather than a negative lag.
func TestLagSecondsClampsNegative(t *testing.T) {
	now := time.Unix(100, 0)
	future := now.Add(5 * time.Second)
	secs, ok := lagSeconds(strconv.FormatInt(future.UnixNano(), 10), now)
	if !ok || secs != 0 {
		t.Fatalf("lagSeconds = %v ok=%v, want 0", secs, ok)
	}
}
