package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/diff"
	"github.com/max-trifonov/letopis/internal/domain"
)

func TestSignIsDeterministicAndVerifiable(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	secret := "s3cr3t"

	got := Sign(secret, body)

	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got != want {
		t.Fatalf("Sign = %q, want %q", got, want)
	}
	// A receiver recomputes the same signature; signing twice is stable.
	if Sign(secret, body) != got {
		t.Fatal("Sign is not deterministic")
	}
	// A different secret yields a different signature.
	if Sign("other", body) == got {
		t.Fatal("Sign collides across secrets")
	}
}

func TestNextDelayScheduleWithoutJitter(t *testing.T) {
	cfg := BackoffConfig{Base: 100 * time.Millisecond, Max: 2 * time.Second}

	// Without jitter the schedule is the capped exponential: 100, 200, 400, 800,
	// 1600, then clamped at 2000ms.
	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		2 * time.Second, // 3200ms capped
		2 * time.Second, // stays capped
	}
	for attempt, w := range want {
		if got := nextDelay(attempt, cfg, nil); got != w {
			t.Errorf("nextDelay(%d) = %v, want %v", attempt, got, w)
		}
	}
}

func TestNextDelayIsMonotonicUpToCap(t *testing.T) {
	cfg := BackoffConfig{Base: 50 * time.Millisecond, Max: time.Second}
	var prev time.Duration
	for attempt := range 10 {
		d := nextDelay(attempt, cfg, nil)
		if d < prev {
			t.Fatalf("nextDelay decreased at attempt %d: %v < %v", attempt, d, prev)
		}
		if d > cfg.Max {
			t.Fatalf("nextDelay(%d)=%v exceeds cap %v", attempt, d, cfg.Max)
		}
		prev = d
	}
}

func TestNextDelayFullJitterStaysWithinBound(t *testing.T) {
	cfg := BackoffConfig{Base: 100 * time.Millisecond, Max: time.Second}
	for attempt := range 8 {
		bound := nextDelay(attempt, cfg, nil) // unjittered cap for this attempt
		for range 50 {
			d := nextDelay(attempt, cfg, fullJitter)
			if d < 0 || d > bound {
				t.Fatalf("jittered nextDelay(%d)=%v outside [0,%v]", attempt, d, bound)
			}
		}
	}
}

func TestBuildBodyShape(t *testing.T) {
	ev := domain.Event{
		EntityID:   "d-1",
		Version:    7,
		Op:         domain.OpUpdate,
		Author:     "alice",
		Source:     "crm",
		TSReceived: time.Unix(1000, 0).UTC(),
		Changes:    []diff.Change{{Path: "status", Op: diff.OpChange, Old: "open", New: "closed"}},
	}

	raw, err := BuildBody("crm.deals", ev, "rule_1", "close-watch")
	if err != nil {
		t.Fatalf("BuildBody: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	event, _ := got["event"].(map[string]any)
	rule, _ := got["rule"].(map[string]any)
	if event["entity_id"] != "d-1" || event["collection"] != "crm.deals" || event["version"].(float64) != 7 {
		t.Fatalf("event block wrong: %+v", event)
	}
	if rule["id"] != "rule_1" || rule["name"] != "close-watch" {
		t.Fatalf("rule block wrong: %+v", rule)
	}
	changes, ok := event["changes"].([]any)
	if !ok || len(changes) != 1 {
		t.Fatalf("changes wrong: %+v", event["changes"])
	}
}

func TestTaskRoundTrips(t *testing.T) {
	in := Task{
		Tenant:      TenantRef{ID: "acme", DBName: "hm_t_acme"},
		DeliveryID:  "dlv_1",
		RuleID:      "rule_1",
		Collection:  "crm.deals",
		URL:         "https://x.test/hook",
		SecretRef:   "k1",
		TimeoutMS:   1000,
		MaxAttempts: 3,
		Attempt:     2,
		Body:        []byte(`{"a":1}`),
	}
	b, err := EncodeTask(in)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	out, err := DecodeTask(b)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.DeliveryID != in.DeliveryID || out.Attempt != in.Attempt || string(out.Body) != string(in.Body) {
		t.Fatalf("round-trip mismatch: %+v", out)
	}
}
