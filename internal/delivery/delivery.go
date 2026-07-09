// Package delivery is the webhook dispatcher: takes a fired webhook action off a
// durable queue, signs the body with HMAC-SHA256, POSTs it to the target, and
// retries with exponential backoff before parking in the DLQ. It runs in the
// worker role so a slow receiver never touches the ingest path.
//
// The backoff schedule and HMAC signature are pure functions, tested without a
// network. The HTTP client's Transport is the injection point for the SSRF guard.
package delivery

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
)

// Webhook signature headers. The signature covers the exact bytes sent, so a
// receiver recomputes HMAC-SHA256 over the raw request body and compares.
const (
	HeaderSignature = "X-HM-Signature"
	HeaderDelivery  = "X-HM-Delivery"
	HeaderRule      = "X-HM-Rule"
	signaturePrefix = "sha256="
)

// TenantRef carries just enough of the resolved principal to re-select the
// tenant's database on the consuming side. Duplicated here rather than imported
// from service to avoid an import cycle.
type TenantRef struct {
	ID     string `json:"id"`
	DBURI  string `json:"db_uri,omitempty"`
	DBName string `json:"db_name,omitempty"`
}

// Task is the unit a webhook delivery is serialized into on the delivery queue.
// DeliveryID is stable across retries (rides in X-HM-Delivery for receiver-side
// dedup); Attempt tracks how many tries have already happened. Body is the exact
// signed payload sent verbatim on every retry.
type Task struct {
	Tenant     TenantRef `json:"tenant"`
	DeliveryID string    `json:"delivery_id"`
	RuleID     string    `json:"rule_id"`
	RuleName   string    `json:"rule_name,omitempty"`
	Collection string    `json:"collection"`
	URL        string    `json:"url"`
	SecretRef  string    `json:"secret_ref,omitempty"`
	TimeoutMS  int       `json:"timeout_ms,omitempty"`
	// MaxAttempts caps tries before the DLQ; 0 lets the dispatcher apply its
	// configured default. Carried per task so a rule can override the instance
	// default.
	MaxAttempts int    `json:"max_attempts,omitempty"`
	Attempt     int    `json:"attempt"`
	Body        []byte `json:"body"`
}

func EncodeTask(t Task) ([]byte, error) { return json.Marshal(t) }

func DecodeTask(b []byte) (Task, error) {
	var t Task
	if err := json.Unmarshal(b, &t); err != nil {
		return Task{}, fmt.Errorf("delivery: decode task: %w", err)
	}
	return t, nil
}

// webhookBody is the wire shape POSTed to a receiver: the triggering event in
// history format plus the rule that fired. Built once and stored as Task.Body,
// so every retry signs and sends identical bytes.
type webhookBody struct {
	Event eventDTO `json:"event"`
	Rule  ruleDTO  `json:"rule"`
}

type ruleDTO struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
}

// eventDTO mirrors the read API's history event shape so a webhook body looks
// like a history entry. Duplicated from the transport layer rather than imported
// to avoid a server→delivery dependency.
type eventDTO struct {
	EntityID   string         `json:"entity_id"`
	Collection string         `json:"collection"`
	Version    int64          `json:"version"`
	Op         string         `json:"op"`
	Author     string         `json:"author_id,omitempty"`
	Source     string         `json:"source,omitempty"`
	TSReceived time.Time      `json:"ts_received"`
	Changes    []changeDTO    `json:"changes"`
	Meta       map[string]any `json:"meta,omitempty"`
}

type changeDTO struct {
	Path string `json:"path"`
	Op   string `json:"op"`
	Old  any    `json:"old,omitempty"`
	New  any    `json:"new,omitempty"`
}

// BuildBody renders the signed webhook payload from a triggering event and rule.
// The bytes are stable (single json.Marshal) and sent unchanged on every retry.
func BuildBody(collection string, ev domain.Event, ruleID, ruleName string) ([]byte, error) {
	changes := make([]changeDTO, len(ev.Changes))
	for i, c := range ev.Changes {
		changes[i] = changeDTO{Path: c.Path, Op: string(c.Op), Old: c.Old, New: c.New}
	}
	body := webhookBody{
		Event: eventDTO{
			EntityID:   ev.EntityID,
			Collection: collection,
			Version:    ev.Version,
			Op:         string(ev.Op),
			Author:     ev.Author,
			Source:     ev.Source,
			TSReceived: ev.TSReceived,
			Changes:    changes,
			Meta:       ev.Meta,
		},
		Rule: ruleDTO{ID: ruleID, Name: ruleName},
	}
	return json.Marshal(body)
}

// Sign computes the X-HM-Signature value: "sha256=" + hex(HMAC-SHA256(secret,
// body)) over the exact bytes sent. Pure and deterministic — a receiver or test
// can recompute and compare.
func Sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return signaturePrefix + hex.EncodeToString(mac.Sum(nil))
}

// BackoffConfig is the exponential retry schedule: the nth retry waits
// min(Base*2^n, Max), with full jitter applied by nextDelay.
type BackoffConfig struct {
	Base time.Duration
	Max  time.Duration
}

// nextDelay returns the wait before retry number `attempt` (0 = after the first
// failure). Schedule is min(Base*2^attempt, Max) with full jitter — uniformly
// random in [0, schedule] to spread thundering-herd restarts. A nil jitter
// returns the unjittered cap, useful in tests.
func nextDelay(attempt int, cfg BackoffConfig, jitter func(d time.Duration) time.Duration) time.Duration {
	base := cfg.Base
	if base <= 0 {
		base = defaultBackoffBase
	}
	maxD := cfg.Max
	if maxD <= 0 {
		maxD = defaultBackoffMax
	}
	// Guard against overflow: a large attempt would shift the duration negative.
	d := maxD
	if attempt >= 0 && attempt < 62 {
		if shifted := base << uint(attempt); shifted > 0 && shifted < maxD {
			d = shifted
		}
	}
	if jitter != nil {
		return jitter(d)
	}
	return d
}

// fullJitter returns a uniformly random duration in [0, d]. math/rand is fine
// here — jitter needs spread, not cryptographic unpredictability.
func fullJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int63n(int64(d) + 1))
}

// Built-in defaults applied when the config (and the rule) leave a knob unset.
const (
	defaultTimeout     = 5 * time.Second
	defaultMaxAttempts = 5
	defaultBackoffBase = 500 * time.Millisecond
	defaultBackoffMax  = 30 * time.Second
)
