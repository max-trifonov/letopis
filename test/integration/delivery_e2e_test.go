//go:build integration

// Package integration — stage-4 delivery e2e suite (S4-08).
// Tests the full pipeline: rule creation → ingest → rules evaluation →
// webhook delivery → retry/DLQ → redeliver. Uses testcontainers for Mongo
// and an in-memory delivery queue to avoid a Redis dependency.
package integration

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/max-trifonov/letopis/internal/delivery"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/metrics"
	"github.com/max-trifonov/letopis/internal/queue"
	"github.com/max-trifonov/letopis/internal/server"
	"github.com/max-trifonov/letopis/internal/service"
	storage "github.com/max-trifonov/letopis/internal/storage/mongo"
	"github.com/max-trifonov/letopis/internal/tenant"

	_ "github.com/max-trifonov/letopis/internal/queue/memory"
)

// deliveryHarness extends the base harness with the webhook delivery pipeline.
// It uses an isolated Prometheus registry so metric assertions don't bleed
// across test runs.
type deliveryHarness struct {
	harness
	met      *metrics.Metrics
	reg      *prometheus.Registry
	stopDisp func()
}

// newDeliveryHarness wires the full delivery stack: rules engine with delivery
// publisher, dispatcher, DLQ. The SSRF policy uses AllowPrivate:true so the
// httptest webhook receiver at 127.0.0.1 is reachable within tests.
func newDeliveryHarness(t *testing.T, secrets map[string]string) *deliveryHarness {
	t.Helper()
	uri := startMongo(t)

	resolver, _, err := tenant.NewResolver([]tenant.Spec{
		{ID: "a", Keys: []tenant.KeySpec{
			{Plaintext: "key-a-dlv", Scopes: []string{"read", "write", "admin"}, Collections: []string{"crm.*"}},
		}},
		{ID: "b", Keys: []tenant.KeySpec{
			{Plaintext: "key-b-dlv", Scopes: []string{"read", "write", "admin"}, Collections: []string{"docs.*"}},
		}},
	})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}

	conn, err := storage.NewConnManager(uri)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	colRepo := storage.NewCollectionRepo(conn)
	cfgResolver := service.NewConfigResolver(colRepo, service.Options{AutoCreate: true})
	snapshotRepo := storage.NewSnapshotRepo(conn)
	ruleRepo := storage.NewRuleRepo(conn)
	dlqRepo := storage.NewDLQRepo(conn)
	auditRepo := storage.NewSystemAuditRepo(conn)

	reg := prometheus.NewRegistry()
	met := metrics.New(reg)

	rulesEngine := service.NewRulesEngine(ruleRepo, slog.Default(), service.WithRuleMetrics(met))

	ingester := service.NewIngester(cfgResolver,
		storage.NewEventRepo(conn),
		storage.NewCurrentRepo(conn),
		service.WithSnapshots(service.NewSnapshotBuilder(snapshotRepo, slog.Default())),
		service.WithRules(rulesEngine),
	)

	// Delivery queue: in-memory, 1 shard for deterministic ordering in tests.
	deliveryQ, err := queue.New(queue.Settings{Driver: queue.DriverMemory, Shards: 1}, nil)
	if err != nil {
		t.Fatalf("delivery queue: %v", err)
	}
	t.Cleanup(func() { _ = deliveryQ.Close() })

	webhookPublisher := service.NewWebhookPublisher(deliveryQ)
	rulesEngine.SetDeliveryPublisher(webhookPublisher)

	// Fast delivery config for tests: tiny backoff, 2 attempts max.
	dispCfg := delivery.Config{
		DefaultTimeout:  200 * time.Millisecond,
		MaxAttempts:     2,
		Backoff:         delivery.BackoffConfig{Base: 5 * time.Millisecond, Max: 10 * time.Millisecond},
		ReclaimInterval: 5 * time.Second,
		ReclaimMinIdle:  30 * time.Second,
	}

	// AllowPrivate so httptest receivers at 127.0.0.1 are reachable.
	gt, err := delivery.NewGuardedTransport(&delivery.SSRFPolicy{AllowPrivate: true})
	if err != nil {
		t.Fatalf("ssrf transport: %v", err)
	}

	disp := delivery.New(deliveryQ, secrets, dispCfg, delivery.Options{
		Sink:      dlqRepo,
		Transport: gt,
		Metrics:   met,
		Jitter:    func(d time.Duration) time.Duration { return d / 2 }, // deterministic jitter
	}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	dispDone := make(chan struct{})
	go func() {
		_ = disp.Run(ctx)
		close(dispDone)
	}()
	stop := func() {
		cancel()
		select {
		case <-dispDone:
		case <-time.After(3 * time.Second):
			t.Error("dispatcher did not stop")
		}
	}
	t.Cleanup(stop)

	reader := service.NewReader(storage.NewEventRepo(conn), storage.NewCurrentRepo(conn), snapshotRepo)
	activities := service.NewActivities(storage.NewFlowRepo(conn))
	catalog := service.NewCatalog(storage.NewStatsRepo(conn), colRepo)
	configAdmin := service.NewCollectionConfigService(colRepo, cfgResolver, auditRepo, slog.Default())
	ruleAdmin := service.NewRuleService(ruleRepo, nil, auditRepo, slog.Default())
	dlqService := service.NewDLQService(dlqRepo, webhookPublisher, slog.Default())

	async := service.NewAsyncIngester(ingester, service.NewPipeline(nil, nil, nil), nil, service.AsyncOptions{})
	batch := service.NewBatchIngester(async, nil, slog.Default())

	handler := server.NewRouter(health.NewRegistry(), resolver, ingester, reader, activities, nil, catalog, configAdmin, batch, ruleAdmin, dlqService)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	h := &deliveryHarness{
		harness:  harness{url: srv.URL, keyA: "key-a-dlv", keyB: "key-b-dlv"},
		met:      met,
		reg:      reg,
		stopDisp: stop,
	}
	return h
}

// verifyHMAC checks that X-HM-Signature is the correct HMAC-SHA256 of body
// under secret.
func verifyHMAC(secret string, body []byte, sig string) bool {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	return sig == want
}

// waitDelivery polls cond until it returns true or 5 s elapses.
func waitDelivery(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting: %s", msg)
}

// TestE2EDeliveryHappyPath: create a rule with a webhook action, ingest a
// matching event, verify the receiver gets a signed POST with the event body.
func TestE2EDeliveryHappyPath(t *testing.T) {
	const secret = "test-secret-1"
	var (
		mu      sync.Mutex
		gotReqs []*http.Request
		gotBody []byte
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotReqs = append(gotReqs, r.Clone(r.Context()))
		gotBody = b
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	h := newDeliveryHarness(t, map[string]string{"whsec_1": secret})

	// Create rule: match update ops on crm.deals.
	ruleBody := fmt.Sprintf(`{
		"name": "notify-updates",
		"enabled": true,
		"condition": {"field": "op", "eq": "update"},
		"actions": [{"type":"webhook","url":%q,"secret_ref":"whsec_1"}]
	}`, receiver.URL)
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/rules", h.keyA, ruleBody)
	h.mustStatus(t, http.StatusCreated, st, "create rule", body)
	ruleMap, _ := body["rule"].(map[string]any)
	ruleID, _ := ruleMap["id"].(string)
	if ruleID == "" {
		t.Fatalf("rule id empty; body=%v", body)
	}

	// Ingest a matching event.
	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-1/state",
		h.keyA, `{"state":{"status":"active"}}`)
	h.mustStatus(t, http.StatusCreated, st, "create state", body)

	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-1/diff",
		h.keyA, `{"op":"update","changes":[{"path":"status","op":"change","old":"active","new":"cancelled"}]}`)
	h.mustStatus(t, http.StatusCreated, st, "update diff", body)

	// Wait for the webhook.
	waitDelivery(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(gotReqs) > 0 }, "webhook delivery")

	mu.Lock()
	req := gotReqs[0]
	rawBody := gotBody
	mu.Unlock()

	// Verify HMAC signature.
	sig := req.Header.Get(delivery.HeaderSignature)
	if !verifyHMAC(secret, rawBody, sig) {
		t.Errorf("HMAC signature invalid: got %q", sig)
	}
	if req.Header.Get(delivery.HeaderDelivery) == "" {
		t.Error("X-HM-Delivery header missing")
	}
	if req.Header.Get(delivery.HeaderRule) != ruleID {
		t.Errorf("X-HM-Rule = %q, want %q", req.Header.Get(delivery.HeaderRule), ruleID)
	}

	// Body must contain event and rule fields.
	var wb map[string]any
	if err := json.Unmarshal(rawBody, &wb); err != nil {
		t.Fatalf("webhook body not JSON: %v", err)
	}
	if _, ok := wb["event"]; !ok {
		t.Error("webhook body missing 'event' field")
	}
	if _, ok := wb["rule"]; !ok {
		t.Error("webhook body missing 'rule' field")
	}
}

// TestE2EDeliveryNonMatch: an event that does NOT match the rule condition must
// not trigger a webhook.
func TestE2EDeliveryNonMatch(t *testing.T) {
	var called atomic.Bool
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	h := newDeliveryHarness(t, map[string]string{"whsec_1": "s"})

	// Rule: only fire on op=delete.
	ruleBody := fmt.Sprintf(`{
		"name": "delete-only",
		"enabled": true,
		"condition": {"field": "op", "eq": "delete"},
		"actions": [{"type":"webhook","url":%q,"secret_ref":"whsec_1"}]
	}`, receiver.URL)
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/rules", h.keyA, ruleBody)
	h.mustStatus(t, http.StatusCreated, st, "create rule", body)

	// Ingest an update — should NOT trigger the rule.
	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-2/state",
		h.keyA, `{"state":{"x":1}}`)
	h.mustStatus(t, http.StatusCreated, st, "create", body)

	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-2/diff",
		h.keyA, `{"op":"update","changes":[{"path":"x","op":"change","old":1,"new":2}]}`)
	h.mustStatus(t, http.StatusCreated, st, "update", body)

	// Give the delivery pipeline time to (not) deliver.
	time.Sleep(200 * time.Millisecond)
	if called.Load() {
		t.Error("non-matching event triggered a webhook")
	}
}

// TestE2EDeliveryDisabledRule: an enabled:false rule must not fire.
func TestE2EDeliveryDisabledRule(t *testing.T) {
	var called atomic.Bool
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	h := newDeliveryHarness(t, map[string]string{"whsec_1": "s"})

	ruleBody := fmt.Sprintf(`{
		"name": "disabled-rule",
		"enabled": false,
		"condition": {"field": "op", "eq": "create"},
		"actions": [{"type":"webhook","url":%q,"secret_ref":"whsec_1"}]
	}`, receiver.URL)
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/rules", h.keyA, ruleBody)
	h.mustStatus(t, http.StatusCreated, st, "create rule", body)

	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-3/state",
		h.keyA, `{"state":{"y":1}}`)
	h.mustStatus(t, http.StatusCreated, st, "create entity", body)

	time.Sleep(200 * time.Millisecond)
	if called.Load() {
		t.Error("disabled rule triggered a webhook")
	}
}

// TestE2EDeliveryRetryAndDLQ: a 5xx receiver causes retries; after max_attempts
// the dead letter appears in GET …/dlq. A redeliver with a fixed receiver clears it.
func TestE2EDeliveryRetryAndDLQ(t *testing.T) {
	var mu sync.Mutex
	calls := 0
	succeed := false // flip after the first redeliver

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		ok := succeed
		mu.Unlock()
		if ok {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer receiver.Close()

	h := newDeliveryHarness(t, map[string]string{"whsec_1": "s"})

	ruleBody := fmt.Sprintf(`{
		"name": "retry-rule",
		"enabled": true,
		"condition": {"field": "op", "eq": "create"},
		"actions": [{"type":"webhook","url":%q,"secret_ref":"whsec_1","retry":{"max_attempts":2}}]
	}`, receiver.URL)
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/rules", h.keyA, ruleBody)
	h.mustStatus(t, http.StatusCreated, st, "create rule", body)
	ruleMap, _ := body["rule"].(map[string]any)
	ruleID, _ := ruleMap["id"].(string)
	if ruleID == "" {
		t.Fatalf("rule id empty; body=%v", body)
	}

	// Trigger the rule.
	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-4/state",
		h.keyA, `{"state":{"z":1}}`)
	h.mustStatus(t, http.StatusCreated, st, "create entity", body)

	// Wait for DLQ entry to appear after retries exhausted.
	dlqPath := "/api/v1/collections/crm.deals/rules/" + ruleID + "/dlq"
	waitDelivery(t, func() bool {
		st, body = h.do(t, http.MethodGet, dlqPath, h.keyA, "")
		if st != http.StatusOK {
			return false
		}
		items, ok := body["items"].([]any)
		return ok && len(items) > 0
	}, "delivery in DLQ")

	st, body = h.do(t, http.MethodGet, dlqPath, h.keyA, "")
	h.mustStatus(t, http.StatusOK, st, "list dlq", body)
	items := body["items"].([]any)
	if len(items) == 0 {
		t.Fatal("DLQ is empty after exhausted retries")
	}
	dl := items[0].(map[string]any)
	if dl["delivery_id"] == "" {
		t.Error("DLQ entry missing delivery_id")
	}

	// Fix the receiver and redeliver.
	mu.Lock()
	succeed = true
	mu.Unlock()

	st, body = h.do(t, http.MethodPost, dlqPath+":redeliver", h.keyA, "")
	h.mustStatus(t, http.StatusAccepted, st, "redeliver", body)
	if body["requeued"].(float64) < 1 {
		t.Error("redeliver returned requeued=0")
	}

	// DLQ should clear after successful redeliver.
	waitDelivery(t, func() bool {
		st, body = h.do(t, http.MethodGet, dlqPath, h.keyA, "")
		items, ok := body["items"].([]any)
		return ok && len(items) == 0
	}, "DLQ cleared after redeliver")
}

// TestE2EDeliverySSRFBlocked: a rule with a private/loopback URL under the
// default SSRF policy is blocked immediately and parked in the DLQ with reason
// containing "ssrf".
func TestE2EDeliverySSRFBlocked(t *testing.T) {
	// Start a receiver on localhost — blocked by strict SSRF policy.
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	// Use a strict policy (no AllowPrivate) via a separate harness.
	h := newDeliveryHarnessStrict(t, map[string]string{"whsec_1": "s"})

	ruleBody := fmt.Sprintf(`{
		"name": "ssrf-rule",
		"enabled": true,
		"condition": {"field": "op", "eq": "create"},
		"actions": [{"type":"webhook","url":%q,"secret_ref":"whsec_1"}]
	}`, receiver.URL)
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/rules", h.keyA, ruleBody)
	h.mustStatus(t, http.StatusCreated, st, "create rule", body)
	ruleMap, _ := body["rule"].(map[string]any)
	ruleID, _ := ruleMap["id"].(string)
	if ruleID == "" {
		t.Fatalf("rule id empty; body=%v", body)
	}

	st, body = h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/entities/d-5/state",
		h.keyA, `{"state":{"a":1}}`)
	h.mustStatus(t, http.StatusCreated, st, "create entity", body)

	dlqPath := "/api/v1/collections/crm.deals/rules/" + ruleID + "/dlq"
	waitDelivery(t, func() bool {
		st, body = h.do(t, http.MethodGet, dlqPath, h.keyA, "")
		items, ok := body["items"].([]any)
		return ok && len(items) > 0
	}, "ssrf-blocked in DLQ")

	st, body = h.do(t, http.MethodGet, dlqPath, h.keyA, "")
	items := body["items"].([]any)
	dl := items[0].(map[string]any)
	lastErr, _ := dl["last_error"].(string)
	if lastErr == "" {
		t.Errorf("DLQ entry for SSRF block has empty last_error: %v", dl)
	}
	// Attempts must be 0 (not retried).
	if att, _ := dl["attempts"].(float64); att > 0 {
		t.Errorf("ssrf-blocked delivery was retried %v times, want 0", att)
	}
}

// TestE2EDeliveryTenantIsolation: rules and DLQ of tenant A are not visible
// to tenant B, even on the same Mongo instance.
func TestE2EDeliveryTenantIsolation(t *testing.T) {
	h := newDeliveryHarness(t, map[string]string{"whsec_1": "s"})

	// Create a rule as tenant A.
	st, body := h.do(t, http.MethodPost, "/api/v1/collections/crm.deals/rules", h.keyA,
		`{"name":"isolate-rule","enabled":true,"condition":{"field":"op","eq":"create"},"actions":[{"type":"log","level":"info"}]}`)
	h.mustStatus(t, http.StatusCreated, st, "create rule A", body)
	ruleMap, _ := body["rule"].(map[string]any)
	ruleID, _ := ruleMap["id"].(string)

	// Tenant B must not see tenant A's rules.
	st, body = h.do(t, http.MethodGet, "/api/v1/collections/docs.files/rules", h.keyB, "")
	// B's collection does not have any rules; list should be empty or 200 with []
	if st != http.StatusOK {
		t.Errorf("tenant B list rules: status=%d", st)
	}
	rules, _ := body["rules"].([]any)
	for _, r := range rules {
		rm := r.(map[string]any)
		if rm["id"] == ruleID {
			t.Error("tenant A's rule visible to tenant B")
		}
	}

	// Tenant B accessing tenant A's rule by ID must fail.
	st, _ = h.do(t, http.MethodGet, "/api/v1/collections/crm.deals/rules/"+ruleID, h.keyB, "")
	if st != http.StatusForbidden {
		t.Errorf("tenant B reading tenant A's rule: status=%d, want 403", st)
	}
}

// newDeliveryHarnessStrict is like newDeliveryHarness but with the SSRF guard
// using the default strict policy (AllowPrivate:false) so loopback is blocked.
func newDeliveryHarnessStrict(t *testing.T, secrets map[string]string) *deliveryHarness {
	t.Helper()
	uri := startMongo(t)

	resolver, _, err := tenant.NewResolver([]tenant.Spec{
		{ID: "a", Keys: []tenant.KeySpec{
			{Plaintext: "key-a-dlv", Scopes: []string{"read", "write", "admin"}, Collections: []string{"crm.*"}},
		}},
	})
	if err != nil {
		t.Fatalf("resolver: %v", err)
	}

	conn, err := storage.NewConnManager(uri)
	if err != nil {
		t.Fatalf("conn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })

	colRepo := storage.NewCollectionRepo(conn)
	cfgResolver := service.NewConfigResolver(colRepo, service.Options{AutoCreate: true})
	snapshotRepo := storage.NewSnapshotRepo(conn)
	ruleRepo := storage.NewRuleRepo(conn)
	dlqRepo := storage.NewDLQRepo(conn)
	auditRepo := storage.NewSystemAuditRepo(conn)

	reg := prometheus.NewRegistry()
	met := metrics.New(reg)
	rulesEngine := service.NewRulesEngine(ruleRepo, slog.Default(), service.WithRuleMetrics(met))

	ingester := service.NewIngester(cfgResolver,
		storage.NewEventRepo(conn),
		storage.NewCurrentRepo(conn),
		service.WithSnapshots(service.NewSnapshotBuilder(snapshotRepo, slog.Default())),
		service.WithRules(rulesEngine),
	)

	deliveryQ, err := queue.New(queue.Settings{Driver: queue.DriverMemory, Shards: 1}, nil)
	if err != nil {
		t.Fatalf("delivery queue: %v", err)
	}
	t.Cleanup(func() { _ = deliveryQ.Close() })

	webhookPublisher := service.NewWebhookPublisher(deliveryQ)
	rulesEngine.SetDeliveryPublisher(webhookPublisher)

	dispCfg := delivery.Config{
		DefaultTimeout:  200 * time.Millisecond,
		MaxAttempts:     2,
		Backoff:         delivery.BackoffConfig{Base: 5 * time.Millisecond, Max: 10 * time.Millisecond},
		ReclaimInterval: 5 * time.Second,
		ReclaimMinIdle:  30 * time.Second,
	}

	// Strict policy: AllowPrivate:false blocks 127.0.0.1.
	gt, err := delivery.NewGuardedTransport(&delivery.SSRFPolicy{AllowPrivate: false})
	if err != nil {
		t.Fatalf("ssrf transport: %v", err)
	}

	disp := delivery.New(deliveryQ, secrets, dispCfg, delivery.Options{
		Sink:      dlqRepo,
		Transport: gt,
		Metrics:   met,
		Jitter:    func(d time.Duration) time.Duration { return 0 },
	}, slog.Default())

	ctx, cancel := context.WithCancel(context.Background())
	dispDone := make(chan struct{})
	go func() {
		_ = disp.Run(ctx)
		close(dispDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-dispDone:
		case <-time.After(3 * time.Second):
		}
	})

	reader := service.NewReader(storage.NewEventRepo(conn), storage.NewCurrentRepo(conn), snapshotRepo)
	activities := service.NewActivities(storage.NewFlowRepo(conn))
	catalog := service.NewCatalog(storage.NewStatsRepo(conn), colRepo)
	configAdmin := service.NewCollectionConfigService(colRepo, cfgResolver, auditRepo, slog.Default())
	ruleAdmin := service.NewRuleService(ruleRepo, nil, auditRepo, slog.Default())
	dlqService := service.NewDLQService(dlqRepo, webhookPublisher, slog.Default())

	async := service.NewAsyncIngester(ingester, service.NewPipeline(nil, nil, nil), nil, service.AsyncOptions{})
	batch := service.NewBatchIngester(async, nil, slog.Default())

	handler := server.NewRouter(health.NewRegistry(), resolver, ingester, reader, activities, nil, catalog, configAdmin, batch, ruleAdmin, dlqService)
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	return &deliveryHarness{
		harness: harness{url: srv.URL, keyA: "key-a-dlv"},
		met:     met,
		reg:     reg,
	}
}
