package delivery

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/queue"

	// The memory driver registers itself so queue.New can build it for the test.
	_ "github.com/max-trifonov/letopis/internal/queue/memory"
)

// recordSink captures parked dead letters.
type recordSink struct {
	mu   sync.Mutex
	dead []domain.DeadLetter
}

func (s *recordSink) Save(_ context.Context, dl domain.DeadLetter) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dead = append(s.dead, dl)
	return nil
}

func (s *recordSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dead)
}

func (s *recordSink) first() domain.DeadLetter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dead[0]
}

// memQueue builds a single-shard in-memory queue so every task lands on shard 0.
func memQueue(t *testing.T) queue.Queue {
	t.Helper()
	q, err := queue.New(queue.Settings{Driver: queue.DriverMemory, Shards: 1}, nil)
	if err != nil {
		t.Fatalf("build memory queue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// runDispatcher starts d.Run in the background and returns a stop func that
// cancels it and waits for it to return.
func runDispatcher(t *testing.T, d *Dispatcher) (context.Context, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = d.Run(ctx)
		close(done)
	}()
	return ctx, func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("dispatcher did not stop")
		}
	}
}

func publishTask(t *testing.T, q queue.Queue, task Task) {
	t.Helper()
	payload, err := EncodeTask(task)
	if err != nil {
		t.Fatalf("encode task: %v", err)
	}
	if err := q.Publish(context.Background(), queue.Message{Key: task.RuleID, Payload: payload}); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting: " + msg)
}

func TestDispatcherDeliversSignedRequest(t *testing.T) {
	var (
		mu   sync.Mutex
		got  *http.Request
		body []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		got = r
		body = b
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := memQueue(t)
	sink := &recordSink{}
	d := New(q, map[string]string{"k1": "sekret"}, Config{}, Options{Sink: sink}, nil)
	_, stop := runDispatcher(t, d)
	defer stop()

	payload := []byte(`{"event":{"entity_id":"d-1"}}`)
	publishTask(t, q, Task{
		Tenant:     TenantRef{ID: "acme"},
		DeliveryID: "dlv_1",
		RuleID:     "rule_1",
		Collection: "crm.deals",
		URL:        srv.URL,
		SecretRef:  "k1",
		Body:       payload,
	})

	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return got != nil }, "request to arrive")

	mu.Lock()
	defer mu.Unlock()
	if got.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", got.Method)
	}
	if string(body) != string(payload) {
		t.Errorf("body = %q, want %q", body, payload)
	}
	if h := got.Header.Get(HeaderSignature); h != Sign("sekret", payload) {
		t.Errorf("signature = %q, want %q", h, Sign("sekret", payload))
	}
	if got.Header.Get(HeaderDelivery) != "dlv_1" {
		t.Errorf("delivery header = %q, want dlv_1", got.Header.Get(HeaderDelivery))
	}
	if got.Header.Get(HeaderRule) != "rule_1" {
		t.Errorf("rule header = %q, want rule_1", got.Header.Get(HeaderRule))
	}
	if sink.count() != 0 {
		t.Errorf("a 2xx delivery should not hit the DLQ; got %d", sink.count())
	}
}

func TestDispatcherRetriesThenDLQ(t *testing.T) {
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	q := memQueue(t)
	sink := &recordSink{}
	// MaxAttempts=2 and zero jitter/backoff so retries are immediate: attempt 1
	// fails and reschedules, attempt 2 exhausts and parks in the DLQ.
	cfg := Config{MaxAttempts: 2, Backoff: BackoffConfig{Base: time.Millisecond, Max: time.Millisecond}}
	d := New(q, map[string]string{"k1": "x"}, cfg, Options{Sink: sink, Jitter: func(time.Duration) time.Duration { return 0 }}, nil)
	_, stop := runDispatcher(t, d)
	defer stop()

	publishTask(t, q, Task{
		Tenant:     TenantRef{ID: "acme"},
		DeliveryID: "dlv_x",
		RuleID:     "rule_1",
		Collection: "crm.deals",
		URL:        srv.URL,
		SecretRef:  "k1",
		Body:       []byte(`{}`),
	})

	waitFor(t, func() bool { return sink.count() == 1 }, "delivery to land in DLQ")

	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 2 {
		t.Errorf("HTTP attempts = %d, want 2 (max_attempts)", gotCalls)
	}
	dl := sink.first()
	if dl.DeliveryID != "dlv_x" || dl.Attempts != 2 || dl.RuleID != "rule_1" {
		t.Errorf("dead letter wrong: %+v", dl)
	}
}

func TestDispatcherUnknownSecretGoesToDLQWithoutCall(t *testing.T) {
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		calls++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := memQueue(t)
	sink := &recordSink{}
	d := New(q, map[string]string{}, Config{}, Options{Sink: sink}, nil) // no secrets
	_, stop := runDispatcher(t, d)
	defer stop()

	publishTask(t, q, Task{
		Tenant:     TenantRef{ID: "acme"},
		DeliveryID: "dlv_n",
		RuleID:     "rule_1",
		Collection: "crm.deals",
		URL:        srv.URL,
		SecretRef:  "missing",
		Body:       []byte(`{}`),
	})

	waitFor(t, func() bool { return sink.count() == 1 }, "delivery to land in DLQ")

	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 0 {
		t.Errorf("unknown secret should not POST; got %d calls", gotCalls)
	}
	if dl := sink.first(); dl.LastError != reasonUnknownSecret {
		t.Errorf("DLQ reason = %q, want %q", dl.LastError, reasonUnknownSecret)
	}
}

func TestDispatcherSSRFBlockedGoesToDLQWithoutRetry(t *testing.T) {
	// A localhost receiver: blocked by the default SSRF policy.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := memQueue(t)
	sink := &recordSink{}
	// GuardedTransport with default policy blocks loopback.
	gt, err := NewGuardedTransport(&SSRFPolicy{})
	if err != nil {
		t.Fatalf("NewGuardedTransport: %v", err)
	}
	cfg := Config{MaxAttempts: 5} // would retry 5x if not blocked immediately
	d := New(q, map[string]string{"k1": "s"}, cfg, Options{Sink: sink, Transport: gt}, nil)
	_, stop := runDispatcher(t, d)
	defer stop()

	publishTask(t, q, Task{
		Tenant:     TenantRef{ID: "acme"},
		DeliveryID: "dlv_ssrf",
		RuleID:     "rule_1",
		Collection: "crm",
		URL:        srv.URL, // points at 127.0.0.1
		SecretRef:  "k1",
		Body:       []byte(`{}`),
	})

	// Should land in DLQ without any retries (blocked is not retryable).
	waitFor(t, func() bool { return sink.count() == 1 }, "ssrf-blocked delivery in DLQ")
	if dl := sink.first(); dl.LastError == "" || dl.Attempts > 0 {
		t.Errorf("ssrf DLQ entry wrong: attempts=%d error=%q", dl.Attempts, dl.LastError)
	}
}

func TestDispatcherSSRFAllowPrivate(t *testing.T) {
	var called bool
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		called = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := memQueue(t)
	sink := &recordSink{}
	// With AllowPrivate=true, localhost is permitted.
	gt, err := NewGuardedTransport(&SSRFPolicy{AllowPrivate: true})
	if err != nil {
		t.Fatalf("NewGuardedTransport: %v", err)
	}
	d := New(q, map[string]string{"k1": "s"}, Config{}, Options{Sink: sink, Transport: gt}, nil)
	_, stop := runDispatcher(t, d)
	defer stop()

	publishTask(t, q, Task{
		Tenant: TenantRef{ID: "acme"}, DeliveryID: "dlv_allow", RuleID: "rule_1",
		Collection: "crm", URL: srv.URL, SecretRef: "k1", Body: []byte(`{}`),
	})

	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return called }, "delivery to succeed")
	if sink.count() != 0 {
		t.Errorf("allowed delivery should not be in DLQ; got %d", sink.count())
	}
}

func TestDispatcherMetrics(t *testing.T) {
	var called bool
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		called = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := &stubMetricsRecord{}
	met := &stubMetrics{record: rec}

	q := memQueue(t)
	d := New(q, map[string]string{"k1": "s"}, Config{}, Options{Metrics: met}, nil)
	_, stop := runDispatcher(t, d)
	defer stop()

	publishTask(t, q, Task{
		Tenant: TenantRef{ID: "acme"}, DeliveryID: "dlv_m1", RuleID: "r1",
		Collection: "c", URL: srv.URL, SecretRef: "k1", Body: []byte(`{}`),
	})

	waitFor(t, func() bool { mu.Lock(); defer mu.Unlock(); return called }, "delivery")
	waitFor(t, func() bool {
		rec.mu.Lock()
		defer rec.mu.Unlock()
		return slices.Contains(rec.outcomes, "delivered")
	}, "delivered metric")
}

type stubMetricsRecord struct {
	mu       sync.Mutex
	outcomes []string
	retries  int
}

// stubMetrics records DeliveryMetrics calls for assertions.
type stubMetrics struct {
	record *stubMetricsRecord
}

func (s *stubMetrics) ObserveDelivery(result string, _ time.Duration) {
	s.record.mu.Lock()
	s.record.outcomes = append(s.record.outcomes, result)
	s.record.mu.Unlock()
}

func (s *stubMetrics) IncDeliveryRetry() {
	s.record.mu.Lock()
	s.record.retries++
	s.record.mu.Unlock()
}

func TestDispatcherTimeoutIsRetryable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	q := memQueue(t)
	sink := &recordSink{}
	// One attempt, a 20ms timeout: the slow handler times out and, with no retries
	// left, the delivery is parked.
	cfg := Config{MaxAttempts: 1}
	d := New(q, map[string]string{"k1": "x"}, cfg, Options{Sink: sink, Jitter: func(time.Duration) time.Duration { return 0 }}, nil)
	_, stop := runDispatcher(t, d)
	defer stop()

	publishTask(t, q, Task{
		Tenant:     TenantRef{ID: "acme"},
		DeliveryID: "dlv_t",
		RuleID:     "rule_1",
		Collection: "crm.deals",
		URL:        srv.URL,
		SecretRef:  "k1",
		TimeoutMS:  20,
		Body:       []byte(`{}`),
	})

	waitFor(t, func() bool { return sink.count() == 1 }, "timed-out delivery to land in DLQ")
}
