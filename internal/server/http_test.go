package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/max-trifonov/letopis/internal/health"
)

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func TestServiceEndpoints(t *testing.T) {
	r := newRouter(health.NewRegistry(), testResolver(t), nil, nil, nil, nil, nil, nil, nil, nil, nil)

	for path, want := range map[string]int{
		"/healthz": http.StatusOK,
		"/readyz":  http.StatusOK,
		"/version": http.StatusOK,
		"/metrics": http.StatusOK,
	} {
		if rec := get(t, r, path); rec.Code != want {
			t.Errorf("GET %s = %d, want %d", path, rec.Code, want)
		}
	}
}

func TestReadyzReportsFailingCheck(t *testing.T) {
	checks := health.NewRegistry()
	checks.Register("mongodb", func(context.Context) error {
		return errors.New("connection refused")
	})

	rec := get(t, newRouter(checks, testResolver(t), nil, nil, nil, nil, nil, nil, nil, nil, nil), "/readyz")
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET /readyz = %d, want 503", rec.Code)
	}
	var body struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Checks["mongodb"] == "" {
		t.Errorf("response misses the failing check: %s", rec.Body.String())
	}
}
