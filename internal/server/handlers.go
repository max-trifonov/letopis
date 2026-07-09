package server

import (
	"encoding/json"
	"net/http"

	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/version"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	// Encoding a flat struct of strings cannot fail; ignoring the
	// network error is fine, the client is gone anyway.
	_ = json.NewEncoder(w).Encode(v)
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func handleReadyz(checks *health.Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		failures := checks.Run(r.Context())
		if len(failures) == 0 {
			writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
			return
		}
		details := make(map[string]string, len(failures))
		for name, err := range failures {
			details[name] = err.Error()
		}
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status": "unavailable",
			"checks": details,
		})
	}
}

func handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version": version.Version,
		"commit":  version.Commit,
		"date":    version.Date,
	})
}
