package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/max-trifonov/letopis/internal/tenant"
)

// KeyResolver is the transport's view of tenant resolution. Declared here so
// handlers depend on a narrow port rather than the concrete resolver.
type KeyResolver interface {
	Resolve(raw string) (tenant.Principal, error)
}

// RequireAuth authenticates every request on the group it guards: reads the
// bearer key, resolves it to a Principal and stores it in the context. A
// missing or invalid key is always 401 — no distinction, to avoid confirming
// key existence to a probe. The tenant is taken from the key, never the URL.
func RequireAuth(r KeyResolver) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			p, err := r.Resolve(bearerToken(req))
			if err != nil {
				if errors.Is(err, tenant.ErrNoKey) {
					writeError(w, http.StatusUnauthorized, "missing API key")
					return
				}
				writeError(w, http.StatusUnauthorized, "invalid API key")
				return
			}
			next.ServeHTTP(w, req.WithContext(tenant.NewContext(req.Context(), p)))
		})
	}
}

// RequireScope gates a route on a scope. Must run inside a group already wrapped
// by RequireAuth; an unauthenticated context is treated as 401, not a panic.
func RequireScope(s tenant.Scope) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			p, ok := tenant.FromContext(req.Context())
			if !ok {
				writeError(w, http.StatusUnauthorized, "missing API key")
				return
			}
			if !p.HasScope(s) {
				writeError(w, http.StatusForbidden, "insufficient scope")
				return
			}
			next.ServeHTTP(w, req)
		})
	}
}

// bearerToken extracts the raw key from "Authorization: Bearer <key>". A
// header without the Bearer scheme yields an empty string, which Resolve
// rejects as ErrNoKey.
func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	h := r.Header.Get("Authorization")
	if len(h) < len(prefix) || !strings.EqualFold(h[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(h[len(prefix):])
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
