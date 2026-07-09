// Package server hosts the transport layer: the HTTP (REST) and gRPC servers.
// Both expose the same service surface; REST is the canonical contract and gRPC
// mirrors it.
package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/max-trifonov/letopis/internal/config"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/tenant"
)

const shutdownTimeout = 10 * time.Second

type HTTP struct {
	cfg config.Listen
	log *slog.Logger
	srv *http.Server
}

func NewHTTP(cfg config.Listen, log *slog.Logger, checks *health.Registry, resolver KeyResolver, ingest IngestService, read ReadService, activities ActivityService, tickets TicketReadService, catalog CatalogService, cfgAdmin ConfigService, batch BatchService, ruleAdmin RuleService, dlq DLQService) *HTTP {
	log = log.With("component", "http")
	return &HTTP{
		cfg: cfg,
		log: log,
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           newRouter(checks, resolver, ingest, read, activities, tickets, catalog, cfgAdmin, batch, ruleAdmin, dlq),
			ReadHeaderTimeout: 10 * time.Second,
			ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
		},
	}
}

// NewRouter is exported for the e2e suite, which drives the API over an httptest
// server. Production traffic goes through NewHTTP. A nil service disables its routes.
func NewRouter(checks *health.Registry, resolver KeyResolver, ingest IngestService, read ReadService, activities ActivityService, tickets TicketReadService, catalog CatalogService, cfgAdmin ConfigService, batch BatchService, ruleAdmin RuleService, dlq DLQService) http.Handler {
	return newRouter(checks, resolver, ingest, read, activities, tickets, catalog, cfgAdmin, batch, ruleAdmin, dlq)
}

func newRouter(checks *health.Registry, resolver KeyResolver, ingest IngestService, read ReadService, activities ActivityService, tickets TicketReadService, catalog CatalogService, cfgAdmin ConfigService, batch BatchService, ruleAdmin RuleService, dlq DLQService) http.Handler {
	r := chi.NewRouter()
	// X-Forwarded-For is not trusted here; proxy header trust requires an
	// explicit deployment opt-in.
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", handleHealthz)
	r.Get("/readyz", handleReadyz(checks))
	r.Get("/version", handleVersion)
	r.Method(http.MethodGet, "/metrics", promhttp.Handler())

	// Everything under /api/v1 is tenant-authenticated.
	r.Route("/api/v1", func(r chi.Router) {
		r.Use(RequireAuth(resolver))
		registerEntityRoutes(r, ingest, read)
		registerCatalogRoutes(r, catalog)
		registerConfigRoutes(r, cfgAdmin)
		registerRuleRoutes(r, ruleAdmin)
		registerDLQRoutes(r, dlq)
		registerBatchRoutes(r, batch)
		registerFlowRoutes(r, activities)
		registerTicketRoutes(r, tickets)
	})

	return r
}

// A nil service skips its routes so test routers can compose a partial set.
func registerEntityRoutes(r chi.Router, ingest IngestService, read ReadService) {
	base := "/collections/{collection}/entities/{entityId}"
	if ingest != nil {
		h := ingestHandlers{svc: ingest}
		r.With(RequireScope(tenant.ScopeWrite)).Post(base+"/state", h.state)
		r.With(RequireScope(tenant.ScopeWrite)).Post(base+"/diff", h.diff)
		r.With(RequireScope(tenant.ScopeWrite)).Post(base+"/delete", h.delete)
	}
	if read != nil {
		h := readHandlers{svc: read}
		r.With(RequireScope(tenant.ScopeRead)).Get(base+"/history", h.history)
		r.With(RequireScope(tenant.ScopeRead)).Get(base+"/state", h.state)
	}
}

func registerCatalogRoutes(r chi.Router, catalog CatalogService) {
	if catalog == nil {
		return
	}
	h := catalogHandlers{svc: catalog}
	r.With(RequireScope(tenant.ScopeRead)).Get("/collections", h.list)
}

func registerConfigRoutes(r chi.Router, cfgAdmin ConfigService) {
	if cfgAdmin == nil {
		return
	}
	h := configHandlers{svc: cfgAdmin}
	path := "/collections/{collection}/config"
	r.With(RequireScope(tenant.ScopeAdmin)).Get(path, h.get)
	r.With(RequireScope(tenant.ScopeAdmin)).Put(path, h.put)
}

func registerRuleRoutes(r chi.Router, ruleAdmin RuleService) {
	if ruleAdmin == nil {
		return
	}
	h := ruleHandlers{svc: ruleAdmin}
	base := "/collections/{collection}/rules"
	r.With(RequireScope(tenant.ScopeAdmin)).Post(base, h.create)
	r.With(RequireScope(tenant.ScopeAdmin)).Get(base, h.list)
	r.With(RequireScope(tenant.ScopeAdmin)).Get(base+"/{ruleId}", h.get)
	r.With(RequireScope(tenant.ScopeAdmin)).Put(base+"/{ruleId}", h.update)
	r.With(RequireScope(tenant.ScopeAdmin)).Delete(base+"/{ruleId}", h.del)
}

func registerDLQRoutes(r chi.Router, dlq DLQService) {
	if dlq == nil {
		return
	}
	h := dlqHandlers{svc: dlq}
	base := "/collections/{collection}/rules/{ruleId}/dlq"
	r.With(RequireScope(tenant.ScopeAdmin)).Get(base, h.list)
	r.With(RequireScope(tenant.ScopeAdmin)).Post(base+":redeliver", h.redeliver)
}

func registerBatchRoutes(r chi.Router, batch BatchService) {
	if batch == nil {
		return
	}
	h := batchHandlers{svc: batch}
	r.With(RequireScope(tenant.ScopeWrite)).Post("/events:batch", h.ingest)
}

func registerFlowRoutes(r chi.Router, activities ActivityService) {
	if activities == nil {
		return
	}
	h := activityHandlers{svc: activities}
	r.With(RequireScope(tenant.ScopeWrite)).Post("/activities", h.record)
	r.With(RequireScope(tenant.ScopeRead)).Get("/flows/{flowId}", h.flow)
}

func registerTicketRoutes(r chi.Router, tickets TicketReadService) {
	if tickets == nil {
		return
	}
	h := ticketHandlers{svc: tickets}
	r.With(RequireScope(tenant.ScopeRead)).Get("/tickets/{ticketId}", h.get)
}

// Run serves until the context is cancelled, then drains in-flight
// requests within shutdownTimeout.
func (h *HTTP) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", h.cfg.Addr)
	if err != nil {
		return err
	}
	h.log.Info("http listening", "addr", ln.Addr().String(), "tls", h.cfg.TLS.Enabled)

	errc := make(chan error, 1)
	go func() {
		if h.cfg.TLS.Enabled {
			errc <- h.srv.ServeTLS(ln, h.cfg.TLS.CertFile, h.cfg.TLS.KeyFile)
		} else {
			errc <- h.srv.Serve(ln)
		}
	}()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := h.srv.Shutdown(sctx); err != nil {
		return err
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	h.log.Info("http stopped")
	return nil
}
