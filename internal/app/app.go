// Package app assembles a Letopis instance from configuration and an
// extension registry. It is the public seam between cmd binaries and the
// core: distributions call Run with their own ext.Registry.
package app

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/max-trifonov/letopis/internal/config"
	"github.com/max-trifonov/letopis/internal/delivery"
	"github.com/max-trifonov/letopis/internal/domain"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/metrics"
	"github.com/max-trifonov/letopis/internal/plugin"
	"github.com/max-trifonov/letopis/internal/queue"

	// Queue drivers register themselves with queue.New via init (database/sql's
	// pattern); blank-importing them here keeps the queue package free of an
	// import cycle with its sub-packages.
	_ "github.com/max-trifonov/letopis/internal/queue/memory"
	_ "github.com/max-trifonov/letopis/internal/queue/redisstreams"
	"github.com/max-trifonov/letopis/internal/server"
	"github.com/max-trifonov/letopis/internal/service"
	storage "github.com/max-trifonov/letopis/internal/storage/mongo"
	redisstore "github.com/max-trifonov/letopis/internal/storage/redis"
	"github.com/max-trifonov/letopis/internal/tenant"
	"github.com/max-trifonov/letopis/internal/version"
	"github.com/max-trifonov/letopis/internal/worker"
	"github.com/max-trifonov/letopis/pkg/ext"
)

var errNilRegistry = errors.New("app: ext registry must be complete, start from ext.Defaults()")

// shutdownGrace bounds how long we wait for MongoDB clients to disconnect
// cleanly during shutdown.
const shutdownGrace = 5 * time.Second

func Run(ctx context.Context, cfg *config.Config, log *slog.Logger, reg *ext.Registry) error {
	if reg == nil || reg.Tenants == nil || reg.Metering == nil {
		return errNilRegistry
	}

	resolver, warnings, err := tenant.NewResolver(cfg.TenantSpecs())
	if err != nil {
		return err
	}
	for _, w := range warnings {
		log.Warn(w)
	}

	conn, err := storage.NewConnManager(cfg.MongoDB.URI)
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if cerr := conn.Close(sctx); cerr != nil {
			log.Warn("closing mongo clients", "err", cerr)
		}
	}()

	checks := health.NewRegistry()
	checks.Register("mongo", conn.Ping)

	colRepo := storage.NewCollectionRepo(conn)
	eventRepo := storage.NewEventRepo(conn)
	currentRepo := storage.NewCurrentRepo(conn)
	snapshotRepo := storage.NewSnapshotRepo(conn)
	flowRepo := storage.NewFlowRepo(conn)
	ruleRepo := storage.NewRuleRepo(conn)
	cfgResolver := service.NewConfigResolver(colRepo, service.Options{AutoCreate: cfg.Collections.AutoCreate})

	met := metrics.New(prometheus.DefaultRegisterer)

	// On ext.Defaults() the registry is empty and the pipeline runs exactly as before.
	pluginHost := plugin.NewHost(reg.PreStore, reg.PostStore, reg.Actions, met)

	snapshotBuilder := service.NewSnapshotBuilder(snapshotRepo, log)
	rulesEngine := service.NewRulesEngine(ruleRepo, log,
		service.WithRuleMetrics(met),
		service.WithRuleCacheTTL(time.Duration(cfg.Rules.CacheTTLSeconds)*time.Second),
		service.WithActionDispatcher(pluginHost),
	)
	ingester := service.NewIngester(cfgResolver, eventRepo, currentRepo,
		service.WithSnapshots(snapshotBuilder),
		service.WithRules(rulesEngine),
		service.WithPlugins(pluginHost),
	)
	reader := service.NewReader(eventRepo, currentRepo, snapshotRepo)
	activities := service.NewActivities(flowRepo)
	catalog := service.NewCatalog(storage.NewStatsRepo(conn), colRepo)
	configAdmin := service.NewCollectionConfigService(colRepo, cfgResolver, storage.NewSystemAuditRepo(conn), log)

	var rdb redis.UniversalClient
	if cfg.Redis.Addr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: cfg.Redis.Addr, Password: cfg.Redis.Password, DB: cfg.Redis.DB})
		defer func() { _ = rdb.Close() }()
	}

	// Without Redis, ticket lookup and the Redis idempotency barrier are
	// unavailable; the storage {event_id} index remains the backstop.
	var ticketSvc *service.TicketService
	var ticketRead server.TicketReadService
	var idempStore *redisstore.IdempotencyStore
	// Without Redis the rules engine falls back to the cache TTL for invalidation.
	var ruleInvalidator *redisstore.RuleInvalidator
	if rdb != nil {
		ticketSvc = service.NewTicketService(redisstore.NewTicketStore(rdb, time.Duration(cfg.Tickets.TTLHours)*time.Hour))
		ticketRead = ticketSvc
		idempStore = redisstore.NewIdempotencyStore(rdb, time.Duration(cfg.Idempotency.TTLHours)*time.Hour)
		ruleInvalidator = redisstore.NewRuleInvalidator(rdb)
	}

	ruleAdmin := service.NewRuleService(ruleRepo, ruleCacheInvalidator(ruleInvalidator), storage.NewSystemAuditRepo(conn), log)

	durableQ, fastQ, err := buildQueues(cfg, rdb)
	if err != nil {
		return err
	}
	if durableQ != nil {
		defer func() { _ = durableQ.Close() }()
	}
	if fastQ != nil {
		defer func() { _ = fastQ.Close() }()
	}

	deliveryQ, err := buildDeliveryQueue(cfg, rdb)
	if err != nil {
		return err
	}
	defer func() { _ = deliveryQ.Close() }()
	webhookPublisher := service.NewWebhookPublisher(deliveryQ)
	rulesEngine.SetDeliveryPublisher(webhookPublisher)

	dlqRepo := storage.NewDLQRepo(conn)
	dlqService := service.NewDLQService(dlqRepo, webhookPublisher, log)

	sampler := buildSampler(cfg, met, log, durableQ, fastQ)
	asyncIngester := service.NewAsyncIngester(
		ingester,
		service.NewPipeline(durableQ, fastQ, met),
		ticketSvc,
		service.AsyncOptions{Idempotency: idempForAsync(idempStore), Limiter: sampler, Logger: log},
	)

	log.Info(
		"starting letopis",
		"version", version.Version,
		"commit", version.Commit,
		"role", cfg.Role,
		"tenants", len(cfg.Tenants),
		"auto_create", cfg.Collections.AutoCreate,
		"queue", cfg.Queue.Driver,
		"tickets", ticketSvc != nil,
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error { return sampler.Run(ctx) })

	if ruleInvalidator != nil {
		g.Go(func() error { return ruleInvalidator.Subscribe(ctx, rulesEngine.Invalidate) })
	}

	batchIngester := service.NewBatchIngester(asyncIngester, ticketSvc, log)

	if cfg.Role.ServesAPI() {
		httpSrv := server.NewHTTP(cfg.Server.HTTP, log, checks, resolver, asyncIngester, reader, activities, ticketRead, catalog, configAdmin, batchIngester, ruleAdmin, dlqService)
		g.Go(func() error { return httpSrv.Run(ctx) })

		grpcSrv, err := server.NewGRPC(cfg.Server.GRPC, log, checks)
		if err != nil {
			return err
		}
		g.Go(func() error { return grpcSrv.Run(ctx) })
	}

	if cfg.Role.RunsWorker() {
		w := worker.New(durableQ, ingester, log, worker.Options{Tickets: ticketUpdater(ticketSvc), Metrics: met, QueueName: "durable"})
		g.Go(func() error { return w.Run(ctx) })

		if fastQ != nil {
			bw := worker.NewBatchWorker(fastQ, ingester, log, worker.BatchOptions{
				Size:      cfg.Queue.BatchSize,
				Linger:    time.Duration(cfg.Queue.BatchLingerMS) * time.Millisecond,
				Tickets:   ticketUpdater(ticketSvc),
				Metrics:   met,
				QueueName: "fast",
			})
			g.Go(func() error { return bw.Run(ctx) })
		}

		dispatchOpts := delivery.Options{
			Sink:    dlqRepo,
			Metrics: met,
		}
		if guard, err := buildSSRFTransport(cfg); err != nil {
			return err
		} else if guard != nil {
			dispatchOpts.Transport = guard
		}
		dispatcher := delivery.New(deliveryQ, cfg.Webhooks.Secrets, deliveryConfig(cfg), dispatchOpts, log)
		g.Go(func() error { return dispatcher.Run(ctx) })

		dlqSampler := buildDLQSampler(dlqRepo, tenantsFromSpecs(cfg.TenantSpecs()), met, log)
		g.Go(func() error { return dlqSampler.Run(ctx) })
	}

	return g.Wait()
}

// buildDeliveryQueue constructs the webhook delivery queue: a second stream on
// the same backend as ingest but under its own stream prefix so the two
// pipelines are isolated.
func buildDeliveryQueue(cfg *config.Config, rdb redis.UniversalClient) (queue.Queue, error) {
	shards := cfg.Webhooks.DeliveryShards
	if shards <= 0 {
		shards = cfg.Queue.Shards
	}
	if cfg.Queue.Driver == queue.DriverMemory {
		return queue.New(queue.Settings{Driver: queue.DriverMemory, Shards: shards}, nil)
	}
	return queue.New(queue.Settings{
		Driver:        cfg.Queue.Driver,
		Shards:        shards,
		StreamPrefix:  deliveryStreamPrefix(cfg.Queue.StreamPrefix),
		ConsumerGroup: cfg.Queue.ConsumerGroup,
	}, rdb)
}

// deliveryStreamPrefix derives the delivery stream prefix from the ingest one:
// ":ingest" suffix is replaced by ":delivery", or ":delivery" is appended.
func deliveryStreamPrefix(ingestPrefix string) string {
	return strings.TrimSuffix(ingestPrefix, ":ingest") + ":delivery"
}

func deliveryConfig(cfg *config.Config) delivery.Config {
	return delivery.Config{
		DefaultTimeout: time.Duration(cfg.Webhooks.DefaultTimeoutMS) * time.Millisecond,
		MaxAttempts:    cfg.Webhooks.MaxAttempts,
		Backoff: delivery.BackoffConfig{
			Base: time.Duration(cfg.Webhooks.Backoff.BaseMS) * time.Millisecond,
			Max:  time.Duration(cfg.Webhooks.Backoff.MaxMS) * time.Millisecond,
		},
	}
}

// buildQueues constructs the durable queue and, for role=all only, the in-memory
// fast queue. A split api/worker deployment has no fast queue, so fast degrades
// to durable in the pipeline.
func buildQueues(cfg *config.Config, rdb redis.UniversalClient) (durable, fast queue.Queue, err error) {
	durable, err = queue.New(queue.Settings{
		Driver:        cfg.Queue.Driver,
		Shards:        cfg.Queue.Shards,
		StreamPrefix:  cfg.Queue.StreamPrefix,
		ConsumerGroup: cfg.Queue.ConsumerGroup,
	}, rdb)
	if err != nil {
		return nil, nil, err
	}
	if cfg.Role == config.RoleAll {
		fast, err = queue.New(queue.Settings{Driver: queue.DriverMemory, Shards: cfg.Queue.Shards}, nil)
		if err != nil {
			return nil, nil, err
		}
	}
	return durable, fast, nil
}

// ticketUpdater adapts the optional ticket service to the worker port, returning
// a nil interface (not a typed nil) when ticketing is disabled so the worker
// skips it rather than calling into a nil pointer.
func ticketUpdater(svc *service.TicketService) worker.TicketUpdater {
	if svc == nil {
		return nil
	}
	return svc
}

// ruleCacheInvalidator adapts the optional Redis pub/sub publisher to the
// service port, returning a nil interface (not a typed nil) when Redis is absent
// so the RuleService skips invalidation rather than calling into a nil pointer.
func ruleCacheInvalidator(inv *redisstore.RuleInvalidator) service.RuleCacheInvalidator {
	if inv == nil {
		return nil
	}
	return inv
}

// idempForAsync adapts the optional Redis dedup store to the service port,
// returning a nil interface (not a typed nil) when Redis is absent so the
// AsyncIngester skips the receipt barrier rather than dereferencing nil.
func idempForAsync(store *redisstore.IdempotencyStore) domain.IdempotencyStore {
	if store == nil {
		return nil
	}
	return store
}

const defaultRetryAfter = 5 * time.Second

// buildSSRFTransport constructs a guarded HTTP transport from the webhooks.ssrf
// config block. The guard always runs (strict defaults block private/loopback
// even with no explicit overrides), closing the SSRF window for deployments
// that serve untrusted rule URLs.
func buildSSRFTransport(cfg *config.Config) (*delivery.GuardedTransport, error) {
	s := cfg.Webhooks.SSRF
	policy := &delivery.SSRFPolicy{
		Allow:        s.Allow,
		Deny:         s.Deny,
		AllowPrivate: s.AllowPrivate,
		AllowHTTP:    s.AllowHTTP,
	}
	return delivery.NewGuardedTransport(policy)
}

// buildDLQSampler returns a poller that periodically counts the DLQ and updates
// the webhook_dlq_size gauge. The count is cheap (index scan on _dlq).
func buildDLQSampler(repo domain.DLQRepository, tenants []tenant.Tenant, met *metrics.Metrics, log *slog.Logger) *metrics.DLQSampler {
	return metrics.NewDLQSampler(repo, tenants, met, log)
}

// tenantsFromSpecs strips specs down to what the DLQ sampler needs to build
// a tenant context per tenant (id + optional per-tenant database override) —
// it has no business seeing key material.
func tenantsFromSpecs(specs []tenant.Spec) []tenant.Tenant {
	tenants := make([]tenant.Tenant, 0, len(specs))
	for _, s := range specs {
		tenants = append(tenants, tenant.Tenant{
			ID:       s.ID,
			Database: tenant.Database{URI: s.DBURI, Name: s.DBName},
		})
	}
	return tenants
}

// buildSampler wires the depth sampler over the queues this process holds.
// Fast is mapped to its own queue when present, else degraded onto durable —
// matching the pipeline so backpressure gates the queue the write actually lands on.
func buildSampler(cfg *config.Config, met *metrics.Metrics, log *slog.Logger, durableQ, fastQ queue.Queue) *metrics.Sampler {
	retryAfter := time.Duration(cfg.Queue.RetryAfterSeconds) * time.Second
	if retryAfter <= 0 {
		retryAfter = defaultRetryAfter
	}
	maxDepth := int64(cfg.Queue.MaxDepth)
	s := metrics.NewSampler(met, log, 0)
	if obs, ok := durableQ.(queue.Observable); ok {
		s.Add(domain.ReliabilityDurable, "durable", obs, maxDepth, retryAfter)
	}
	// Fast routes to its own queue when one exists, else degrades onto durable —
	// match the pipeline so backpressure gates the queue the write actually lands on.
	fastObs, ok := fastQ.(queue.Observable)
	if !ok {
		fastObs, ok = durableQ.(queue.Observable)
	}
	if ok {
		s.Add(domain.ReliabilityFast, "fast", fastObs, maxDepth, retryAfter)
	}
	return s
}
