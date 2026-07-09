package server

import (
	"context"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	healthsvc "google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/max-trifonov/letopis/internal/config"
	letopisv1 "github.com/max-trifonov/letopis/internal/gen/letopis/v1"
	"github.com/max-trifonov/letopis/internal/health"
	"github.com/max-trifonov/letopis/internal/version"
)

type GRPC struct {
	cfg    config.Listen
	log    *slog.Logger
	srv    *grpc.Server
	checks *health.Registry
	hsrv   *healthsvc.Server
}

func NewGRPC(cfg config.Listen, log *slog.Logger, checks *health.Registry) (*GRPC, error) {
	var opts []grpc.ServerOption
	if cfg.TLS.Enabled {
		creds, err := credentials.NewServerTLSFromFile(cfg.TLS.CertFile, cfg.TLS.KeyFile)
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.Creds(creds))
	}

	srv := grpc.NewServer(opts...)
	hsrv := healthsvc.NewServer()
	healthpb.RegisterHealthServer(srv, hsrv)
	letopisv1.RegisterSystemServiceServer(srv, systemService{})
	// Reflection makes grpcurl and friends work out of the box; the API
	// is not a secret, it's documented.
	reflection.Register(srv)

	return &GRPC{
		cfg:    cfg,
		log:    log.With("component", "grpc"),
		srv:    srv,
		checks: checks,
		hsrv:   hsrv,
	}, nil
}

func (g *GRPC) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", g.cfg.Addr)
	if err != nil {
		return err
	}
	g.log.Info("grpc listening", "addr", ln.Addr().String(), "tls", g.cfg.TLS.Enabled)

	stop := make(chan struct{})
	go g.watchReadiness(ctx, stop)

	errc := make(chan error, 1)
	go func() { errc <- g.srv.Serve(ln) }()

	select {
	case err := <-errc:
		close(stop)
		return err
	case <-ctx.Done():
	}
	close(stop)

	// GracefulStop waits for in-flight RPCs forever; cap it the same way
	// we cap HTTP draining.
	done := make(chan struct{})
	go func() {
		g.srv.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(shutdownTimeout):
		g.srv.Stop()
	}
	g.log.Info("grpc stopped")
	return nil
}

// watchReadiness feeds the aggregated checks into the standard gRPC
// health service, so both transports report the same readiness.
func (g *GRPC) watchReadiness(ctx context.Context, stop <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		status := healthpb.HealthCheckResponse_SERVING
		if len(g.checks.Run(ctx)) > 0 {
			status = healthpb.HealthCheckResponse_NOT_SERVING
		}
		g.hsrv.SetServingStatus("", status)
		select {
		case <-ticker.C:
		case <-stop:
			return
		}
	}
}

type systemService struct {
	letopisv1.UnimplementedSystemServiceServer
}

func (systemService) GetVersion(context.Context, *letopisv1.GetVersionRequest) (*letopisv1.GetVersionResponse, error) {
	return &letopisv1.GetVersionResponse{
		Version: version.Version,
		Commit:  version.Commit,
		Date:    version.Date,
	}, nil
}
