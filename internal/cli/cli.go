// Package cli parses arguments, loads configuration and hands control
// to app.Run. It exists so that cmd/letopis stays a few lines and other
// distributions can reuse the exact same startup behaviour.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/max-trifonov/letopis/internal/app"
	"github.com/max-trifonov/letopis/internal/config"
	"github.com/max-trifonov/letopis/internal/version"
	"github.com/max-trifonov/letopis/pkg/ext"
)

const usage = `Usage: letopis <command> [flags]

Commands:
  serve     run the service (see "letopis serve -h")
  version   print build information
`

func Main(args []string, reg *ext.Registry) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, usage)
		return errors.New("command required")
	}
	switch args[0] {
	case "serve":
		return serve(args[1:], reg)
	case "version":
		fmt.Printf("letopis %s (commit %s, built %s)\n", version.Version, version.Commit, version.Date)
		return nil
	default:
		fmt.Fprint(os.Stderr, usage)
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func serve(args []string, reg *ext.Registry) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to config file (default: ./config.yaml, then next to the binary)")
	rolef := fs.String("role", "", "override role from config: api | worker | all")
	if err := fs.Parse(args); err != nil {
		return err
	}

	path, err := config.Resolve(*cfgPath)
	if err != nil {
		return err
	}
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	if *rolef != "" {
		cfg.Role = config.Role(*rolef)
		if err := cfg.Validate(); err != nil {
			return err
		}
	}

	log := newLogger(os.Stderr, cfg.Log)
	log.Info("config loaded", "path", path)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err = app.Run(ctx, cfg, log, reg)
	if errors.Is(err, context.Canceled) {
		// Normal shutdown via signal.
		return nil
	}
	return err
}

func newLogger(w io.Writer, cfg config.Log) *slog.Logger {
	var level slog.Level
	// Already validated by config.Load; default branch is unreachable.
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	if cfg.Format == "text" {
		return slog.New(slog.NewTextHandler(w, opts))
	}
	return slog.New(slog.NewJSONHandler(w, opts))
}
