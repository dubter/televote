package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/dubter/televote/internal/platform/config"
	"github.com/dubter/televote/internal/platform/health"
	"github.com/dubter/televote/internal/platform/metrics"
	"github.com/dubter/televote/internal/platform/observability"
	"github.com/dubter/televote/internal/transport/httpapi"
)

const (
	telemetryFlushTimeout = 5 * time.Second
	shortRevisionLen      = 12
)

type runtime struct {
	cfg       *config.Config
	log       *slog.Logger
	metrics   *metrics.Metrics
	telemetry *observability.Telemetry
	gate      *health.Gate
}

type worker func(context.Context)

func boot(ctx context.Context) (*runtime, error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	telemetry, err := observability.Setup(ctx, observability.Config{
		Endpoint:    cfg.OTLPEndpoint,
		ServiceName: cfg.OTelServiceName,
		Version:     buildVersion(),
		Env:         cfg.Env,
		SampleRatio: cfg.TraceSampleRatio,
	}, stdoutHandler(cfg))
	if err != nil {
		return nil, fmt.Errorf("telemetry: %w", err)
	}

	logger := newLogger(cfg, telemetry.Logs)
	slog.SetDefault(logger)

	logger.Info("starting",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.Int("redis_nodes", len(cfg.RedisAddrs)),
		slog.String("dedup_ttl_lower_bound", cfg.DedupTTLLowerBound().String()),
		slog.String("drain_budget", cfg.DrainBudget().String()),
	)
	if cfg.UsesInsecureDefaults() {
		logger.Warn("default secrets from .env.example are in use: suitable only for a test stand")
	}

	return &runtime{
		cfg:       cfg,
		log:       logger,
		metrics:   metrics.New(prometheus.DefaultRegisterer),
		telemetry: telemetry,
		gate:      health.NewGate(),
	}, nil
}

func (rt *runtime) flush(ctx context.Context) {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), telemetryFlushTimeout)
	defer cancel()
	if err := rt.telemetry.Shutdown(flushCtx); err != nil {
		rt.log.Warn("telemetry was not fully flushed", slog.Any("error", err))
	}
}

func (rt *runtime) probes(readiness ...health.Checker) *health.Probes {
	return health.New(nil, append(readiness, rt.gate.Checker()))
}

func (rt *runtime) serve(ctx context.Context, handler http.Handler, workers ...worker) error {
	cfg, logger := rt.cfg, rt.log

	srv := newServer(ctx, cfg, logger, cfg.HTTPAddr, handler)

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- listen(srv) }()

	var debugSrv *http.Server
	if cfg.DebugEnabled() {
		debugSrv = newServer(ctx, cfg, logger, cfg.DebugAddr, httpapi.DebugRouter())
		logger.Info("pprof started", slog.String("addr", cfg.DebugAddr))
		go func() {
			if err := listen(debugSrv); err != nil {
				logger.Error("pprof server stopped", slog.Any("error", err))
			}
		}()
	}

	var background sync.WaitGroup
	for _, w := range workers {
		background.Go(func() { w(sigCtx) })
	}

	rt.gate.SetReady(true)
	logger.Info("ready to accept traffic")

	select {
	case err := <-serveErr:
		return err
	case <-sigCtx.Done():
	}

	stop()

	logger.Info("shutdown signal received, dropping readiness", slog.String("grace", cfg.ShutdownGrace.String()))
	rt.gate.SetReady(false)

	shutdownCtx, cancel := context.WithTimeout(ctx, cfg.ShutdownGrace)
	defer cancel()

	var shutdownErrs []error
	if debugSrv != nil {
		if err := debugSrv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("pprof server did not stop cleanly", slog.Any("error", err))
		}
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("graceful shutdown did not fit into %s: %w", cfg.ShutdownGrace, err))
	}
	if err := <-serveErr; err != nil {
		shutdownErrs = append(shutdownErrs, err)
	}
	waitBackground(&background, cfg.ShutdownGrace, logger)

	if err := errors.Join(shutdownErrs...); err != nil {
		return err
	}
	logger.Info("stopped cleanly")
	return nil
}

func waitBackground(wg *sync.WaitGroup, timeout time.Duration, logger *slog.Logger) {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		logger.Warn("background workers did not stop in time", slog.String("timeout", timeout.String()))
	}
}

func newServer(ctx context.Context, cfg *config.Config, logger *slog.Logger, addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
		MaxHeaderBytes:    http.DefaultMaxHeaderBytes,
		BaseContext:       func(net.Listener) context.Context { return ctx },
		ErrorLog:          slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
}

func listen(srv *http.Server) error {
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("listener %s: %w", srv.Addr, err)
	}
	return nil
}

func stdoutHandler(cfg *config.Config) slog.Handler {
	return slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)})
}

func newLogger(cfg *config.Config, handler slog.Handler) *slog.Logger {
	return slog.New(handler).With(
		slog.String("service", cfg.OTelServiceName),
		slog.String("env", cfg.Env),
		slog.String("version", buildVersion()),
	)
}

func buildVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}

	var revision, modified string
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			revision = s.Value
		case "vcs.modified":
			modified = s.Value
		}
	}
	if revision == "" {
		return info.Main.Version
	}
	if len(revision) > shortRevisionLen {
		revision = revision[:shortRevisionLen]
	}
	if modified == "true" {
		return revision + "-dirty"
	}
	return revision
}

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}
