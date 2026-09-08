package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof" //nolint:gosec // G108: pprof runs on its own mux and listener, DefaultServeMux is not exposed
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dubter/televote/internal/adapter/httpapi"
	"github.com/dubter/televote/internal/platform/config"
	"github.com/dubter/televote/internal/platform/health"
	"github.com/dubter/televote/internal/platform/observability"
)

func Run(ctx context.Context, r Role) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	telemetry, err := observability.Setup(ctx, observability.Config{
		Endpoint:    cfg.OTLPEndpoint,
		ServiceName: cfg.OTelServiceName,
		Version:     buildVersion(),
		Env:         cfg.Env,
		SampleRatio: cfg.TraceSampleRatio,
	}, stdoutHandler(cfg))
	if err != nil {
		return fmt.Errorf("telemetry: %w", err)
	}

	logger := newLogger(cfg, telemetry.Logs)
	slog.SetDefault(logger)
	defer flushTelemetry(ctx, telemetry, logger)

	logger.Info("starting",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.Int("redis_nodes", len(cfg.RedisAddrs)),
		slog.String("dedup_ttl_lower_bound", cfg.DedupTTLLowerBound().String()),
		slog.String("drain_budget", cfg.DrainBudget().String()),
	)
	if cfg.UsesInsecureDefaults() {
		logger.Warn("default secrets from .env.example are in use: suitable only for a test stand")
	}

	gate := health.NewGate()

	application, err := buildApp(ctx, cfg, logger, r)
	if err != nil {
		return fmt.Errorf("build application: %w", err)
	}
	defer application.Close() //nolint:contextcheck // drains on its own deadline

	healthHandler := health.Handler(nil, append(application.readiness(), gate.Checker()))

	mux := http.NewServeMux()
	mux.Handle("/livez", healthHandler)
	mux.Handle("/readyz", healthHandler)
	mux.Handle("/metrics", promhttp.Handler())
	if application.advisor != nil {
		mux.Handle("/internal/capacity", httpapi.CapacityHandler(application.advisor))
	}
	if application.router != nil {
		mux.Handle("/", application.router)
	}

	srv := newServer(ctx, cfg, logger, cfg.HTTPAddr, mux)

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() { serveErr <- listen(srv) }()

	var debugSrv *http.Server
	if cfg.DebugEnabled() {
		debugSrv = newServer(ctx, cfg, logger, cfg.DebugAddr, debugMux())
		logger.Info("pprof started", slog.String("addr", cfg.DebugAddr))
		go func() {
			if err := listen(debugSrv); err != nil {
				logger.Error("pprof server stopped", slog.Any("error", err))
			}
		}()
	}

	application.runBackground(sigCtx)

	gate.SetReady(true)
	logger.Info("ready to accept traffic")

	select {
	case err := <-serveErr:
		return err
	case <-sigCtx.Done():
	}

	stop()

	logger.Info("shutdown signal received, dropping readiness", slog.String("grace", cfg.ShutdownGrace.String()))
	gate.SetReady(false)

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
	application.waitBackground(cfg.ShutdownGrace)

	if err := errors.Join(shutdownErrs...); err != nil {
		return err
	}
	logger.Info("stopped cleanly")
	return nil
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

func debugMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func stdoutHandler(cfg *config.Config) slog.Handler {
	return slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)})
}

func flushTelemetry(ctx context.Context, t *observability.Telemetry, logger *slog.Logger) {
	flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), telemetryFlushTimeout)
	defer cancel()
	if err := t.Shutdown(flushCtx); err != nil {
		logger.Warn("telemetry was not fully flushed", slog.Any("error", err))
	}
}

const telemetryFlushTimeout = 5 * time.Second

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

const shortRevisionLen = 12

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo
	}
	return l
}
