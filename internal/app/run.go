package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof" //nolint:gosec // G108: pprof вешается на отдельный mux и отдельный слушатель, DefaultServeMux наружу не выставлен
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dubter/televote/internal/config"
	"github.com/dubter/televote/pkg/health"
)

var Version = "dev"

func Run(ctx context.Context, r Role) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("конфигурация: %w", err)
	}

	logger := newLogger(cfg)
	slog.SetDefault(logger)

	logger.Info("старт",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.Int("redis_nodes", len(cfg.RedisAddrs)),
		slog.String("dedup_ttl_lower_bound", cfg.DedupTTLLowerBound().String()),
		slog.String("drain_budget", cfg.DrainBudget().String()),
	)
	if cfg.UsesInsecureDefaults() {
		logger.Warn("используются дефолтные секреты из .env.example: годится только для стенда")
	}

	gate := health.NewGate()

	application, err := buildApp(ctx, cfg, logger, r)
	if err != nil {
		return fmt.Errorf("сборка приложения: %w", err)
	}
	defer application.Close() //nolint:contextcheck // дренаж по собственному сроку

	healthHandler := health.Handler(nil, append(application.readiness(), gate.Checker()))

	mux := http.NewServeMux()
	mux.Handle("/livez", healthHandler)
	mux.Handle("/readyz", healthHandler)
	mux.Handle("/metrics", promhttp.Handler())
	if application.advisor != nil {
		mux.Handle("/internal/capacity", application.advisor.Handler())
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
		logger.Info("pprof поднят", slog.String("addr", cfg.DebugAddr))
		go func() {
			if err := listen(debugSrv); err != nil {
				logger.Error("pprof-сервер остановлен", slog.Any("error", err))
			}
		}()
	}

	application.runBackground(sigCtx)

	gate.SetReady(true)
	logger.Info("готов принимать трафик")

	select {
	case err := <-serveErr:
		return err
	case <-sigCtx.Done():
	}

	stop()

	logger.Info("получен сигнал остановки, снимаем готовность", slog.String("grace", cfg.ShutdownGrace.String()))
	gate.SetReady(false)

	shutdownCtx, cancel := context.WithTimeout(ctx, cfg.ShutdownGrace)
	defer cancel()

	var shutdownErrs []error
	if debugSrv != nil {
		if err := debugSrv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("pprof-сервер не остановился штатно", slog.Any("error", err))
		}
	}
	if err := srv.Shutdown(shutdownCtx); err != nil {
		shutdownErrs = append(shutdownErrs, fmt.Errorf("graceful shutdown не уложился в %s: %w", cfg.ShutdownGrace, err))
	}
	if err := <-serveErr; err != nil {
		shutdownErrs = append(shutdownErrs, err)
	}

	if err := errors.Join(shutdownErrs...); err != nil {
		return err
	}
	logger.Info("остановлен штатно")
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
		return fmt.Errorf("слушатель %s: %w", srv.Addr, err)
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

func newLogger(cfg *config.Config) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLevel(cfg.LogLevel)})
	return slog.New(handler).With(
		slog.String("service", cfg.OTelServiceName),
		slog.String("env", cfg.Env),
		slog.String("version", Version),
	)
}

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo // значение уже проверено конфигом, это страховка
	}
	return l
}
