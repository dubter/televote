// Команда televote — HTTP-сервис анонимного голосования.
//
// Здесь только сборка: чтение конфига, логгер, health, HTTP-сервер с полным
// набором таймаутов и graceful shutdown. Бизнес-логика живёт в internal/.
package main

import (
	"context"
	"errors"
	"flag"
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

// version подставляется линкером: -ldflags "-X main.version=…".
var version = "dev"

// roleFlag выбирает, что делает процесс: в проде приём и консьюмеры
// масштабируются под разные графики нагрузки и живут в разных деплойментах.
var (
	roleFlag = flag.String("role", string(roleAll), "роль процесса: api | consumer | all")

	// healthFlag превращает бинарь в собственный healthcheck: образ
	// distroless, в нём нет ни shell, ни curl, и проверять готовность
	// контейнера больше нечем.
	healthFlag = flag.Bool("healthcheck", false, "проверить /readyz локального процесса и выйти")
)

func main() {
	flag.Parse()

	if *healthFlag {
		os.Exit(selfHealthcheck())
	}

	if err := run(context.Background()); err != nil {
		// Логгер к этому моменту может быть ещё не настроен — стандартного хватит,
		// важно, чтобы причина отказа старта дошла до stderr целиком.
		slog.Error("сервис остановлен с ошибкой", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("конфигурация: %w", err)
	}

	logger := newLogger(cfg)
	slog.SetDefault(logger)

	// Обе величины инварианта дедупликации печатаются при старте: нарушение
	// уже поймано конфигом, но дежурному нужно видеть запас, а не верить на слово.
	logger.Info("старт",
		slog.String("http_addr", cfg.HTTPAddr),
		slog.Int("redis_nodes", len(cfg.RedisAddrs)),
		slog.String("dedup_ttl_lower_bound", cfg.DedupTTLLowerBound().String()),
		slog.String("drain_budget", cfg.DrainBudget().String()),
	)
	if cfg.UsesInsecureDefaults() {
		// В production Load() до этого места не доходит — там это ошибка старта.
		logger.Warn("используются дефолтные секреты из .env.example: годится только для стенда")
	}

	// Готовность снимается до Shutdown, чтобы балансировщик увёл трафик раньше,
	// чем сервер начнёт закрывать соединения.
	gate := health.NewGate()

	application, err := buildApp(ctx, cfg, logger, role(*roleFlag))
	if err != nil {
		return fmt.Errorf("сборка приложения: %w", err)
	}
	defer application.Close() //nolint:contextcheck // дренаж по собственному сроку

	// Health обязан работать, даже когда всё остальное сломано, поэтому висит
	// на корневом mux до и независимо от прикладного роутера.
	healthHandler := health.Handler(nil, append(application.readiness(), gate.Checker()))

	mux := http.NewServeMux()
	mux.Handle("/livez", healthHandler)
	mux.Handle("/readyz", healthHandler)
	mux.Handle("/metrics", promhttp.Handler())
	if application.advisor != nil {
		// Не публичный маршрут: его читает external scaler KEDA.
		mux.Handle("/internal/capacity", application.advisor.Handler())
	}
	if application.router != nil {
		mux.Handle("/", application.router)
	}

	srv := newServer(ctx, cfg, logger, cfg.HTTPAddr, mux)

	// Сигнал отменяет отдельный контекст, а не корневой: запросы в полёте
	// обязаны дожить до конца, их дренирует Shutdown, а не отмена контекста.
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

	// Кэш конфигов прогрет внутри buildApp — до этой точки инстанс трафика
	// не получает.
	application.runBackground(sigCtx)

	gate.SetReady(true)
	logger.Info("готов принимать трафик")

	select {
	case err := <-serveErr:
		return err
	case <-sigCtx.Done():
	}

	// Второй сигнал вернёт поведение по умолчанию и убьёт процесс: зависший
	// shutdown не должен требовать SIGKILL вручную.
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

// newServer собирает http.Server со всеми таймаутами.
//
// ReadHeaderTimeout здесь не для галочки: без него соединение, отдающее
// заголовки по байту в минуту, живёт вечно, и тысяча таких соединений
// выбирает лимит файловых дескрипторов (Slowloris).
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

// debugMux регистрирует pprof на собственном mux: импорт net/http/pprof
// вешает обработчики на DefaultServeMux, и если отдавать наружу его,
// профили процесса окажутся публичными.
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
		slog.String("version", version),
	)
}

func parseLevel(s string) slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(s)); err != nil {
		return slog.LevelInfo // значение уже проверено конфигом, это страховка
	}
	return l
}
