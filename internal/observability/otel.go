package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

type Config struct {
	Endpoint    string
	ServiceName string
	Version     string
	Env         string
	SampleRatio float64
}

type Telemetry struct {
	Logs     slog.Handler
	shutdown []func(context.Context) error
}

func Setup(ctx context.Context, cfg Config, stdout slog.Handler) (*Telemetry, error) {
	t := &Telemetry{Logs: withTrace(stdout)}
	if cfg.Endpoint == "" {
		return t, nil
	}

	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceName(cfg.ServiceName),
		semconv.ServiceVersion(cfg.Version),
		semconv.DeploymentEnvironmentNameKey.String(cfg.Env),
	))
	if err != nil {
		return nil, fmt.Errorf("телеметрия: описание сервиса: %w", err)
	}

	traces, err := otlptracegrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("телеметрия: экспортёр трейсов: %w", err)
	}
	tracers := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traces),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(cfg.SampleRatio))),
	)
	otel.SetTracerProvider(tracers)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	logs, err := otlploggrpc.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("телеметрия: экспортёр логов: %w", err)
	}
	loggers := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logs)),
		log.WithResource(res),
	)

	t.Logs = fanout{t.Logs, otelslog.NewHandler(cfg.ServiceName, otelslog.WithLoggerProvider(loggers))}
	t.shutdown = []func(context.Context) error{tracers.Shutdown, loggers.Shutdown}
	return t, nil
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	errs := make([]error, 0, len(t.shutdown))
	for _, fn := range t.shutdown {
		errs = append(errs, fn(ctx))
	}
	return errors.Join(errs...)
}
