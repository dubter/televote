package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dubter/televote/internal/adapter/producer"
	"github.com/dubter/televote/internal/platform/observability"
	"github.com/dubter/televote/internal/service/voting"
	"github.com/dubter/televote/internal/transport/httpapi"
)

func RunAPI(ctx context.Context) error {
	rt, err := boot(ctx)
	if err != nil {
		return err
	}
	defer rt.flush(ctx)

	cfg := rt.cfg

	pgRead, err := rt.openPostgres(ctx, cfg.PostgresReadDSN)
	if err != nil {
		return fmt.Errorf("postgres (read): %w", err)
	}
	defer pgRead.Close()

	pgWrite, err := rt.openPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("postgres (write): %w", err)
	}
	defer pgWrite.Close()

	sink, err := producer.New(producer.Config{
		Brokers:        cfg.KafkaBrokers,
		Topic:          cfg.KafkaTopic,
		Linger:         cfg.KafkaLinger,
		ProduceTimeout: cfg.KafkaProduceTimeout,
		Hooks:          observability.KafkaHooks(cfg.KafkaConsumerGroup),
	})
	if err != nil {
		return fmt.Errorf("kafka producer: %w", err)
	}
	defer rt.drain("kafka producer", sink.Close)

	cache, err := rt.warmPollCache(ctx, pgRead)
	if err != nil {
		return err
	}

	accept, err := voting.New(cache, sink, rt.metrics, time.Now)
	if err != nil {
		return fmt.Errorf("voting: %w", err)
	}

	public, err := httpapi.NewPublicHandler(accept, cache, time.Now, cfg.MaxBodyBytes)
	if err != nil {
		return fmt.Errorf("public handler: %w", err)
	}

	admin, err := rt.buildAdmin(ctx, pgWrite)
	if err != nil {
		return err
	}

	router := httpapi.APIRouter(rt.probes(sink.Ping), public, admin, httpapi.NewPages(cfg.PublicBaseURL), httpapi.RouterConfig{
		TrustedProxies:  cfg.TrustedProxies,
		VoteRateLimit:   cfg.RateLimitPerMin,
		AdminRateLimit:  adminRateLimit,
		Logger:          rt.log,
		RequestObserver: rt.metrics,
		RateWindow:      time.Minute,
		ServiceName:     cfg.OTelServiceName,
	})

	return rt.serve(ctx, router, cache.Run)
}

func (rt *runtime) drain(name string, closeFn func() error) {
	if err := closeFn(); err != nil {
		rt.log.Error(name+" was not released cleanly", slog.Any("error", err))
	}
}
