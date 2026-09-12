package app

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/service/counting"
	"github.com/dubter/televote/internal/transport/httpapi"
	kafkaworker "github.com/dubter/televote/internal/worker/kafka"
)

func RunConsumer(ctx context.Context) error {
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

	store, err := rt.openRedis(ctx)
	if err != nil {
		return err
	}
	defer store.Close()

	kafka, err := rt.openKafka(kgo.ConsumeTopics(cfg.KafkaTopic))
	if err != nil {
		return err
	}
	defer kafka.Close()

	cache, err := rt.warmPollCache(ctx, pgRead)
	if err != nil {
		return err
	}

	tally, err := rt.newTally(store)
	if err != nil {
		return err
	}

	count, err := counting.New(tally, cache, rt.metrics, rt.log, counting.Config{
		RetryBudget:   cfg.VoteRetryBudget,
		LookupBudget:  cfg.PollConfigRefresh * lookupRefreshFactor,
		ErrorRatio:    cfg.BreakerErrorRatio,
		BreakerWindow: cfg.BreakerWindow,
	})
	if err != nil {
		return fmt.Errorf("counting: %w", err)
	}

	consumer, err := kafkaworker.NewConsumer(kafka, count, rt.metrics, rt.log, kafkaworker.Config{Workers: cfg.ConsumerWorkers})
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}

	router := httpapi.ConsumerRouter(rt.probes(pgRead.Ping, store.Ping, kafka.Ping))

	return rt.serve(ctx, router, cache.Run, consumer.Run)
}
