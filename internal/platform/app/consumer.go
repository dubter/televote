package app

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/service/consumer"
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

	counting, err := consumer.NewCounting(kafka, tally, cache, rt.metrics, rt.log, consumer.Config{
		RetryBudget:   cfg.VoteRetryBudget,
		LookupBudget:  cfg.PollConfigRefresh * lookupRefreshFactor,
		ErrorRatio:    cfg.BreakerErrorRatio,
		BreakerWindow: cfg.BreakerWindow,
		Workers:       cfg.ConsumerWorkers,
	})
	if err != nil {
		return fmt.Errorf("counting consumer: %w", err)
	}

	mux := rt.newMux(pgRead.Ping, store.Ping, kafka.Ping)

	return rt.serve(ctx, mux, cache.Run, rt.logged("counting consumer", counting.Run))
}
