package app

import (
	"context"
	"fmt"

	"github.com/dubter/televote/internal/adapter/postgres"
	"github.com/dubter/televote/internal/service/capacity"
	"github.com/dubter/televote/internal/service/snapshot"
	"github.com/dubter/televote/internal/transport/httpapi"
)

func RunSnapshot(ctx context.Context) error {
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

	store, err := rt.openRedis(ctx)
	if err != nil {
		return err
	}
	defer store.Close()

	kafka, err := rt.openKafka()
	if err != nil {
		return err
	}
	defer kafka.Close()

	tally, err := rt.newTally(store)
	if err != nil {
		return err
	}

	pollsRead, err := postgres.NewPollRepo(pgRead)
	if err != nil {
		return fmt.Errorf("poll repository (read): %w", err)
	}
	pollsWrite, err := postgres.NewPollRepo(pgWrite)
	if err != nil {
		return fmt.Errorf("poll repository (write): %w", err)
	}
	results, err := postgres.NewResultRepo(pgWrite)
	if err != nil {
		return fmt.Errorf("result repository: %w", err)
	}

	lag := kafkaLag{kafka, cfg.KafkaTopic}

	snapshotter, err := snapshot.New(tally, results, pollsWrite, lag, snapshot.Config{
		Interval: cfg.SnapshotInterval,
		Grace:    cfg.SnapshotFinalGrace,
		Log:      rt.log,
		Observer: rt.metrics,
	})
	if err != nil {
		return fmt.Errorf("snapshotter: %w", err)
	}

	advisor, err := capacity.New(pollsRead, lag, capacity.Config{
		DrainWindow: cfg.DrainWindow,
		PrewarmLead: cfg.PollMinLeadTime,
	})
	if err != nil {
		return fmt.Errorf("capacity advisor: %w", err)
	}

	router := httpapi.SnapshotRouter(rt.probes(pgRead.Ping, store.Ping, kafka.Ping), advisor)

	return rt.serve(ctx, router, snapshotter.Run)
}
