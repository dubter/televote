package kafka

//go:generate mockgen -source=consumer.go -destination=mocks/consumer.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"

	"github.com/dubter/televote/internal/domain"
)

type Counting interface {
	Count(ctx context.Context, msg domain.VoteMessage) (domain.VoteResult, error)
}

type Observer interface {
	VoteRejected(reason string)
}

const (
	reasonMalformed   = "malformed"
	defaultWorkers    = 64
	fetchErrorBackoff = 250 * time.Millisecond
)

type Config struct {
	Workers int
}

type Consumer struct {
	client   *kgo.Client
	counting Counting
	obs      Observer
	log      *slog.Logger
	workers  int
}

func NewConsumer(client *kgo.Client, counting Counting, obs Observer, log *slog.Logger, cfg Config) (*Consumer, error) {
	switch {
	case client == nil:
		return nil, errors.New("kafka: client is required")
	case counting == nil:
		return nil, errors.New("kafka: counting use case is required")
	case obs == nil:
		return nil, errors.New("kafka: observer is required")
	case log == nil:
		return nil, errors.New("kafka: logger is required")
	}
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}
	return &Consumer{client: client, counting: counting, obs: obs, log: log, workers: cfg.Workers}, nil
}

func (c *Consumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return
				}
				c.log.ErrorContext(ctx, "kafka: fetch failed",
					slog.String("topic", e.Topic), slog.String("error", e.Err.Error()))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(fetchErrorBackoff):
			}
			continue
		}

		c.handleBatch(ctx, fetches)

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.WarnContext(ctx, "kafka: offset commit failed", slog.String("error", err.Error()))
		}
	}
}

func (c *Consumer) handleBatch(ctx context.Context, fetches kgo.Fetches) {
	var (
		wg   sync.WaitGroup
		slot = make(chan struct{}, c.workers)
	)

	fetches.EachRecord(func(rec *kgo.Record) {
		select {
		case slot <- struct{}{}:
		case <-ctx.Done():
			return
		}

		wg.Go(func() {
			defer func() { <-slot }()
			c.handle(ctx, rec)
		})
	})
	wg.Wait()
}

func (c *Consumer) handle(ctx context.Context, rec *kgo.Record) {
	if rec.Context != nil {
		if parent := trace.SpanContextFromContext(rec.Context); parent.IsValid() {
			ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
		}
	}

	var msg domain.VoteMessage
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		c.obs.VoteRejected(reasonMalformed)
		c.log.WarnContext(ctx, "kafka: message rejected",
			slog.String("reason", reasonMalformed),
			slog.Int64("partition", int64(rec.Partition)), slog.Int64("offset", rec.Offset),
			slog.String("error", err.Error()))
		return
	}

	if _, err := c.counting.Count(ctx, msg); err != nil {
		c.log.WarnContext(ctx, "kafka: vote rejected",
			slog.Int64("partition", int64(rec.Partition)), slog.Int64("offset", rec.Offset),
			slog.String("error", err.Error()))
	}
}
