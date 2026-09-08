package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/pollcfg"
	"github.com/dubter/televote/internal/producer"
	"github.com/dubter/televote/internal/vote"
)

type Applier interface {
	Cast(ctx context.Context, pollID uuid.UUID, shardCount uint16, v vote.VoterID, choices []uint8) (vote.Result, error)
}

type ConfigLookup interface {
	ByID(id uuid.UUID) (*pollcfg.HotConfig, bool)
}

type Observer interface {
	VoteCounted(ctx context.Context, result vote.Result)
	VoteRejected(ctx context.Context, reason string)
	ApplySeconds(d float64)
}

const (
	reasonMalformed   = "malformed"
	reasonUnknownPoll = "unknown_poll"
	reasonOutOfWindow = "out_of_window"
	reasonBadVoterID  = "bad_voter_id"
)

type Counting struct {
	client  *kgo.Client
	applier Applier
	lookup  ConfigLookup
	obs     Observer
	log     *slog.Logger

	retryBudget time.Duration

	lookupBudget time.Duration
}

func NewCounting(client *kgo.Client, applier Applier, lookup ConfigLookup, obs Observer, log *slog.Logger) (*Counting, error) {
	switch {
	case client == nil:
		return nil, errors.New("consumer: не задан клиент Kafka")
	case applier == nil:
		return nil, errors.New("consumer: не задан applier")
	case lookup == nil:
		return nil, errors.New("consumer: не задан источник конфигов")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Counting{
		client:       client,
		applier:      applier,
		lookup:       lookup,
		obs:          obs,
		log:          log,
		retryBudget:  30 * time.Second,
		lookupBudget: 10 * time.Second,
	}, nil
}

func (c *Counting) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil //nolint:nilerr
				}
				c.log.ErrorContext(ctx, "consumer: чтение из Kafka",
					slog.String("topic", e.Topic), slog.String("error", e.Err.Error()))
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			c.applyRecord(ctx, rec)
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.WarnContext(ctx, "consumer: коммит оффсетов не удался",
				slog.String("error", err.Error()))
		}
	}
	return nil
}

func (c *Counting) applyRecord(ctx context.Context, rec *kgo.Record) {
	var msg producer.VoteMessage
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		c.reject(ctx, reasonMalformed, err)
		return
	}

	cfg, ok := c.awaitConfig(ctx, msg.PollID)
	if !ok {
		c.reject(ctx, reasonUnknownPoll, fmt.Errorf("опрос %s не найден", msg.PollID))
		return
	}

	if !cfg.Window.IsOpenAt(msg.ProducedAt) {
		c.reject(ctx, reasonOutOfWindow, fmt.Errorf("голос вне окна: %s", msg.ProducedAt))
		return
	}

	voterID, err := vote.ParseVoterID(msg.VoterID)
	if err != nil {
		c.reject(ctx, reasonBadVoterID, err)
		return
	}

	start := time.Now()
	res, err := c.applyWithRetry(ctx, cfg, voterID, msg.Choices)
	if c.obs != nil {
		c.obs.ApplySeconds(time.Since(start).Seconds())
	}
	if err != nil {
		c.log.ErrorContext(ctx, "consumer: голос не применён",
			slog.String("poll", msg.PollID.String()), slog.String("error", err.Error()))
		return
	}
	if c.obs != nil {
		c.obs.VoteCounted(ctx, res)
	}
}

func (c *Counting) awaitConfig(ctx context.Context, pollID uuid.UUID) (*pollcfg.HotConfig, bool) {
	if cfg, ok := c.lookup.ByID(pollID); ok {
		return cfg, true
	}

	deadline := time.Now().Add(c.lookupBudget)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(250 * time.Millisecond):
		}
		if cfg, ok := c.lookup.ByID(pollID); ok {
			return cfg, true
		}
	}
	return nil, false
}

func (c *Counting) applyWithRetry(
	ctx context.Context,
	cfg *pollcfg.HotConfig,
	voterID vote.VoterID,
	choices []uint8,
) (vote.Result, error) {
	deadline := time.Now().Add(c.retryBudget)
	backoff := 20 * time.Millisecond

	for {
		res, err := c.applier.Cast(ctx, cfg.ID, cfg.ShardCount, voterID, choices)
		if err == nil {
			return res, nil
		}
		if !vote.IsRetryable(err) || time.Now().After(deadline) {
			return 0, err
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

func (c *Counting) reject(ctx context.Context, reason string, err error) {
	if c.obs != nil {
		c.obs.VoteRejected(ctx, reason)
	}
	c.log.WarnContext(ctx, "consumer: сообщение отвергнуто",
		slog.String("reason", reason), slog.String("error", err.Error()))
}
