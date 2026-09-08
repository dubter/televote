package consumer

//go:generate mockgen -source=counting.go -destination=mocks/counting.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/dubter/televote/internal/adapter/producer"
	"github.com/dubter/televote/internal/service/pollcfg"
	"github.com/dubter/televote/internal/service/vote"
)

type Applier interface {
	Cast(ctx context.Context, pollID uuid.UUID, shardCount uint16, v vote.VoterID, choices []uint8) (vote.Result, error)
}

type ConfigLookup interface {
	ByID(id uuid.UUID) (*pollcfg.HotConfig, bool)
}

type Observer interface {
	VoteCounted(result vote.Result)
	VoteRejected(reason string)
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

	retryBudget  time.Duration
	lookupBudget time.Duration
	breaker      *gobreaker.CircuitBreaker[vote.Result]
	tracer       trace.Tracer
}

type Config struct {
	RetryBudget   time.Duration
	LookupBudget  time.Duration
	ErrorRatio    float64
	BreakerWindow time.Duration
}

func NewCounting(
	client *kgo.Client,
	applier Applier,
	lookup ConfigLookup,
	obs Observer,
	log *slog.Logger,
	cfg Config,
) (*Counting, error) {
	switch {
	case client == nil:
		return nil, errors.New("consumer: kafka client is required")
	case applier == nil:
		return nil, errors.New("consumer: applier is required")
	case lookup == nil:
		return nil, errors.New("consumer: config source is required")
	}
	c := newCounting(applier, lookup, obs, log, cfg)
	c.client = client
	return c, nil
}

func newCounting(applier Applier, lookup ConfigLookup, obs Observer, log *slog.Logger, cfg Config) *Counting {
	if log == nil {
		log = slog.Default()
	}
	if cfg.RetryBudget <= 0 {
		cfg.RetryBudget = defaultRetryBudget
	}
	if cfg.LookupBudget <= 0 {
		cfg.LookupBudget = defaultLookupBudget
	}
	if cfg.ErrorRatio <= 0 || cfg.ErrorRatio > 1 {
		cfg.ErrorRatio = defaultErrorRatio
	}
	if cfg.BreakerWindow <= 0 {
		cfg.BreakerWindow = defaultBreakerWindow
	}

	return &Counting{
		applier:      applier,
		lookup:       lookup,
		obs:          obs,
		log:          log,
		retryBudget:  cfg.RetryBudget,
		lookupBudget: cfg.LookupBudget,
		breaker:      newBreaker(cfg, log),
		tracer:       otel.Tracer("televote/consumer"),
	}
}

const (
	defaultRetryBudget   = 30 * time.Second
	defaultLookupBudget  = 10 * time.Second
	defaultErrorRatio    = 0.5
	defaultBreakerWindow = 5 * time.Second
	breakerMinRequests   = 20
)

func newBreaker(cfg Config, log *slog.Logger) *gobreaker.CircuitBreaker[vote.Result] {
	return gobreaker.NewCircuitBreaker[vote.Result](gobreaker.Settings{
		Name:     "redis-apply",
		Interval: cfg.BreakerWindow,
		Timeout:  cfg.BreakerWindow,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.Requests >= breakerMinRequests &&
				float64(c.TotalFailures)/float64(c.Requests) >= cfg.ErrorRatio
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Warn("consumer: breaker changed state",
				slog.String("breaker", name),
				slog.String("from", from.String()), slog.String("to", to.String()))
		},
	})
}

func (c *Counting) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil //nolint:nilerr
				}
				c.log.ErrorContext(ctx, "consumer: read from kafka",
					slog.String("topic", e.Topic), slog.String("error", e.Err.Error()))
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			c.applyRecord(ctx, rec)
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.WarnContext(ctx, "consumer: offset commit failed",
				slog.String("error", err.Error()))
		}
	}
	return nil
}

func (c *Counting) applyRecord(ctx context.Context, rec *kgo.Record) {
	if parent := remoteSpan(rec); parent.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	}
	ctx, span := c.tracer.Start(ctx, "vote.apply")
	defer span.End()

	var msg producer.VoteMessage
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		c.reject(ctx, reasonMalformed, err)
		return
	}

	cfg, ok := c.awaitConfig(ctx, msg.PollID)
	if !ok {
		c.reject(ctx, reasonUnknownPoll, fmt.Errorf("poll %s not found", msg.PollID))
		return
	}

	if !cfg.Window.IsOpenAt(msg.ProducedAt) {
		c.reject(ctx, reasonOutOfWindow, fmt.Errorf("vote out of window: %s", msg.ProducedAt))
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
		c.log.ErrorContext(ctx, "consumer: vote not applied",
			slog.String("poll", msg.PollID.String()), slog.String("error", err.Error()))
		return
	}
	if c.obs != nil {
		c.obs.VoteCounted(res)
	}
}

func (c *Counting) retryable(err error) bool {
	return vote.IsRetryable(err) ||
		errors.Is(err, gobreaker.ErrOpenState) ||
		errors.Is(err, gobreaker.ErrTooManyRequests)
}

func remoteSpan(rec *kgo.Record) trace.SpanContext {
	if rec.Context == nil {
		return trace.SpanContext{}
	}
	return trace.SpanContextFromContext(rec.Context)
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
		res, err := c.breaker.Execute(func() (vote.Result, error) {
			return c.applier.Cast(ctx, cfg.ID, cfg.ShardCount, voterID, choices)
		})
		if err == nil {
			return res, nil
		}
		if !c.retryable(err) || time.Now().After(deadline) {
			return 0, fmt.Errorf("consumer: apply vote of poll %s: %w", cfg.ID, err)
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
		c.obs.VoteRejected(reason)
	}
	c.log.WarnContext(ctx, "consumer: message rejected",
		slog.String("reason", reason), slog.String("error", err.Error()))
}
