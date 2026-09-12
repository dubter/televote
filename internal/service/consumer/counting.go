package consumer

//go:generate mockgen -source=counting.go -destination=mocks/counting.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/pollcfg"
)

type Applier interface {
	Apply(ctx context.Context, target domain.Sharding, v domain.VoterID, choices []uint8) (domain.VoteResult, error)
}

type ConfigLookup interface {
	ByID(id uuid.UUID) (*pollcfg.HotConfig, bool)
}

type Observer interface {
	VoteCounted(result string)
	VoteRejected(reason string)
	ApplySeconds(d float64)
	SetBreakerOpen(open bool)
}

const (
	reasonMalformed   = "malformed"
	reasonUnknownPoll = "unknown_poll"
	reasonOutOfWindow = "out_of_window"
	reasonBadVoterID  = "bad_voter_id"
	reasonApplyFailed = "apply_failed"
)

type Counting struct {
	client  *kgo.Client
	applier Applier
	lookup  ConfigLookup
	obs     Observer
	log     *slog.Logger

	retryBudget  time.Duration
	lookupBudget time.Duration
	breaker      *gobreaker.CircuitBreaker[domain.VoteResult]
	workers      int
	tracer       trace.Tracer
}

type Config struct {
	RetryBudget   time.Duration
	LookupBudget  time.Duration
	ErrorRatio    float64
	BreakerWindow time.Duration
	Workers       int
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
	if cfg.Workers <= 0 {
		cfg.Workers = defaultWorkers
	}

	return &Counting{
		applier:      applier,
		lookup:       lookup,
		obs:          obs,
		log:          log,
		retryBudget:  cfg.RetryBudget,
		lookupBudget: cfg.LookupBudget,
		breaker:      newBreaker(cfg, log, obs),
		workers:      cfg.Workers,
		tracer:       otel.Tracer("televote/consumer"),
	}
}

const (
	defaultRetryBudget   = 30 * time.Second
	defaultLookupBudget  = 10 * time.Second
	defaultErrorRatio    = 0.5
	defaultBreakerWindow = 5 * time.Second
	initialApplyBackoff  = 20 * time.Millisecond
	maxApplyBackoff      = 2 * time.Second
	fetchErrorBackoff    = 250 * time.Millisecond
	defaultWorkers       = 64
	breakerMinRequests   = 20
)

func newBreaker(cfg Config, log *slog.Logger, obs Observer) *gobreaker.CircuitBreaker[domain.VoteResult] {
	return gobreaker.NewCircuitBreaker[domain.VoteResult](gobreaker.Settings{
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
			if obs != nil {
				obs.SetBreakerOpen(to != gobreaker.StateClosed)
			}
		},
	})
}

func (c *Counting) Run(ctx context.Context) {
	for ctx.Err() == nil {
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return
				}
				c.log.ErrorContext(ctx, "consumer: read from kafka",
					slog.String("topic", e.Topic), slog.String("error", e.Err.Error()))
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(fetchErrorBackoff):
			}
			continue
		}

		c.applyBatch(ctx, fetches)

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.log.WarnContext(ctx, "consumer: offset commit failed",
				slog.String("error", err.Error()))
		}
	}
}

func (c *Counting) applyBatch(ctx context.Context, fetches kgo.Fetches) {
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

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-slot }()
			c.applyRecord(ctx, rec)
		}()
	})
	wg.Wait()
}

func (c *Counting) applyRecord(ctx context.Context, rec *kgo.Record) {
	if parent := remoteSpan(rec); parent.IsValid() {
		ctx = trace.ContextWithRemoteSpanContext(ctx, parent)
	}
	ctx, span := c.tracer.Start(ctx, "vote.apply")
	defer span.End()

	at := []slog.Attr{
		slog.Int64("partition", int64(rec.Partition)),
		slog.Int64("offset", rec.Offset),
	}

	var msg domain.VoteMessage
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		c.reject(ctx, reasonMalformed, err, at...)
		return
	}

	cfg, ok := c.awaitConfig(ctx, msg.PollID)
	if !ok {
		c.reject(ctx, reasonUnknownPoll, fmt.Errorf("poll %s not found", msg.PollID), at...)
		return
	}

	if !cfg.Window.Contains(msg.ProducedAt) {
		c.reject(ctx, reasonOutOfWindow, fmt.Errorf("vote out of window: %s", msg.ProducedAt), at...)
		return
	}

	voterID, err := domain.ParseVoterID(msg.VoterID)
	if err != nil {
		c.reject(ctx, reasonBadVoterID, err, at...)
		return
	}

	start := time.Now()
	res, err := c.applyWithRetry(ctx, cfg, voterID, msg.Choices)
	if c.obs != nil {
		c.obs.ApplySeconds(time.Since(start).Seconds())
	}
	if err != nil {
		c.reject(ctx, reasonApplyFailed, err,
			append(at, slog.String("poll", msg.PollID.String()))...)
		return
	}
	if c.obs != nil {
		c.obs.VoteCounted(res.String())
	}
}

func retryable(err error) bool {
	return errors.Is(err, domain.ErrStoreUnavailable) ||
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
	voterID domain.VoterID,
	choices []uint8,
) (domain.VoteResult, error) {
	deadline := time.Now().Add(c.retryBudget)
	backoff := initialApplyBackoff
	warned := false

	for {
		res, err := c.breaker.Execute(func() (domain.VoteResult, error) {
			return c.applier.Apply(ctx, cfg.Sharding(), voterID, choices)
		})
		if err == nil {
			return res, nil
		}
		if !retryable(err) {
			return 0, fmt.Errorf("consumer: apply vote of poll %s: %w", cfg.ID, err)
		}
		if !warned && time.Now().After(deadline) {
			warned = true
			c.log.WarnContext(ctx, "consumer: redis keeps failing, holding the partition",
				slog.String("poll", cfg.ID.String()),
				slog.String("budget", c.retryBudget.String()),
				slog.String("error", err.Error()))
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxApplyBackoff {
			backoff *= 2
		}
	}
}

func (c *Counting) reject(ctx context.Context, reason string, err error, attrs ...slog.Attr) {
	if c.obs != nil {
		c.obs.VoteRejected(reason)
	}
	c.log.LogAttrs(ctx, slog.LevelWarn, "consumer: message rejected",
		append([]slog.Attr{
			slog.String("reason", reason),
			slog.String("error", err.Error()),
		}, attrs...)...)
}
