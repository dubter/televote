package counting

//go:generate mockgen -source=counting.go -destination=mocks/counting.go -package=mocks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker/v2"
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
	reasonUnknownPoll = "unknown_poll"
	reasonOutOfWindow = "out_of_window"
	reasonBadVoterID  = "bad_voter_id"
	reasonApplyFailed = "apply_failed"
)

const (
	defaultRetryBudget   = 30 * time.Second
	defaultLookupBudget  = 10 * time.Second
	defaultErrorRatio    = 0.5
	defaultBreakerWindow = 5 * time.Second
	initialApplyBackoff  = 20 * time.Millisecond
	maxApplyBackoff      = 2 * time.Second
	lookupPollInterval   = 250 * time.Millisecond
	breakerMinRequests   = 20
)

type Config struct {
	RetryBudget   time.Duration
	LookupBudget  time.Duration
	ErrorRatio    float64
	BreakerWindow time.Duration
}

type Service struct {
	applier Applier
	lookup  ConfigLookup
	obs     Observer
	log     *slog.Logger

	retryBudget  time.Duration
	lookupBudget time.Duration
	breaker      *gobreaker.CircuitBreaker[domain.VoteResult]
	tracer       trace.Tracer
}

func New(applier Applier, lookup ConfigLookup, obs Observer, log *slog.Logger, cfg Config) (*Service, error) {
	switch {
	case applier == nil:
		return nil, errors.New("counting: applier is required")
	case lookup == nil:
		return nil, errors.New("counting: poll config lookup is required")
	case obs == nil:
		return nil, errors.New("counting: observer is required")
	case log == nil:
		return nil, errors.New("counting: logger is required")
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

	return &Service{
		applier:      applier,
		lookup:       lookup,
		obs:          obs,
		log:          log,
		retryBudget:  cfg.RetryBudget,
		lookupBudget: cfg.LookupBudget,
		breaker:      newBreaker(cfg, log, obs),
		tracer:       otel.Tracer("televote/counting"),
	}, nil
}

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
			log.Warn("counting: breaker changed state",
				slog.String("breaker", name),
				slog.String("from", from.String()), slog.String("to", to.String()))
			obs.SetBreakerOpen(to != gobreaker.StateClosed)
		},
	})
}

func (s *Service) Count(ctx context.Context, msg domain.VoteMessage) (domain.VoteResult, error) {
	ctx, span := s.tracer.Start(ctx, "vote.apply")
	defer span.End()

	cfg, ok := s.awaitConfig(ctx, msg.PollID)
	if !ok {
		s.obs.VoteRejected(reasonUnknownPoll)
		return 0, fmt.Errorf("counting: poll %s: %w", msg.PollID, domain.ErrNotFound)
	}

	if !cfg.Window.Contains(msg.ProducedAt) {
		s.obs.VoteRejected(reasonOutOfWindow)
		return 0, fmt.Errorf("counting: vote produced at %s: %w", msg.ProducedAt.Format(time.RFC3339), domain.ErrPollClosed)
	}

	voterID, err := domain.ParseVoterID(msg.VoterID)
	if err != nil {
		s.obs.VoteRejected(reasonBadVoterID)
		return 0, fmt.Errorf("%w: %w", domain.ErrInvalidVote, err)
	}

	start := time.Now()
	res, err := s.applyWithRetry(ctx, cfg, voterID, msg.Choices)
	s.obs.ApplySeconds(time.Since(start).Seconds())
	if err != nil {
		s.obs.VoteRejected(reasonApplyFailed)
		return 0, err
	}
	s.obs.VoteCounted(res.String())
	return res, nil
}

func (s *Service) awaitConfig(ctx context.Context, pollID uuid.UUID) (*pollcfg.HotConfig, bool) {
	if cfg, ok := s.lookup.ByID(pollID); ok {
		return cfg, true
	}

	deadline := time.Now().Add(s.lookupBudget)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return nil, false
		case <-time.After(lookupPollInterval):
		}
		if cfg, ok := s.lookup.ByID(pollID); ok {
			return cfg, true
		}
	}
	return nil, false
}

func (s *Service) applyWithRetry(
	ctx context.Context,
	cfg *pollcfg.HotConfig,
	voterID domain.VoterID,
	choices []uint8,
) (domain.VoteResult, error) {
	deadline := time.Now().Add(s.retryBudget)
	backoff := initialApplyBackoff
	warned := false

	for {
		res, err := s.breaker.Execute(func() (domain.VoteResult, error) {
			return s.applier.Apply(ctx, cfg.Sharding(), voterID, choices)
		})
		if err == nil {
			return res, nil
		}
		if !retryable(err) {
			return 0, fmt.Errorf("counting: apply vote of poll %s: %w", cfg.ID, err)
		}
		if !warned && time.Now().After(deadline) {
			warned = true
			s.log.WarnContext(ctx, "counting: redis keeps failing, holding the partition",
				slog.String("poll", cfg.ID.String()),
				slog.String("budget", s.retryBudget.String()),
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

func retryable(err error) bool {
	return errors.Is(err, domain.ErrStoreUnavailable) ||
		errors.Is(err, gobreaker.ErrOpenState) ||
		errors.Is(err, gobreaker.ErrTooManyRequests)
}
