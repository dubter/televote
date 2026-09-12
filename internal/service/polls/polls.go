package polls

//go:generate mockgen -source=polls.go -destination=mocks/polls.go -package=mocks

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dubter/televote/internal/domain"
)

type Store interface {
	Create(ctx context.Context, p *domain.Poll) error
	GetBySlug(ctx context.Context, slug string) (*domain.Poll, error)
	List(ctx context.Context) ([]*domain.Poll, error)
	CloseNow(ctx context.Context, id uuid.UUID, version uint32) error
	Transition(ctx context.Context, id uuid.UUID, to domain.Status, version uint32) error
}

type ResultStore interface {
	Get(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, error)
}

type Auditor interface {
	Audit(ctx context.Context, actor, action, entity string, payload any) error
}

type Outcome struct {
	Poll      *domain.Poll
	Aggregate domain.Aggregate
	Final     bool
}

type Service struct {
	store       Store
	results     ResultStore
	audit       Auditor
	now         func() time.Time
	minLeadTime time.Duration
	log         *slog.Logger
}

func New(
	store Store, results ResultStore, audit Auditor,
	now func() time.Time, minLeadTime time.Duration, log *slog.Logger,
) (*Service, error) {
	switch {
	case store == nil:
		return nil, errors.New("polls: store is required")
	case results == nil:
		return nil, errors.New("polls: result store is required")
	case audit == nil:
		return nil, errors.New("polls: auditor is required")
	case now == nil:
		return nil, errors.New("polls: clock is required")
	case log == nil:
		return nil, errors.New("polls: logger is required")
	}
	return &Service{store: store, results: results, audit: audit, now: now, minLeadTime: minLeadTime, log: log}, nil
}

func (s *Service) Create(ctx context.Context, actor string, spec domain.PollSpec) (*domain.Poll, error) {
	poll, err := domain.NewPoll(spec, s.now(), s.minLeadTime)
	if err != nil {
		return nil, err
	}
	if err := s.store.Create(ctx, poll); err != nil {
		return nil, err
	}
	s.record(ctx, actor, "create_poll", poll.Slug, map[string]any{"question": poll.Question})
	return poll, nil
}

func (s *Service) List(ctx context.Context) ([]*domain.Poll, error) {
	return s.store.List(ctx)
}

func (s *Service) Open(ctx context.Context, actor, slug string) (*domain.Poll, error) {
	poll, err := s.store.GetBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	if !poll.Status.CanTransitionTo(domain.StatusOpen) {
		return nil, fmt.Errorf("%w: %s → %s", domain.ErrBadTransition, poll.Status, domain.StatusOpen)
	}
	if err := s.store.Transition(ctx, poll.ID, domain.StatusOpen, poll.Version); err != nil {
		return nil, err
	}
	s.record(ctx, actor, "open_poll", slug, map[string]any{"from": string(poll.Status), "to": string(domain.StatusOpen)})

	poll.Status = domain.StatusOpen
	return poll, nil
}

func (s *Service) Close(ctx context.Context, actor, slug string) (*domain.Poll, error) {
	poll, err := s.store.GetBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	now := s.now()
	if poll.Status != domain.StatusOpen || !poll.Window().IsOpenAt(now) {
		return nil, fmt.Errorf("%w: %s → %s", domain.ErrBadTransition, poll.Status, domain.StatusClosed)
	}
	if err := s.store.CloseNow(ctx, poll.ID, poll.Version); err != nil {
		return nil, err
	}
	s.record(ctx, actor, "close_poll", slug, map[string]any{"from": string(poll.Status)})

	poll.ClosesAt = now
	return poll, nil
}

func (s *Service) Results(ctx context.Context, slug string) (Outcome, error) {
	poll, err := s.store.GetBySlug(ctx, slug)
	if err != nil {
		return Outcome{}, err
	}
	agg, err := s.results.Get(ctx, poll.ID)
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{
		Poll:      poll,
		Aggregate: agg,
		Final:     poll.Status == domain.StatusClosed || poll.Status == domain.StatusArchived,
	}, nil
}

func (s *Service) record(ctx context.Context, actor, action, entity string, payload any) {
	if err := s.audit.Audit(ctx, actor, action, entity, payload); err != nil {
		s.log.ErrorContext(ctx, "polls: audit record was not written",
			slog.String("action", action), slog.String("entity", entity), slog.String("error", err.Error()))
	}
}
