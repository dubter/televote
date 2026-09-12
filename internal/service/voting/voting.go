package voting

//go:generate mockgen -source=voting.go -destination=mocks/voting.go -package=mocks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/pollcfg"
)

type ConfigLookup interface {
	BySlug(slug string) (*pollcfg.HotConfig, bool)
}

type Sink interface {
	Send(ctx context.Context, m domain.VoteMessage) error
}

type Observer interface {
	VoteAccepted()
	VoteRejected(reason string)
	ProduceSeconds(d float64)
}

const (
	reasonUnknownPoll    = "unknown_poll"
	reasonInvalidChoices = "invalid_choices"
	reasonPollClosed     = "poll_closed"
	reasonBadVoter       = "bad_voter"
	reasonUnavailable    = "unavailable"
)

type Service struct {
	lookup ConfigLookup
	sink   Sink
	obs    Observer
	now    func() time.Time
}

func New(lookup ConfigLookup, sink Sink, obs Observer, now func() time.Time) (*Service, error) {
	switch {
	case lookup == nil:
		return nil, errors.New("voting: poll config lookup is required")
	case sink == nil:
		return nil, errors.New("voting: vote sink is required")
	case obs == nil:
		return nil, errors.New("voting: observer is required")
	case now == nil:
		return nil, errors.New("voting: clock is required")
	}
	return &Service{lookup: lookup, sink: sink, obs: obs, now: now}, nil
}

func (s *Service) Accept(ctx context.Context, slug, clientID string, choices []uint8) error {
	cfg, ok := s.lookup.BySlug(slug)
	if !ok {
		s.obs.VoteRejected(reasonUnknownPoll)
		return fmt.Errorf("voting: poll %q: %w", slug, domain.ErrNotFound)
	}

	if err := cfg.Rules.Validate(choices); err != nil {
		s.obs.VoteRejected(reasonInvalidChoices)
		return err
	}
	if !cfg.Window.IsOpenAt(s.now()) {
		s.obs.VoteRejected(reasonPollClosed)
		return domain.ErrPollClosed
	}

	voterID, err := domain.DeriveVoterID(cfg.Salt, clientID)
	if err != nil {
		s.obs.VoteRejected(reasonBadVoter)
		return err
	}

	msg := domain.VoteMessage{
		PollID:     cfg.ID,
		VoterID:    voterID.Hex(),
		Choices:    choices,
		ProducedAt: s.now().UTC(),
	}

	start := s.now()
	if err := s.sink.Send(ctx, msg); err != nil {
		s.obs.VoteRejected(reasonUnavailable)
		return fmt.Errorf("%w: %w", domain.ErrQueueUnavailable, err)
	}
	s.obs.ProduceSeconds(s.now().Sub(start).Seconds())
	s.obs.VoteAccepted()
	return nil
}
