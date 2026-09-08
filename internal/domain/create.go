package domain

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
)

type PollSpec struct {
	Slug               string
	Question           string
	Type               PollType
	Options            []string
	MinChoices         uint8
	MaxChoices         uint8
	OpensAt            time.Time
	ClosesAt           time.Time
	ExpectedAudience   int64
	ExpectedConversion float64
	RedisMasters       int
}

const MinOptions = 2

func NewPoll(spec PollSpec, now time.Time, minLeadTime time.Duration) (*Poll, error) {
	if err := spec.validate(now, minLeadTime); err != nil {
		return nil, err
	}

	options := make([]Option, 0, len(spec.Options))
	for i, text := range spec.Options {
		options = append(options, Option{Idx: uint8(i), Text: strings.TrimSpace(text)})
	}

	minChoices, maxChoices := spec.MinChoices, spec.MaxChoices
	if spec.Type == PollTypeSingle {
		minChoices, maxChoices = 1, 1
	}

	masters := spec.RedisMasters
	if masters <= 0 {
		masters = 1
	}

	return &Poll{
		ID:                 uuid.New(),
		Slug:               spec.Slug,
		Question:           strings.TrimSpace(spec.Question),
		Type:               spec.Type,
		Options:            options,
		MinChoices:         minChoices,
		MaxChoices:         maxChoices,
		Status:             StatusScheduled,
		OpensAt:            spec.OpensAt,
		ClosesAt:           spec.ClosesAt,
		ShardCount:         ShardCountFor(masters),
		ExpectedAudience:   spec.ExpectedAudience,
		ExpectedConversion: spec.ExpectedConversion,
		Version:            1,
	}, nil
}

var ErrInvalidPoll = fmt.Errorf("invalid_poll")

func (s PollSpec) validate(now time.Time, minLeadTime time.Duration) error {
	switch {
	case strings.TrimSpace(s.Slug) == "":
		return fmt.Errorf("%w: пустой slug", ErrInvalidPoll)
	case strings.TrimSpace(s.Question) == "":
		return fmt.Errorf("%w: пустой вопрос", ErrInvalidPoll)
	case !s.Type.Valid():
		return fmt.Errorf("%w: неизвестный тип %q", ErrInvalidPoll, s.Type)
	case len(s.Options) < MinOptions:
		return fmt.Errorf("%w: нужно минимум %d варианта", ErrInvalidPoll, MinOptions)
	case len(s.Options) > MaxOptions:
		return fmt.Errorf("%w: вариантов больше %d", ErrInvalidPoll, MaxOptions)
	}

	for i, text := range s.Options {
		if strings.TrimSpace(text) == "" {
			return fmt.Errorf("%w: пустой вариант на позиции %d", ErrInvalidPoll, i)
		}
	}

	if s.Type == PollTypeMultiple {
		if s.MinChoices == 0 {
			return fmt.Errorf("%w: множественный выбор требует min_choices", ErrInvalidPoll)
		}
		if s.MaxChoices > 0 && s.MaxChoices < s.MinChoices {
			return fmt.Errorf("%w: max_choices меньше min_choices", ErrInvalidPoll)
		}
		if int(s.MinChoices) > len(s.Options) {
			return fmt.Errorf("%w: min_choices больше числа вариантов", ErrInvalidPoll)
		}
	}

	switch {
	case s.OpensAt.IsZero() || s.ClosesAt.IsZero():
		return fmt.Errorf("%w: окно голосования не задано", ErrInvalidPoll)
	case !s.ClosesAt.After(s.OpensAt):
		return fmt.Errorf("%w: closes_at не позже opens_at", ErrInvalidPoll)
	}

	if minLeadTime > 0 && s.OpensAt.Sub(now) < minLeadTime {
		return fmt.Errorf("%w: опрос открывается раньше чем через %s, ёмкость не успеет подняться",
			ErrInvalidPoll, minLeadTime)
	}
	return nil
}

func (p *Poll) ExpectedVotes() int64 {
	v := float64(p.ExpectedAudience) * p.ExpectedConversion
	switch {
	case v <= 0:
		return 0
	case v > math.MaxInt64:
		return math.MaxInt64
	default:
		return int64(v)
	}
}
