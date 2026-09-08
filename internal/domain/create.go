package domain

import (
	"fmt"
	"math"
	"regexp"
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

const (
	MinOptions = 2

	MaxQuestionLen = 1024
	MaxOptionLen   = 512
	MaxSlugLen     = 64
)

var slugShape = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

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
		ShardCount:         ShardCountFor(spec.RedisMasters),
		ExpectedAudience:   spec.ExpectedAudience,
		ExpectedConversion: spec.ExpectedConversion,
		Version:            1,
	}, nil
}

var ErrInvalidPoll = fmt.Errorf("invalid_poll")

func (s PollSpec) validate(now time.Time, minLeadTime time.Duration) error {
	question := strings.TrimSpace(s.Question)

	switch {
	case !slugShape.MatchString(s.Slug):
		return fmt.Errorf("%w: slug does not match ^[a-z0-9][a-z0-9_-]{0,%d}$", ErrInvalidPoll, MaxSlugLen-1)
	case question == "":
		return fmt.Errorf("%w: empty question", ErrInvalidPoll)
	case len(question) > MaxQuestionLen:
		return fmt.Errorf("%w: question is longer than %d characters", ErrInvalidPoll, MaxQuestionLen)
	case !s.Type.Valid():
		return fmt.Errorf("%w: unknown type %q", ErrInvalidPoll, s.Type)
	case len(s.Options) < MinOptions:
		return fmt.Errorf("%w: at least %d options are required", ErrInvalidPoll, MinOptions)
	case len(s.Options) > MaxOptions:
		return fmt.Errorf("%w: more than %d options", ErrInvalidPoll, MaxOptions)
	}

	for i, text := range s.Options {
		option := strings.TrimSpace(text)
		if option == "" {
			return fmt.Errorf("%w: empty option at position %d", ErrInvalidPoll, i)
		}
		if len(option) > MaxOptionLen {
			return fmt.Errorf("%w: option %d is longer than %d characters", ErrInvalidPoll, i, MaxOptionLen)
		}
	}

	if s.ExpectedAudience < 0 {
		return fmt.Errorf("%w: negative expected audience", ErrInvalidPoll)
	}
	if s.ExpectedConversion < 0 || s.ExpectedConversion > 1 {
		return fmt.Errorf("%w: conversion %v is out of [0,1] — it is a ratio, not percent",
			ErrInvalidPoll, s.ExpectedConversion)
	}

	if s.Type == PollTypeMultiple {
		if s.MinChoices == 0 {
			return fmt.Errorf("%w: multiple choice requires min_choices", ErrInvalidPoll)
		}
		if s.MaxChoices > 0 && s.MaxChoices < s.MinChoices {
			return fmt.Errorf("%w: max_choices is less than min_choices", ErrInvalidPoll)
		}
		if int(s.MinChoices) > len(s.Options) {
			return fmt.Errorf("%w: min_choices is greater than the number of options", ErrInvalidPoll)
		}
	}

	switch {
	case s.OpensAt.IsZero() || s.ClosesAt.IsZero():
		return fmt.Errorf("%w: voting window is not set", ErrInvalidPoll)
	case !s.ClosesAt.After(s.OpensAt):
		return fmt.Errorf("%w: closes_at is not after opens_at", ErrInvalidPoll)
	}

	if minLeadTime > 0 && s.OpensAt.Sub(now) < minLeadTime {
		return fmt.Errorf("%w: poll opens in less than %s, capacity will not scale up in time",
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
