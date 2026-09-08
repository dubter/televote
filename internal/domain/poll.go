package domain

import (
	"time"

	"github.com/google/uuid"
)

type PollType string

const (
	PollTypeSingle   PollType = "single"
	PollTypeMultiple PollType = "multiple"
)

func (t PollType) Valid() bool {
	switch t {
	case PollTypeSingle, PollTypeMultiple:
		return true
	default:
		return false
	}
}

type Status string

const (
	StatusDraft     Status = "draft"
	StatusScheduled Status = "scheduled"
	StatusOpen      Status = "open"
	StatusClosed    Status = "closed"
	StatusArchived  Status = "archived"
)

func (s Status) Valid() bool {
	switch s {
	case StatusDraft, StatusScheduled, StatusOpen, StatusClosed, StatusArchived:
		return true
	default:
		return false
	}
}

const MaxOptions = 255

type Option struct {
	Idx  uint8
	Text string
}

type Poll struct {
	ID                         uuid.UUID
	Slug                       string
	Question                   string
	Type                       PollType
	Options                    []Option
	MinChoices                 uint8
	MaxChoices                 uint8
	Status                     Status
	OpensAt                    time.Time
	ClosesAt                   time.Time
	ShardCount                 uint16
	ResultsVisibleDuringVoting bool
	Salt                       []byte
	ExpectedAudience           int64
	ExpectedConversion         float64
	Version                    uint32
}

func (p *Poll) OptionCount() uint8 {
	if len(p.Options) > MaxOptions {
		return MaxOptions
	}
	return uint8(len(p.Options))
}

const (
	ShardsPerMaster = 500

	MaxShardCount = 16384
)

func ShardCountFor(masters int) uint16 {
	if masters <= 1 {
		return ShardsPerMaster
	}
	if masters > MaxShardCount/ShardsPerMaster {
		return MaxShardCount
	}
	return uint16(masters * ShardsPerMaster)
}
