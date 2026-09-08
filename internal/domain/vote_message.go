package domain

import (
	"time"

	"github.com/google/uuid"
)

type VoteMessage struct {
	PollID     uuid.UUID `json:"poll_id"`
	VoterID    string    `json:"voter_id"`
	Choices    []uint8   `json:"choices"`
	ProducedAt time.Time `json:"produced_at"`
}
