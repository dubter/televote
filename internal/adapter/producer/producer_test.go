package producer_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/adapter/producer"
	"github.com/dubter/televote/internal/domain"
)

func TestNew_RejectsIncompleteConfig(t *testing.T) {
	t.Parallel()

	_, err := producer.New(producer.Config{Topic: "votes"})
	require.ErrorIs(t, err, producer.ErrNoBrokers)

	_, err = producer.New(producer.Config{Brokers: []string{"localhost:9092"}})
	require.ErrorIs(t, err, producer.ErrNoTopic)
}

func TestVoteMessage_IsCompact(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(domain.VoteMessage{
		PollID:     uuid.New(),
		VoterID:    strings.Repeat("a", 32),
		Choices:    []uint8{1},
		ProducedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	assert.Less(t, len(raw), 200, "сообщение раздулось: %s", raw)
}
