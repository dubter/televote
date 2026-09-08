package producer_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/producer"
)

func TestNew_RejectsIncompleteConfig(t *testing.T) {
	t.Parallel()

	_, err := producer.New(producer.Config{Topic: "votes"})
	require.ErrorIs(t, err, producer.ErrNoBrokers)

	_, err = producer.New(producer.Config{Brokers: []string{"localhost:9092"}})
	require.ErrorIs(t, err, producer.ErrNoTopic)
}

// Сообщение переживает retention Kafka. Полный IP или User-Agent в нём
// означали бы хранилище персональных данных вместо буфера — прямое нарушение
// заявленной анонимности (NFR-4).
func TestVoteMessage_ContainsNoRawIP(t *testing.T) {
	t.Parallel()

	m := producer.VoteMessage{
		PollID:     uuid.New(),
		VoterID:    "0123456789abcdef0123456789abcdef",
		Choices:    []uint8{1},
		Net16:      "203.0.0.0/16",
		UAClass:    "iOS 18",
		ProducedAt: time.Now(),
	}

	raw, err := json.Marshal(m)
	require.NoError(t, err)
	payload := string(raw)

	assert.NotContains(t, payload, "203.0.113.42")
	assert.Contains(t, payload, "203.0.0.0/16")

	// User-Agent целиком — это отпечаток. Уезжает только грубый класс.
	assert.NotContains(t, payload, "Mozilla")
	assert.NotContains(t, payload, "AppleWebKit")

	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(raw, &fields))
	for _, forbidden := range []string{"ip", "addr", "address", "user_agent", "ua_full", "email"} {
		_, present := fields[forbidden]
		assert.False(t, present, "в сообщении не должно быть поля %q", forbidden)
	}
}

// Окно голосования проверяет консьюмер, и проверяет он по этой метке:
// голос с последней секунды эфира обрабатывается через минуты после закрытия.
func TestVoteMessage_ProducedAtRoundTrips(t *testing.T) {
	t.Parallel()

	want := time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	raw, err := json.Marshal(producer.VoteMessage{ProducedAt: want})
	require.NoError(t, err)

	var back producer.VoteMessage
	require.NoError(t, json.Unmarshal(raw, &back))
	assert.True(t, want.Equal(back.ProducedAt))
}

// Сообщение отправляется 30 млн раз за минуту: каждый лишний байт в поле —
// это мегабайты трафика внутри кластера и объём на диске брокеров.
func TestVoteMessage_IsCompact(t *testing.T) {
	t.Parallel()

	raw, err := json.Marshal(producer.VoteMessage{
		PollID:     uuid.New(),
		VoterID:    strings.Repeat("a", 32),
		Choices:    []uint8{1},
		Net16:      "203.0.0.0/16",
		UAClass:    "iOS 18",
		ProducedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	assert.Less(t, len(raw), 200, "сообщение раздулось: %s", raw)
}
