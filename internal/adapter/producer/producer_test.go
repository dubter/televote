package producer_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/adapter/producer"
)

func TestNew_RejectsIncompleteConfig(t *testing.T) {
	t.Parallel()

	_, err := producer.New(producer.Config{Topic: "votes"})
	require.ErrorIs(t, err, producer.ErrNoBrokers)

	_, err = producer.New(producer.Config{Brokers: []string{"localhost:9092"}})
	require.ErrorIs(t, err, producer.ErrNoTopic)
}
