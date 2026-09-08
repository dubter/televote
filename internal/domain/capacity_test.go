package domain_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
)

func TestCapacityFor(t *testing.T) {
	t.Parallel()

	const thirtyMillion = 30_000_000

	tests := []struct {
		name  string
		votes int64
		drain time.Duration
		check func(t *testing.T, c domain.Capacity)
	}{
		{
			name:  "расчётный эфир с дренажом в пять минут",
			votes: thirtyMillion,
			drain: 5 * time.Minute,
			check: func(t *testing.T, c domain.Capacity) {
				assert.InDelta(t, 30, c.VoteAPI, 6, "приём считается по пику")
				assert.InDelta(t, 3, c.RedisMasters, 2, "дренаж за 5 минут")
			},
		},
		{
			name:  "сжатый дренаж дорожает по Redis",
			votes: thirtyMillion,
			drain: time.Minute,
			check: func(t *testing.T, c domain.Capacity) {
				assert.Greater(t, c.RedisMasters, 10, "минутный дренаж требует кратно больше мастеров")
			},
		},
		{
			name:  "нет голосов — базовая линия, а не ноль подов",
			votes: 0,
			drain: 5 * time.Minute,
			check: func(t *testing.T, c domain.Capacity) {
				assert.Positive(t, c.VoteAPI, "админка обязана отвечать между эфирами")
				assert.Zero(t, c.RedisMasters, "Redis между эфирами не нужен вовсе")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.check(t, domain.CapacityFor(tc.votes, tc.drain))
		})
	}
}

func TestCapacityFor_LongerDrainNeedsFewerMasters(t *testing.T) {
	t.Parallel()

	fast := domain.CapacityFor(30_000_000, time.Minute)
	slow := domain.CapacityFor(30_000_000, 15*time.Minute)

	assert.Greater(t, fast.RedisMasters, slow.RedisMasters)
	assert.Equal(t, fast.VoteAPI, slow.VoteAPI, "приём от длины дренажа не зависит")
}

func TestCapacityFor_IsMonotonic(t *testing.T) {
	t.Parallel()

	prev := domain.CapacityFor(0, 5*time.Minute)
	for votes := int64(1_000_000); votes <= 100_000_000; votes += 7_000_000 {
		c := domain.CapacityFor(votes, 5*time.Minute)
		require.GreaterOrEqual(t, c.VoteAPI, prev.VoteAPI, "votes=%d", votes)
		require.GreaterOrEqual(t, c.RedisMasters, prev.RedisMasters, "votes=%d", votes)
		require.Positive(t, c.KafkaPartitions)
		prev = c
	}
}

func TestCapacityFor_PartitionsCoverConsumers(t *testing.T) {
	t.Parallel()

	for _, votes := range []int64{1_000_000, 30_000_000, 100_000_000} {
		c := domain.CapacityFor(votes, 5*time.Minute)
		assert.GreaterOrEqual(t, c.KafkaPartitions, c.Consumers, "votes=%d", votes)
	}
}

func TestPoll_ExpectedVotes(t *testing.T) {
	t.Parallel()

	p := &domain.Poll{ExpectedAudience: 100_000_000, ExpectedConversion: 0.3}
	assert.EqualValues(t, 30_000_000, p.ExpectedVotes())

	assert.Zero(t, (&domain.Poll{}).ExpectedVotes())
	assert.Zero(t, (&domain.Poll{ExpectedAudience: -5, ExpectedConversion: 0.3}).ExpectedVotes(),
		"порча строки не должна давать отрицательную ёмкость")
}
