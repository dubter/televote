package consumer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/service/consumer/mocks"
	"github.com/dubter/televote/internal/service/vote"
)

var errRedisDown = errors.New("redis недоступен")

func mockCounting(t *testing.T, cfg Config) (*Counting, *mocks.MockApplier, *mocks.MockConfigLookup, *mocks.MockObserver) {
	t.Helper()

	ctrl := gomock.NewController(t)
	applier := mocks.NewMockApplier(ctrl)
	lookup := mocks.NewMockConfigLookup(ctrl)
	obs := mocks.NewMockObserver(ctrl)

	c := newCounting(applier, lookup, obs, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	return c, applier, lookup, obs
}

func TestCounting_AppliesVoteExactlyOnceWithPollShardCount(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := mockCounting(t, Config{RetryBudget: time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).Times(1)
	applier.EXPECT().
		Cast(gomock.Any(), cfg.ID, cfg.ShardCount, gomock.Any(), []uint8{1}).
		Return(vote.ResultCounted, nil).
		Times(1)
	obs.EXPECT().ApplySeconds(gomock.Any()).Times(1)
	obs.EXPECT().VoteCounted(vote.ResultCounted).Times(1)

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{1}, opensAt.Add(time.Second)))
}

func TestCounting_RetriesRetryableErrorUntilSuccess(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := mockCounting(t, Config{RetryBudget: 2 * time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true)
	gomock.InOrder(
		applier.EXPECT().Cast(gomock.Any(), cfg.ID, cfg.ShardCount, gomock.Any(), gomock.Any()).
			Return(vote.Result(0), errRedisDown),
		applier.EXPECT().Cast(gomock.Any(), cfg.ID, cfg.ShardCount, gomock.Any(), gomock.Any()).
			Return(vote.Result(0), errRedisDown),
		applier.EXPECT().Cast(gomock.Any(), cfg.ID, cfg.ShardCount, gomock.Any(), gomock.Any()).
			Return(vote.ResultCounted, nil),
	)
	obs.EXPECT().ApplySeconds(gomock.Any())
	obs.EXPECT().VoteCounted(vote.ResultCounted)

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{0}, opensAt.Add(time.Second)))
}

func TestCounting_DoesNotRetryPermanentErrorEvenOnce(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := mockCounting(t, Config{RetryBudget: time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true)
	applier.EXPECT().Cast(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(vote.Result(0), vote.ErrInvalidArgs).
		Times(1)
	obs.EXPECT().ApplySeconds(gomock.Any())

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{0}, opensAt.Add(time.Second)))
}

func TestCounting_WaitsForConfigInsteadOfDroppingVote(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := mockCounting(t, Config{RetryBudget: time.Second, LookupBudget: 2 * time.Second})

	gomock.InOrder(
		lookup.EXPECT().ByID(cfg.ID).Return(nil, false),
		lookup.EXPECT().ByID(cfg.ID).Return(nil, false),
		lookup.EXPECT().ByID(cfg.ID).Return(cfg, true),
	)
	applier.EXPECT().Cast(gomock.Any(), cfg.ID, gomock.Any(), gomock.Any(), gomock.Any()).
		Return(vote.ResultCounted, nil)
	obs.EXPECT().ApplySeconds(gomock.Any())
	obs.EXPECT().VoteCounted(vote.ResultCounted)

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{0}, opensAt.Add(time.Second)))
}

func TestCounting_BreakerStopsCallingRedisAfterErrorRatio(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := mockCounting(t, Config{
		RetryBudget: 50 * time.Millisecond, LookupBudget: time.Second,
		ErrorRatio: 0.5, BreakerWindow: time.Minute,
	})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).AnyTimes()
	obs.EXPECT().ApplySeconds(gomock.Any()).AnyTimes()

	applier.EXPECT().Cast(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(vote.Result(0), errRedisDown).
		MinTimes(breakerMinRequests).
		MaxTimes(breakerMinRequests * 3)

	for i := range 40 {
		c.applyRecord(context.Background(), record(t, cfg, string(rune('a'+i%26)), []uint8{0}, opensAt.Add(time.Second)))
	}

	_, err := c.breaker.Execute(func() (vote.Result, error) { return vote.ResultCounted, nil })
	require.Error(t, err, "брейкер обязан быть открыт после серии отказов Redis")
	assert.True(t, c.retryable(err), "открытый брейкер — временное состояние, голос надо повторить")
}
