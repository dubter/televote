package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/consumer/mocks"
	"github.com/dubter/televote/internal/service/pollcfg"
)

var (
	opensAt  = time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	closesAt = opensAt.Add(time.Minute)
	testSalt = []byte("consumer-test-salt-0123456789abc")

	errRedisDown = fmt.Errorf("%w: redis недоступен", domain.ErrStoreUnavailable)
)

func testConfig(t *testing.T) *pollcfg.HotConfig {
	t.Helper()

	p := &domain.Poll{
		ID:         uuid.New(),
		Slug:       "final",
		Type:       domain.PollTypeSingle,
		Options:    []domain.Option{{Idx: 0}, {Idx: 1}, {Idx: 2}},
		Status:     domain.StatusOpen,
		OpensAt:    opensAt,
		ClosesAt:   closesAt,
		ShardCount: 500,
		Salt:       testSalt,
	}
	return &pollcfg.HotConfig{
		ID: p.ID, Slug: p.Slug, Rules: p.ChoiceRules(),
		Window: p.Window(), ShardCount: p.ShardCount, Salt: p.Salt,
	}
}

func newTestCounting(t *testing.T, cfg Config) (
	*Counting, *mocks.MockApplier, *mocks.MockConfigLookup, *mocks.MockObserver,
) {
	t.Helper()

	ctrl := gomock.NewController(t)
	applier := mocks.NewMockApplier(ctrl)
	lookup := mocks.NewMockConfigLookup(ctrl)
	obs := mocks.NewMockObserver(ctrl)

	c := newCounting(applier, lookup, obs, slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	return c, applier, lookup, obs
}

func record(t *testing.T, cfg *pollcfg.HotConfig, clientID string, choices []uint8, at time.Time) *kgo.Record {
	t.Helper()

	v, err := domain.DeriveVoterID(cfg.Salt, clientID)
	require.NoError(t, err)

	payload, err := json.Marshal(domain.VoteMessage{
		PollID: cfg.ID, VoterID: v.Hex(), Choices: choices, ProducedAt: at,
	})
	require.NoError(t, err)
	return &kgo.Record{Value: payload}
}

func TestCounting_AppliesVoteExactlyOnceWithPollShardCount(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{RetryBudget: time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).Times(1)
	applier.EXPECT().
		Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), []uint8{1}).
		Return(domain.VoteCounted, nil).
		Times(1)
	obs.EXPECT().ApplySeconds(gomock.Any()).Times(1)
	obs.EXPECT().VoteCounted(domain.VoteCounted.String()).Times(1)

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{1}, opensAt.Add(time.Second)))
}

func TestCounting_DuplicateDeliveryDoesNotDoubleCount(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{RetryBudget: time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).AnyTimes()
	obs.EXPECT().ApplySeconds(gomock.Any()).AnyTimes()
	obs.EXPECT().VoteRejected(reasonApplyFailed).AnyTimes()

	gomock.InOrder(
		applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), []uint8{2}).
			Return(domain.VoteCounted, nil),
		applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), []uint8{2}).
			Return(domain.VoteAlreadyCounted, nil).Times(4),
	)
	obs.EXPECT().VoteCounted(domain.VoteCounted.String()).Times(1)
	obs.EXPECT().VoteCounted(domain.VoteAlreadyCounted.String()).Times(4)

	rec := record(t, cfg, "viewer", []uint8{2}, opensAt.Add(time.Second))
	for range 5 {
		c.applyRecord(context.Background(), rec)
	}
}

func TestCounting_UsesProducedAtNotProcessingTime(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{RetryBudget: time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true)
	applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), gomock.Any()).
		Return(domain.VoteCounted, nil).
		Times(1)
	obs.EXPECT().ApplySeconds(gomock.Any())
	obs.EXPECT().VoteCounted(domain.VoteCounted.String())

	c.applyRecord(context.Background(), record(t, cfg, "late", []uint8{0}, closesAt.Add(-time.Second)))
}

func TestCounting_RejectsVoteProducedOutsideWindow(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, _, lookup, obs := newTestCounting(t, Config{RetryBudget: time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).Times(2)
	obs.EXPECT().VoteRejected(reasonOutOfWindow).Times(2)

	c.applyRecord(context.Background(), record(t, cfg, "early", []uint8{0}, opensAt.Add(-time.Second)))
	c.applyRecord(context.Background(), record(t, cfg, "late", []uint8{0}, closesAt.Add(time.Second)))
}

func TestCounting_RejectsMalformedAndUnknown(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, _, lookup, obs := newTestCounting(t, Config{
		RetryBudget: time.Second, LookupBudget: 20 * time.Millisecond,
	})

	foreignID := uuid.New()
	lookup.EXPECT().ByID(foreignID).Return(nil, false).MinTimes(1)
	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).Times(1)

	gomock.InOrder(
		obs.EXPECT().VoteRejected(reasonMalformed),
		obs.EXPECT().VoteRejected(reasonUnknownPoll),
		obs.EXPECT().VoteRejected(reasonBadVoterID),
	)

	c.applyRecord(context.Background(), &kgo.Record{Value: []byte("не json")})

	foreign, err := json.Marshal(domain.VoteMessage{
		PollID: foreignID, VoterID: "0123456789abcdef0123456789abcdef",
		Choices: []uint8{0}, ProducedAt: opensAt.Add(time.Second),
	})
	require.NoError(t, err)
	c.applyRecord(context.Background(), &kgo.Record{Value: foreign})

	badVoter, err := json.Marshal(domain.VoteMessage{
		PollID: cfg.ID, VoterID: "не-hex", Choices: []uint8{0}, ProducedAt: opensAt.Add(time.Second),
	})
	require.NoError(t, err)
	c.applyRecord(context.Background(), &kgo.Record{Value: badVoter})
}

func TestCounting_RetriesRetryableErrorUntilSuccess(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{RetryBudget: 2 * time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true)
	gomock.InOrder(
		applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), gomock.Any()).
			Return(domain.VoteResult(0), errRedisDown),
		applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), gomock.Any()).
			Return(domain.VoteResult(0), errRedisDown),
		applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), gomock.Any()).
			Return(domain.VoteCounted, nil),
	)
	obs.EXPECT().ApplySeconds(gomock.Any())
	obs.EXPECT().VoteCounted(domain.VoteCounted.String())

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{0}, opensAt.Add(time.Second)))
}

func TestCounting_DoesNotRetryPermanentError(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{RetryBudget: time.Second, LookupBudget: time.Second})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true)
	applier.EXPECT().Apply(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(domain.VoteResult(0), domain.ErrInvalidVote).
		Times(1)
	obs.EXPECT().ApplySeconds(gomock.Any())
	obs.EXPECT().VoteCounted(gomock.Any()).Times(0)
	obs.EXPECT().VoteRejected(reasonApplyFailed).Times(1)

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{0}, opensAt.Add(time.Second)))
}

func TestCounting_HoldsPartitionInsteadOfDroppingVote(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{
		RetryBudget: 10 * time.Millisecond, LookupBudget: time.Second,
	})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).AnyTimes()
	applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), gomock.Any()).
		Return(domain.VoteResult(0), errRedisDown).
		MinTimes(2)
	obs.EXPECT().ApplySeconds(gomock.Any()).AnyTimes()
	obs.EXPECT().VoteRejected(reasonApplyFailed).AnyTimes()
	obs.EXPECT().VoteCounted(gomock.Any()).Times(0)

	voter, err := domain.DeriveVoterID(cfg.Salt, "viewer")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	res, err := c.applyWithRetry(ctx, cfg, voter, []uint8{0})
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"ретраи обязаны идти, пока жив контекст: держать партицию лучше, чем потерять голос")
	assert.Equal(t, domain.VoteResult(0), res, "неприменённый голос не имеет права выглядеть посчитанным")

	short, cancelShort := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelShort()

	c.applyRecord(short, record(t, cfg, "viewer", []uint8{0}, opensAt.Add(time.Second)))
}

func TestCounting_WaitsForConfigInsteadOfDroppingVote(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{RetryBudget: time.Second, LookupBudget: 2 * time.Second})

	gomock.InOrder(
		lookup.EXPECT().ByID(cfg.ID).Return(nil, false),
		lookup.EXPECT().ByID(cfg.ID).Return(nil, false),
		lookup.EXPECT().ByID(cfg.ID).Return(cfg, true),
	)
	applier.EXPECT().Apply(gomock.Any(), cfg.Sharding(), gomock.Any(), gomock.Any()).
		Return(domain.VoteCounted, nil)
	obs.EXPECT().ApplySeconds(gomock.Any())
	obs.EXPECT().VoteCounted(domain.VoteCounted.String())

	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{0}, opensAt.Add(time.Second)))
}

func TestCounting_GivesUpOnGenuinelyUnknownPoll(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, _, lookup, obs := newTestCounting(t, Config{
		RetryBudget: time.Second, LookupBudget: 20 * time.Millisecond,
	})

	lookup.EXPECT().ByID(cfg.ID).Return(nil, false).MinTimes(1)
	obs.EXPECT().VoteRejected(reasonUnknownPoll).Times(1)

	start := time.Now()
	c.applyRecord(context.Background(), record(t, cfg, "viewer", []uint8{1}, opensAt.Add(time.Second)))

	assert.Less(t, time.Since(start), 2*time.Second, "консьюмер завис на чужом сообщении")
}

func TestCounting_BreakerStopsCallingRedisAfterErrorRatio(t *testing.T) {
	t.Parallel()

	cfg := testConfig(t)
	c, applier, lookup, obs := newTestCounting(t, Config{
		RetryBudget: 50 * time.Millisecond, LookupBudget: time.Second,
		ErrorRatio: 0.5, BreakerWindow: time.Minute,
	})

	lookup.EXPECT().ByID(cfg.ID).Return(cfg, true).AnyTimes()
	obs.EXPECT().ApplySeconds(gomock.Any()).AnyTimes()
	obs.EXPECT().VoteRejected(reasonApplyFailed).AnyTimes()
	obs.EXPECT().VoteCounted(gomock.Any()).Times(0)
	obs.EXPECT().SetBreakerOpen(true).MinTimes(1)

	applier.EXPECT().Apply(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(domain.VoteResult(0), errRedisDown).
		MinTimes(breakerMinRequests).
		MaxTimes(breakerMinRequests * 3)

	for i := range 40 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		c.applyRecord(ctx, record(t, cfg, string(rune('a'+i%26)), []uint8{0}, opensAt.Add(time.Second)))
		cancel()
	}

	_, err := c.breaker.Execute(func() (domain.VoteResult, error) { return domain.VoteCounted, nil })
	require.Error(t, err, "брейкер обязан быть открыт после серии отказов Redis")
	assert.True(t, retryable(err), "открытый брейкер — временное состояние, голос надо повторить")
}
