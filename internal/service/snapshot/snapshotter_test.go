package snapshot_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/snapshot"
	"github.com/dubter/televote/internal/service/snapshot/mocks"
)

var (
	opensAt  = time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	closesAt = opensAt.Add(time.Minute)
)

func openPoll() *domain.Poll {
	return &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusOpen,
		OpensAt: opensAt, ClosesAt: closesAt, ShardCount: 500, Version: 1,
	}
}

type stored struct {
	mu  sync.Mutex
	agg domain.Aggregate
}

func (s *stored) get() domain.Aggregate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.agg
}

func resultStore(t *testing.T, ctrl *gomock.Controller) (*mocks.MockResults, *stored) {
	t.Helper()

	s := &stored{}
	m := mocks.NewMockResults(ctrl)
	m.EXPECT().Get(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, uuid.UUID) (domain.Aggregate, error) { return s.get(), nil },
	).AnyTimes()
	m.EXPECT().Upsert(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uuid.UUID, a domain.Aggregate) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.agg = a.MergeMax(s.agg)
			return nil
		},
	).AnyTimes()
	return m, s
}

func newSnapshotter(
	t *testing.T,
	agg snapshot.Aggregator, res snapshot.Results, polls snapshot.Polls, lag snapshot.LagReader,
	now time.Time,
) *snapshot.Snapshotter {
	t.Helper()

	s, err := snapshot.New(agg, res, polls, lag, snapshot.Config{
		Grace: 30 * time.Second,
		Now:   func() time.Time { return now },
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return s
}

func TestTickOnce_WritesAggregateToPostgres(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()

	agg := mocks.NewMockAggregator(ctrl)
	agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).
		Return(domain.NewAggregateFrom(map[uint8]int64{0: 120, 1: 45}, 165), nil)

	res, store := resultStore(t, ctrl)
	s := newSnapshotter(t, agg, res, mocks.NewMockPolls(ctrl), mocks.NewMockLagReader(ctrl), opensAt.Add(time.Second))

	got, err := s.TickOnce(context.Background(), p)
	require.NoError(t, err)
	assert.Equal(t, map[uint8]int64{0: 120, 1: 45}, got.Votes)
	assert.EqualValues(t, 165, got.Ballots)
	assert.Equal(t, got.Votes, store.get().Votes)
}

func TestTickOnce_IsIdempotent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()

	agg := mocks.NewMockAggregator(ctrl)
	agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).
		Return(domain.NewAggregateFrom(map[uint8]int64{0: 100}, 100), nil).
		Times(5)

	res, store := resultStore(t, ctrl)
	s := newSnapshotter(t, agg, res, mocks.NewMockPolls(ctrl), mocks.NewMockLagReader(ctrl), opensAt.Add(time.Second))

	for range 5 {
		_, err := s.TickOnce(context.Background(), p)
		require.NoError(t, err)
	}

	raw := store.get()
	assert.Equal(t, map[uint8]int64{0: 100}, raw.Votes)
	assert.EqualValues(t, 100, raw.Ballots)
}

func TestNFR3_SnapshotIsMonotonic(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()

	agg := mocks.NewMockAggregator(ctrl)
	gomock.InOrder(
		agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).
			Return(domain.NewAggregateFrom(map[uint8]int64{0: 1000, 1: 500}, 1500), nil),
		agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).
			Return(domain.NewAggregateFrom(map[uint8]int64{0: 900, 1: 600}, 1490), nil),
	)

	res, _ := resultStore(t, ctrl)
	s := newSnapshotter(t, agg, res, mocks.NewMockPolls(ctrl), mocks.NewMockLagReader(ctrl), opensAt.Add(time.Second))

	_, err := s.TickOnce(context.Background(), p)
	require.NoError(t, err)

	got, err := s.TickOnce(context.Background(), p)
	require.NoError(t, err)

	assert.Equal(t, map[uint8]int64{0: 1000, 1: 600}, got.Votes, "счётчик откатился назад")
	assert.EqualValues(t, 1500, got.Ballots)
}

func TestFR6_ScheduledOpensAutomatically(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()
	p.Status = domain.StatusScheduled

	polls := mocks.NewMockPolls(ctrl)
	polls.EXPECT().ListActive(gomock.Any()).Return([]*domain.Poll{p}, nil)
	polls.EXPECT().Transition(gomock.Any(), p.ID, domain.StatusOpen, p.Version).Return(nil).Times(1)

	res, _ := resultStore(t, ctrl)
	s := newSnapshotter(t, mocks.NewMockAggregator(ctrl), res, polls, mocks.NewMockLagReader(ctrl), opensAt.Add(time.Second))

	require.NoError(t, s.Tick(context.Background()))
}

func TestTick_DoesNotOpenBeforeSchedule(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()
	p.Status = domain.StatusScheduled

	polls := mocks.NewMockPolls(ctrl)
	polls.EXPECT().ListActive(gomock.Any()).Return([]*domain.Poll{p}, nil)
	polls.EXPECT().Transition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	res, _ := resultStore(t, ctrl)
	s := newSnapshotter(t, mocks.NewMockAggregator(ctrl), res, polls, mocks.NewMockLagReader(ctrl), opensAt.Add(-time.Minute))

	require.NoError(t, s.Tick(context.Background()))
}

func TestFinalize_WaitsForZeroLag(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()
	afterGrace := closesAt.Add(time.Minute)

	agg := mocks.NewMockAggregator(ctrl)
	agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).
		Return(domain.NewAggregateFrom(map[uint8]int64{0: 10}, 10), nil).
		AnyTimes()

	polls := mocks.NewMockPolls(ctrl)
	polls.EXPECT().ListActive(gomock.Any()).Return([]*domain.Poll{p}, nil).AnyTimes()
	polls.EXPECT().Transition(gomock.Any(), p.ID, domain.StatusClosed, p.Version).Return(nil).Times(1)

	res, store := resultStore(t, ctrl)

	busyLag := mocks.NewMockLagReader(ctrl)
	busyLag.EXPECT().Lag(gomock.Any()).Return(int64(42), nil)
	busy := newSnapshotter(t, agg, res, polls, busyLag, afterGrace)
	require.NoError(t, busy.Tick(context.Background()))

	drainedLag := mocks.NewMockLagReader(ctrl)
	drainedLag.EXPECT().Lag(gomock.Any()).Return(int64(0), nil)
	done := newSnapshotter(t, agg, res, polls, drainedLag, afterGrace)
	require.NoError(t, done.Tick(context.Background()))

	assert.EqualValues(t, 10, store.get().Ballots)
}

func TestFinalize_WaitsForGracePeriod(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()

	agg := mocks.NewMockAggregator(ctrl)
	agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).Return(domain.NewAggregate(), nil)

	polls := mocks.NewMockPolls(ctrl)
	polls.EXPECT().ListActive(gomock.Any()).Return([]*domain.Poll{p}, nil)
	polls.EXPECT().Transition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	lag := mocks.NewMockLagReader(ctrl)
	lag.EXPECT().Lag(gomock.Any()).Times(0)

	res, _ := resultStore(t, ctrl)
	s := newSnapshotter(t, agg, res, polls, lag, closesAt.Add(time.Second))

	require.NoError(t, s.Tick(context.Background()))
}

func TestTickOnce_PropagatesRedisFailure(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()

	agg := mocks.NewMockAggregator(ctrl)
	agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).
		Return(domain.Aggregate{}, errors.New("redis недоступен"))

	res, _ := resultStore(t, ctrl)
	s := newSnapshotter(t, agg, res, mocks.NewMockPolls(ctrl), mocks.NewMockLagReader(ctrl), opensAt.Add(time.Second))

	_, err := s.TickOnce(context.Background(), p)
	require.Error(t, err)
}

func TestTick_ReportsLagAndBallotsToObserver(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	p := openPoll()

	agg := mocks.NewMockAggregator(ctrl)
	agg.EXPECT().Aggregate(gomock.Any(), p.Sharding()).
		Return(domain.NewAggregateFrom(map[uint8]int64{0: 7}, 7), nil)

	polls := mocks.NewMockPolls(ctrl)
	polls.EXPECT().ListActive(gomock.Any()).Return([]*domain.Poll{p}, nil)

	lag := mocks.NewMockLagReader(ctrl)
	lag.EXPECT().Lag(gomock.Any()).Return(int64(1234), nil)

	obs := mocks.NewMockObserver(ctrl)
	obs.EXPECT().SetBallots(p.Slug, int64(7)).Times(1)
	obs.EXPECT().SetConsumerLag(int64(1234)).Times(1)

	res, _ := resultStore(t, ctrl)
	s, err := snapshot.New(agg, res, polls, lag, snapshot.Config{
		Grace:    30 * time.Second,
		Now:      func() time.Time { return closesAt.Add(time.Minute) },
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Observer: obs,
	})
	require.NoError(t, err)

	polls.EXPECT().Transition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	require.NoError(t, s.Tick(context.Background()))
}
