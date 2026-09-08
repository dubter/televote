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

	"github.com/OWNER/televote/internal/domain"
	"github.com/OWNER/televote/internal/snapshot"
)

var (
	opensAt  = time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	closesAt = opensAt.Add(time.Minute)
)

// fakeAgg — счётчики в Redis. Умеет «терять» данные, как при failover.
type fakeAgg struct {
	mu  sync.Mutex
	agg domain.Aggregate
	err error
}

func (f *fakeAgg) Aggregate(context.Context, uuid.UUID, uint16) (domain.Aggregate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return domain.Aggregate{}, f.err
	}
	return f.agg, nil
}

func (f *fakeAgg) set(votes map[uint8]int64, ballots int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.agg = domain.Aggregate{Votes: votes, Ballots: ballots}
}

// fakeResults — Postgres. Upsert моделирует GREATEST, как в настоящей схеме.
type fakeResults struct {
	mu       sync.Mutex
	raw      domain.Aggregate
	adjusted domain.Aggregate
	excluded []string
	saved    int
}

func (f *fakeResults) Upsert(_ context.Context, _ uuid.UUID, a domain.Aggregate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.raw = a.MergeMax(f.raw)
	return nil
}

func (f *fakeResults) Get(context.Context, uuid.UUID) (domain.Aggregate, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.raw, nil
}

func (f *fakeResults) SaveAdjusted(_ context.Context, _ uuid.UUID, a domain.Aggregate, nets []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adjusted, f.excluded, f.saved = a, nets, f.saved+1
	return nil
}

func (f *fakeResults) snapshot() (domain.Aggregate, domain.Aggregate, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.raw, f.adjusted, f.saved
}

type transition struct {
	id uuid.UUID
	to domain.Status
}

type fakePolls struct {
	mu          sync.Mutex
	polls       []*domain.Poll
	transitions []transition
}

func (f *fakePolls) ListActive(context.Context) ([]*domain.Poll, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*domain.Poll, len(f.polls))
	copy(out, f.polls)
	return out, nil
}

func (f *fakePolls) Transition(_ context.Context, id uuid.UUID, to domain.Status, _ uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.transitions = append(f.transitions, transition{id, to})
	for _, p := range f.polls {
		if p.ID == id {
			p.Status = to
		}
	}
	return nil
}

func (f *fakePolls) moves() []transition {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]transition, len(f.transitions))
	copy(out, f.transitions)
	return out
}

type fakeLag struct {
	lag int64
	err error
}

func (f fakeLag) Lag(context.Context) (int64, error) { return f.lag, f.err }

func openPoll() *domain.Poll {
	return &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusOpen,
		OpensAt: opensAt, ClosesAt: closesAt, ShardCount: 500, Version: 1,
	}
}

func newSnapshotter(t *testing.T, agg snapshot.Aggregator, res snapshot.Results, polls snapshot.Polls, lag snapshot.LagReader, now time.Time) *snapshot.Snapshotter {
	t.Helper()

	s, err := snapshot.New(agg, res, polls, lag, snapshot.Config{
		Interval: time.Hour,
		Grace:    30 * time.Second,
		Now:      func() time.Time { return now },
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	return s
}

func TestTickOnce_WritesAggregateToPostgres(t *testing.T) {
	t.Parallel()

	p := openPoll()
	agg := &fakeAgg{}
	agg.set(map[uint8]int64{0: 120, 1: 45}, 165)
	res := &fakeResults{}

	s := newSnapshotter(t, agg, res, &fakePolls{polls: []*domain.Poll{p}}, fakeLag{}, opensAt.Add(time.Second))

	got, err := s.TickOnce(context.Background(), p)
	require.NoError(t, err)
	assert.Equal(t, map[uint8]int64{0: 120, 1: 45}, got.Votes)
	assert.EqualValues(t, 165, got.Ballots)

	raw, _, _ := res.snapshot()
	assert.Equal(t, got.Votes, raw.Votes)
}

// Снапшотер пишет абсолютные значения, поэтому повтор цикла ничего не меняет.
// Пиши он дельты, каждый повтор завышал бы результат.
func TestTickOnce_IsIdempotent(t *testing.T) {
	t.Parallel()

	p := openPoll()
	agg := &fakeAgg{}
	agg.set(map[uint8]int64{0: 100}, 100)
	res := &fakeResults{}

	s := newSnapshotter(t, agg, res, &fakePolls{polls: []*domain.Poll{p}}, fakeLag{}, opensAt.Add(time.Second))

	for range 5 {
		_, err := s.TickOnce(context.Background(), p)
		require.NoError(t, err)
	}

	raw, _, _ := res.snapshot()
	assert.Equal(t, map[uint8]int64{0: 100}, raw.Votes)
	assert.EqualValues(t, 100, raw.Ballots)
}

// Redis теряет часть данных при failover и поднимается с меньшими счётчиками.
// Без максимума цифра в админке уменьшилась бы на глазах у зрителей.
func TestNFR3_SnapshotIsMonotonic(t *testing.T) {
	t.Parallel()

	p := openPoll()
	agg := &fakeAgg{}
	res := &fakeResults{}
	s := newSnapshotter(t, agg, res, &fakePolls{polls: []*domain.Poll{p}}, fakeLag{}, opensAt.Add(time.Second))

	agg.set(map[uint8]int64{0: 1000, 1: 500}, 1500)
	_, err := s.TickOnce(context.Background(), p)
	require.NoError(t, err)

	// Failover: часть данных не доехала до реплики.
	agg.set(map[uint8]int64{0: 900, 1: 600}, 1490)
	got, err := s.TickOnce(context.Background(), p)
	require.NoError(t, err)

	assert.Equal(t, map[uint8]int64{0: 1000, 1: 600}, got.Votes, "счётчик откатился назад")
	assert.EqualValues(t, 1500, got.Ballots)
}

// FSM описывает переход scheduled → open, но выполнять его больше некому:
// без этого опрос, созданный заранее, так и остался бы закрытым.
func TestFR6_ScheduledOpensAutomatically(t *testing.T) {
	t.Parallel()

	p := openPoll()
	p.Status = domain.StatusScheduled

	polls := &fakePolls{polls: []*domain.Poll{p}}
	s := newSnapshotter(t, &fakeAgg{}, &fakeResults{}, polls, fakeLag{}, opensAt.Add(time.Second))

	require.NoError(t, s.Tick(context.Background()))

	moves := polls.moves()
	require.Len(t, moves, 1)
	assert.Equal(t, domain.StatusOpen, moves[0].to)
}

func TestTick_DoesNotOpenBeforeSchedule(t *testing.T) {
	t.Parallel()

	p := openPoll()
	p.Status = domain.StatusScheduled

	polls := &fakePolls{polls: []*domain.Poll{p}}
	s := newSnapshotter(t, &fakeAgg{}, &fakeResults{}, polls, fakeLag{}, opensAt.Add(-time.Minute))

	require.NoError(t, s.Tick(context.Background()))
	assert.Empty(t, polls.moves())
}

// Финализация ждёт нулевого лага, а не таймаута: ненулевой лаг означает, что
// принятые голоса ещё не доехали до Redis, и итог был бы неполным.
func TestFinalize_WaitsForZeroLag(t *testing.T) {
	t.Parallel()

	p := openPoll()
	agg := &fakeAgg{}
	agg.set(map[uint8]int64{0: 10}, 10)
	res := &fakeResults{}
	polls := &fakePolls{polls: []*domain.Poll{p}}

	afterGrace := closesAt.Add(time.Minute)

	// Дренаж ещё идёт.
	busy := newSnapshotter(t, agg, res, polls, fakeLag{lag: 42}, afterGrace)
	require.NoError(t, busy.Tick(context.Background()))

	_, _, saved := res.snapshot()
	assert.Zero(t, saved, "итог зафиксирован до конца дренажа")
	assert.Empty(t, polls.moves())

	// Дренаж закончен.
	done := newSnapshotter(t, agg, res, polls, fakeLag{lag: 0}, afterGrace)
	require.NoError(t, done.Tick(context.Background()))

	_, adjusted, saved := res.snapshot()
	assert.Equal(t, 1, saved)
	assert.EqualValues(t, 10, adjusted.Ballots)

	moves := polls.moves()
	require.Len(t, moves, 1)
	assert.Equal(t, domain.StatusClosed, moves[0].to)
}

// Финальный снимок в момент closes_at потерял бы хвост голосов, ещё летящих
// по сети и лежащих в батчах продюсера.
func TestFinalize_WaitsForGracePeriod(t *testing.T) {
	t.Parallel()

	p := openPoll()
	res := &fakeResults{}
	polls := &fakePolls{polls: []*domain.Poll{p}}

	// Окно закрылось, но grace ещё не истёк.
	s := newSnapshotter(t, &fakeAgg{}, res, polls, fakeLag{lag: 0}, closesAt.Add(time.Second))
	require.NoError(t, s.Tick(context.Background()))

	_, _, saved := res.snapshot()
	assert.Zero(t, saved)
	assert.Empty(t, polls.moves())
}

// Исключения оператора применяются только к публикуемому результату:
// poll_results остаётся монотонной, потому что это аудит подсчёта.
func TestFinalize_ExclusionsGoToAdjustedOnly(t *testing.T) {
	t.Parallel()

	p := openPoll()
	agg := &fakeAgg{}
	agg.set(map[uint8]int64{0: 500}, 500)
	res := &fakeResults{}

	s := newSnapshotter(t, agg, res, &fakePolls{polls: []*domain.Poll{p}}, fakeLag{lag: 0}, closesAt.Add(time.Minute))
	require.NoError(t, s.Finalize(context.Background(), p, []string{"203.0.0.0/16"}))

	raw, _, _ := res.snapshot()
	assert.Equal(t, map[uint8]int64{0: 500}, raw.Votes, "сырой результат не трогается исключениями")
	_, _, saved := res.snapshot()
	assert.Equal(t, 1, saved)
	assert.Equal(t, []string{"203.0.0.0/16"}, res.excluded)
}

func TestTickOnce_PropagatesRedisFailure(t *testing.T) {
	t.Parallel()

	p := openPoll()
	agg := &fakeAgg{err: errors.New("redis недоступен")}

	s := newSnapshotter(t, agg, &fakeResults{}, &fakePolls{polls: []*domain.Poll{p}}, fakeLag{}, opensAt.Add(time.Second))

	_, err := s.TickOnce(context.Background(), p)
	require.Error(t, err)
}
