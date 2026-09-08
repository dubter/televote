package pollcfg_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/pollcfg"
	"github.com/dubter/televote/internal/service/pollcfg/mocks"
)

var errRepoDown = errors.New("postgres лежит")

type source struct {
	mu    sync.Mutex
	polls []*domain.Poll
	err   error
	calls atomic.Int64
}

func (s *source) listActive(ctx context.Context) ([]*domain.Poll, error) {
	s.calls.Add(1)

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return nil, s.err
	}

	out := make([]*domain.Poll, len(s.polls))
	copy(out, s.polls)
	return out, nil
}

func (s *source) serve(polls ...*domain.Poll) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.polls, s.err = polls, nil
}

func (s *source) fail(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *source) callCount() int64 { return s.calls.Load() }

func newRepo(t *testing.T, polls ...*domain.Poll) (*mocks.MockRepo, *source) {
	t.Helper()

	s := &source{polls: polls}
	m := mocks.NewMockRepo(gomock.NewController(t))
	m.EXPECT().ListActive(gomock.Any()).DoAndReturn(s.listActive).AnyTimes()
	return m, s
}

func newBlockingRepo(t *testing.T) *mocks.MockRepo {
	t.Helper()

	m := mocks.NewMockRepo(gomock.NewController(t))
	m.EXPECT().ListActive(gomock.Any()).DoAndReturn(
		func(ctx context.Context) ([]*domain.Poll, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	).AnyTimes()
	return m
}

type logCapture struct {
	mu    sync.Mutex
	lines []string
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }

func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", r.Level, r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	})

	c.mu.Lock()
	defer c.mu.Unlock()
	c.lines = append(c.lines, b.String())
	return nil
}

func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

func (c *logCapture) contains(level slog.Level, substr string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, line := range c.lines {
		if strings.HasPrefix(line, level.String()) && strings.Contains(line, substr) {
			return true
		}
	}
	return false
}

func (c *logCapture) logger() *slog.Logger { return slog.New(c) }

func openPoll(slug string) *domain.Poll {
	now := time.Now()
	return &domain.Poll{
		ID:       uuid.New(),
		Slug:     slug,
		Question: "кто победит в финале?",
		Type:     domain.PollTypeMultiple,
		Options: []domain.Option{
			{Idx: 0, Text: "первый"},
			{Idx: 1, Text: "второй"},
			{Idx: 2, Text: "третий"},
		},
		MinChoices: 1,
		MaxChoices: 2,
		Status:     domain.StatusOpen,
		OpensAt:    now.Add(-time.Minute),
		ClosesAt:   now.Add(time.Minute),
		ShardCount: domain.ShardCountFor(6),
		Version:    3,
	}
}

func scheduledPoll(slug string) *domain.Poll {
	p := openPoll(slug)
	p.ID = uuid.New()
	p.Status = domain.StatusScheduled
	p.OpensAt = time.Now().Add(time.Hour)
	p.ClosesAt = p.OpensAt.Add(time.Minute)
	return p
}

func newCache(t *testing.T, r pollcfg.Repo, interval time.Duration, opts ...pollcfg.Option) *pollcfg.Cache {
	t.Helper()

	c, err := pollcfg.NewCache(r, interval, opts...)
	require.NoError(t, err)
	return c
}

func runInBackground(t *testing.T, c *pollcfg.Cache) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("рефрешер не остановился после отмены контекста")
		}
	})
}

func assertMatchesPoll(t *testing.T, want *domain.Poll, got *pollcfg.HotConfig) {
	t.Helper()

	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.Slug, got.Slug)
	assert.Equal(t, want.Question, got.Question, "публичной ручке нужен вопрос")
	assert.Equal(t, want.Options, got.Options, "публичной ручке нужны опции")
	assert.Equal(t, want.ChoiceRules(), got.Rules, "правило выбора обязано быть доменным")
	assert.Equal(t, want.Window(), got.Window, "окно и статус нужны приёму")
	assert.Equal(t, want.ShardCount, got.ShardCount, "shard_count берётся из строки опроса")
}

func TestNewCache_RejectsUnusableDependencies(t *testing.T) {
	t.Parallel()

	repo, _ := newRepo(t)

	tests := []struct {
		name     string
		repo     pollcfg.Repo
		interval time.Duration
		wantErr  error
	}{
		{name: "источник не задан", repo: nil, interval: time.Second, wantErr: pollcfg.ErrNoRepo},
		{name: "нулевой интервал", repo: repo, interval: 0, wantErr: pollcfg.ErrBadInterval},
		{name: "отрицательный интервал", repo: repo, interval: -time.Second, wantErr: pollcfg.ErrBadInterval},
		{name: "всё на месте", repo: repo, interval: time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := pollcfg.NewCache(tc.repo, tc.interval)
			if tc.wantErr == nil {
				require.NoError(t, err)
				require.NotNil(t, c)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
			assert.Nil(t, c, "полуготовый кэш хуже отказа: он тихо отдаёт пустой конфиг")
		})
	}
}

func TestBySlug_ReturnsWarmedConfig(t *testing.T) {
	t.Parallel()

	live, upcoming := openPoll("final"), scheduledPoll("semifinal")
	repo, _ := newRepo(t, live, upcoming)
	c := newCache(t, repo, time.Hour)
	require.NoError(t, c.Warm(context.Background()))

	tests := []struct {
		name string
		slug string
		want *domain.Poll
	}{
		{name: "опрос в эфире", slug: "final", want: live},
		{name: "опрос до открытия остаётся в кэше", slug: "semifinal", want: upcoming},
		{name: "неизвестный slug", slug: "нет-такого"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := c.BySlug(tc.slug)
			if tc.want == nil {
				assert.False(t, ok)
				assert.Nil(t, got)
				return
			}
			require.True(t, ok)
			assertMatchesPoll(t, tc.want, got)
		})
	}
}

func TestBySlug_ColdCacheReturnsNotFound(t *testing.T) {
	t.Parallel()

	repo, _ := newRepo(t, openPoll("final"))
	c := newCache(t, repo, time.Hour)

	got, ok := c.BySlug("final")
	assert.False(t, ok)
	assert.Nil(t, got)
	assert.True(t, c.LastRefresh().IsZero(), "непрогретый кэш не имеет времени обновления")
}

func TestWarm_ErrorsWhenRepoFails(t *testing.T) {
	t.Parallel()

	repo, src := newRepo(t, openPoll("final"))
	src.fail(errRepoDown)

	c := newCache(t, repo, time.Hour)

	err := c.Warm(context.Background())
	require.ErrorIs(t, err, errRepoDown)

	_, ok := c.BySlug("final")
	assert.False(t, ok, "провалившийся прогрев не имеет права выглядеть успешным")
	assert.True(t, c.LastRefresh().IsZero())
}

func TestWarm_TimesOutOnHangingRepo(t *testing.T) {
	t.Parallel()

	c := newCache(t, newBlockingRepo(t), time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.Warm(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second, "зависший источник не имеет права держать старт инстанса")
}

func TestWarm_SkipsUnusablePolls(t *testing.T) {
	t.Parallel()

	noSlug := openPoll("")
	noID := openPoll("no-id")
	noID.ID = uuid.Nil
	good := openPoll("final")

	repo, _ := newRepo(t, nil, noSlug, noID, good)
	c := newCache(t, repo, time.Hour)
	require.NoError(t, c.Warm(context.Background()))

	got, ok := c.BySlug("final")
	require.True(t, ok, "негодные строки не имеют права уронить прогрев целиком")
	assertMatchesPoll(t, good, got)

	byID, ok := c.ByID(good.ID)
	require.True(t, ok, "консьюмер ищет конфиг по ID, а не по slug")
	assert.Equal(t, got, byID)

	_, ok = c.BySlug("")
	assert.False(t, ok, "конфиг под пустым ключом ответил бы на запрос без slug")

	_, ok = c.ByID(uuid.Nil)
	assert.False(t, ok, "нулевой ID склеил бы разные опросы в одну запись")
}

func TestWarm_SnapshotDoesNotAliasPoll(t *testing.T) {
	t.Parallel()

	p := openPoll("final")
	repo, _ := newRepo(t, p)
	c := newCache(t, repo, time.Hour)
	require.NoError(t, c.Warm(context.Background()))

	got, ok := c.BySlug("final")
	require.True(t, ok)

	p.Options[0].Text = "подменённый вариант"
	p.Options = append(p.Options, domain.Option{Idx: 3, Text: "четвёртый"})

	assert.Equal(t, "первый", got.Options[0].Text)
	assert.Len(t, got.Options, 3)
}

func TestRun_PicksUpNewPoll(t *testing.T) {
	t.Parallel()

	live := openPoll("final")
	repo, src := newRepo(t, live)
	c := newCache(t, repo, 5*time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	added := openPoll("halftime")
	src.serve(live, added)
	runInBackground(t, c)

	require.Eventually(t, func() bool {
		_, ok := c.BySlug("halftime")
		return ok
	}, 2*time.Second, 2*time.Millisecond, "рефрешер не подхватил новый опрос")

	got, ok := c.BySlug("halftime")
	require.True(t, ok)
	assertMatchesPoll(t, added, got)

	_, ok = c.BySlug("final")
	assert.True(t, ok, "обновление не имеет права потерять прежние опросы")
}

func TestRun_DropsPollThatLeftActiveSet(t *testing.T) {
	t.Parallel()

	live, ended := openPoll("final"), openPoll("last-year")
	repo, src := newRepo(t, live, ended)
	c := newCache(t, repo, 5*time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	src.serve(live)
	runInBackground(t, c)

	require.Eventually(t, func() bool {
		_, ok := c.BySlug("last-year")
		return !ok
	}, 2*time.Second, 2*time.Millisecond, "снимок подменяется целиком, а не дополняется")

	_, ok := c.ByID(ended.ID)
	assert.False(t, ok, "поиск по ID обязан обновляться вместе с поиском по slug")
}

func TestRun_KeepsStaleConfigWhenRepoFails(t *testing.T) {
	t.Parallel()

	live := openPoll("final")
	repo, src := newRepo(t, live)
	logs := &logCapture{}
	c := newCache(t, repo, 5*time.Millisecond, pollcfg.WithLogger(logs.logger()))
	require.NoError(t, c.Warm(context.Background()))

	warmedAt := c.LastRefresh()
	require.False(t, warmedAt.IsZero())

	src.fail(errRepoDown)
	before := src.callCount()
	runInBackground(t, c)

	require.Eventually(t, func() bool {
		return src.callCount() >= before+3
	}, 2*time.Second, 2*time.Millisecond, "рефрешер перестал ходить к источнику")

	got, ok := c.BySlug("final")
	require.True(t, ok, "отказ источника не имеет права обнулить конфиг")
	assertMatchesPoll(t, live, got)

	assert.Equal(t, warmedAt, c.LastRefresh(),
		"LastRefresh — метка последнего УДАЧНОГО обновления, иначе по ней нельзя увидеть застой")
	assert.True(t, logs.contains(slog.LevelWarn, "postgres лежит"),
		"молчаливый отказ рефрешера — это и есть тихий отказ из CLAUDE.md")
}

func TestRun_ResumesAfterRepoRecovers(t *testing.T) {
	t.Parallel()

	live := openPoll("final")
	repo, src := newRepo(t, live)
	c := newCache(t, repo, 5*time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	warmedAt := c.LastRefresh()
	src.fail(errRepoDown)
	runInBackground(t, c)

	added := openPoll("halftime")
	src.serve(live, added)

	require.Eventually(t, func() bool {
		_, ok := c.BySlug("halftime")
		return ok
	}, 2*time.Second, 2*time.Millisecond, "кэш не восстановился после возврата источника")

	assert.True(t, c.LastRefresh().After(warmedAt), "удачное обновление обязано двигать LastRefresh")
}

func TestPollCfg_NoIOOnHotPath(t *testing.T) {
	t.Parallel()

	repo, src := newRepo(t, openPoll("final"))
	c := newCache(t, repo, time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	afterWarm := src.callCount()
	require.Equal(t, int64(1), afterWarm, "прогрев — единственное обращение к источнику")

	time.Sleep(20 * time.Millisecond)

	for range 10_000 {
		cfg, ok := c.BySlug("final")
		require.True(t, ok)
		require.NotNil(t, cfg)
		_ = c.LastRefresh()
	}

	assert.Equal(t, afterWarm, src.callCount(),
		"чтение конфига обязано ходить только в память: ни Postgres, ни сети на горячем пути")
}

func TestCache_RaceFree(t *testing.T) {
	t.Parallel()

	const (
		readers        = 100
		readsPerReader = 500
	)

	stable := openPoll("final")
	repo, src := newRepo(t, stable)
	c := newCache(t, repo, time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))
	runInBackground(t, c)

	churn := make(chan struct{})
	go func() {
		defer close(churn)
		for i := range 200 {
			src.serve(stable, openPoll(fmt.Sprintf("extra-%d", i)))
		}
	}()

	var misses, mismatches atomic.Int64
	var wg sync.WaitGroup
	wg.Add(readers)
	for range readers {
		go func() {
			defer wg.Done()
			for range readsPerReader {
				cfg, ok := c.BySlug("final")
				if !ok {
					misses.Add(1)
					continue
				}
				if cfg.ID != stable.ID || len(cfg.Options) != len(stable.Options) ||
					cfg.Rules != stable.ChoiceRules() || cfg.Window != stable.Window() {
					mismatches.Add(1)
				}
				_ = c.LastRefresh()
			}
		}()
	}
	wg.Wait()
	<-churn

	assert.Zero(t, misses.Load(), "опубликованный конфиг не имеет права пропадать между обновлениями")
	assert.Zero(t, mismatches.Load(), "читатель увидел полуобновлённый снимок")
}

func TestWarm_WarnsAboutPollWithoutSalt(t *testing.T) {
	t.Parallel()

	logs := &logCapture{}
	repo, _ := newRepo(t, openPoll("final"))
	c := newCache(t, repo, time.Hour, pollcfg.WithLogger(logs.logger()))
	require.NoError(t, c.Warm(context.Background()))

	assert.True(t, logs.contains(slog.LevelWarn, "final"),
		"опрос без соли обязан быть назван в логе")
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	repo, _ := newRepo(t, openPoll("final"))
	c := newCache(t, repo, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run не вернулся после отмены контекста: graceful shutdown повиснет")
	}
}
