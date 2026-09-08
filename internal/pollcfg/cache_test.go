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

	"github.com/OWNER/televote/internal/domain"
	"github.com/OWNER/televote/internal/pollcfg"
)

// errRepoDown — отказ источника конфига. Отдельная переменная, а не строка на
// месте: тесты сверяют ошибку через errors.Is, и обёртка кэша обязана её
// сохранять — иначе дежурный не отличит падение Postgres от битого конфига.
var errRepoDown = errors.New("postgres лежит")

// fakeRepo — источник конфига без Postgres: реализация того же узкого
// интерфейса, что и storage/postgres.PollRepo.
type fakeRepo struct {
	mu    sync.Mutex
	polls []*domain.Poll
	err   error
	calls atomic.Int64
}

func newFakeRepo(polls ...*domain.Poll) *fakeRepo {
	return &fakeRepo{polls: polls}
}

func (f *fakeRepo) ListActive(ctx context.Context) ([]*domain.Poll, error) {
	f.calls.Add(1)

	// Фейк, игнорирующий ctx, не проверил бы ни таймаут рефрешера, ни
	// остановку по отмене: оба пути ведут себя как мгновенный успех.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	out := make([]*domain.Poll, len(f.polls))
	copy(out, f.polls)
	return out, nil
}

func (f *fakeRepo) serve(polls ...*domain.Poll) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.polls, f.err = polls, nil
}

func (f *fakeRepo) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeRepo) callCount() int64 { return f.calls.Load() }

// blockingRepo висит до отмены ctx — так выглядит запрос к Postgres, который
// не вернётся никогда.
type blockingRepo struct{}

func (blockingRepo) ListActive(ctx context.Context) ([]*domain.Poll, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// logCapture собирает записи slog: рефрешер не имеет права молчать об отказе
// источника, и «не имеет права» проверяется тестом, а не обещанием.
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

// openPoll — опрос в эфире. Множественный выбор взят намеренно: у него
// заполнены и min, и max, поэтому расхождение правил кэша с доменными
// правилами станет видно, а на single-опросе прошло бы незамеченным.
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

// scheduledPoll — опрос, который ещё не открылся. В кэше он обязан быть:
// публичной ручке нужен вопрос до эфира, а приём отвечает на него
// poll_closed (409), а не «нет такого опроса» (404).
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

// runInBackground поднимает рефрешер и гарантирует, что он остановится до
// конца теста: горутина, живущая дольше теста, портит следующий тест.
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

// assertMatchesPoll сверяет конфиг с опросом целиком.
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

	tests := []struct {
		name     string
		repo     pollcfg.Repo
		interval time.Duration
		wantErr  error
	}{
		{name: "источник не задан", repo: nil, interval: time.Second, wantErr: pollcfg.ErrNoRepo},
		{name: "нулевой интервал", repo: newFakeRepo(), interval: 0, wantErr: pollcfg.ErrBadInterval},
		{name: "отрицательный интервал", repo: newFakeRepo(), interval: -time.Second, wantErr: pollcfg.ErrBadInterval},
		{name: "всё на месте", repo: newFakeRepo(), interval: time.Second},
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
	c := newCache(t, newFakeRepo(live, upcoming), time.Hour)
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

// Консьюмер подсчёта получает из Kafka pollID, а не slug: без поиска по ID он
// был бы вынужден обходить всю карту на каждое сообщение.
func TestByID_ReturnsWarmedConfig(t *testing.T) {
	t.Parallel()

	live := openPoll("final")
	c := newCache(t, newFakeRepo(live), time.Hour)
	require.NoError(t, c.Warm(context.Background()))

	got, ok := c.ByID(live.ID)
	require.True(t, ok)
	assertMatchesPoll(t, live, got)

	_, ok = c.ByID(uuid.New())
	assert.False(t, ok, "неизвестный ID не имеет права вернуть чужой конфиг")
}

// Холодный кэш обязан отвечать «нет», а не паниковать: до Warm инстанс не
// проходит readiness, но /readyz и голос могут прийти в одну и ту же секунду.
func TestBySlug_ColdCacheReturnsNotFound(t *testing.T) {
	t.Parallel()

	c := newCache(t, newFakeRepo(openPoll("final")), time.Hour)

	got, ok := c.BySlug("final")
	assert.False(t, ok)
	assert.Nil(t, got)
	assert.True(t, c.LastRefresh().IsZero(), "непрогретый кэш не имеет времени обновления")
}

// Холодный старт не проходит readiness: инстанс с пустым кэшем ответил бы 404
// на живой опрос.
func TestWarm_ErrorsWhenRepoFails(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo(openPoll("final"))
	repo.fail(errRepoDown)

	c := newCache(t, repo, time.Hour)

	err := c.Warm(context.Background())
	require.ErrorIs(t, err, errRepoDown)

	_, ok := c.BySlug("final")
	assert.False(t, ok, "провалившийся прогрев не имеет права выглядеть успешным")
	assert.True(t, c.LastRefresh().IsZero())
}

// Прогрев обязан быть ограничен по времени: зависший запрос к Postgres иначе
// держал бы старт инстанса вечно, а рефрешер — навсегда на прежнем снимке.
func TestWarm_TimesOutOnHangingRepo(t *testing.T) {
	t.Parallel()

	c := newCache(t, blockingRepo{}, time.Hour, pollcfg.WithRefreshTimeout(20*time.Millisecond))

	start := time.Now()
	err := c.Warm(context.Background())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second, "таймаут запроса не сработал")
}

func TestWarm_SkipsUnusablePolls(t *testing.T) {
	t.Parallel()

	noSlug := openPoll("")
	noID := openPoll("no-id")
	noID.ID = uuid.Nil
	good := openPoll("final")

	c := newCache(t, newFakeRepo(nil, noSlug, noID, good), time.Hour)
	require.NoError(t, c.Warm(context.Background()))

	got, ok := c.BySlug("final")
	require.True(t, ok, "негодные строки не имеют права уронить прогрев целиком")
	assertMatchesPoll(t, good, got)

	_, ok = c.BySlug("")
	assert.False(t, ok, "конфиг под пустым ключом ответил бы на запрос без slug")

	_, ok = c.ByID(uuid.Nil)
	assert.False(t, ok, "нулевой ID склеил бы разные опросы в одну запись")
}

// Снимок публикуется читателям без блокировок, поэтому не имеет права
// смотреть в срезы строки опроса: правка строки после прогрева стала бы
// гонкой, которую не покажет ни лог, ни падение.
func TestWarm_SnapshotDoesNotAliasPoll(t *testing.T) {
	t.Parallel()

	p := openPoll("final")
	c := newCache(t, newFakeRepo(p), time.Hour)
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
	repo := newFakeRepo(live)
	c := newCache(t, repo, 5*time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	added := openPoll("halftime")
	repo.serve(live, added)
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

// Опрос, ушедший из выборки активных, обязан исчезнуть из кэша: рефрешер
// подменяет снимок целиком, а не доливает в него.
func TestRun_DropsPollThatLeftActiveSet(t *testing.T) {
	t.Parallel()

	live, ended := openPoll("final"), openPoll("last-year")
	repo := newFakeRepo(live, ended)
	c := newCache(t, repo, 5*time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	repo.serve(live)
	runInBackground(t, c)

	require.Eventually(t, func() bool {
		_, ok := c.BySlug("last-year")
		return !ok
	}, 2*time.Second, 2*time.Millisecond, "снимок подменяется целиком, а не дополняется")

	_, ok := c.ByID(ended.ID)
	assert.False(t, ok, "поиск по ID обязан обновляться вместе с поиском по slug")
}

// Падение Postgres не имеет права остановить голосование: кэш продолжает
// отдавать прежний снимок, а не пустоту.
func TestRun_KeepsStaleConfigWhenRepoFails(t *testing.T) {
	t.Parallel()

	live := openPoll("final")
	repo := newFakeRepo(live)
	logs := &logCapture{}
	c := newCache(t, repo, 5*time.Millisecond, pollcfg.WithLogger(logs.logger()))
	require.NoError(t, c.Warm(context.Background()))

	warmedAt := c.LastRefresh()
	require.False(t, warmedAt.IsZero())

	repo.fail(errRepoDown)
	before := repo.callCount()
	runInBackground(t, c)

	require.Eventually(t, func() bool {
		return repo.callCount() >= before+3
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
	repo := newFakeRepo(live)
	c := newCache(t, repo, 5*time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	warmedAt := c.LastRefresh()
	repo.fail(errRepoDown)
	runInBackground(t, c)

	added := openPoll("halftime")
	repo.serve(live, added)

	require.Eventually(t, func() bool {
		_, ok := c.BySlug("halftime")
		return ok
	}, 2*time.Second, 2*time.Millisecond, "кэш не восстановился после возврата источника")

	assert.True(t, c.LastRefresh().After(warmedAt), "удачное обновление обязано двигать LastRefresh")
}

// Инвариант из CLAUDE.md: конфиг опроса — фоновый рефрешер, а не ленивый TTL.
// Истечение TTL при 2M RPS дало бы thundering herd из тысяч одновременных
// промахов в Postgres, который на горячем пути вообще не должен появляться.
func TestPollCfg_NoIOOnHotPath(t *testing.T) {
	t.Parallel()

	repo := newFakeRepo(openPoll("final"))
	c := newCache(t, repo, time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))

	afterWarm := repo.callCount()
	require.Equal(t, int64(1), afterWarm, "прогрев — единственное обращение к источнику")

	time.Sleep(20 * time.Millisecond)

	for range 10_000 {
		cfg, ok := c.BySlug("final")
		require.True(t, ok)
		require.NotNil(t, cfg)
		_ = c.LastRefresh()
	}

	assert.Equal(t, afterWarm, repo.callCount(),
		"чтение конфига обязано ходить только в память: ни Postgres, ни сети на горячем пути")
}

// 100 читателей против рефрешера, подменяющего снимок: под -race это
// единственный способ показать, что чтение конфига действительно
// неблокирующее и не разъезжается с обновлением.
func TestCache_RaceFree(t *testing.T) {
	t.Parallel()

	const (
		readers        = 100
		readsPerReader = 500
	)

	stable := openPoll("final")
	repo := newFakeRepo(stable)
	c := newCache(t, repo, time.Millisecond)
	require.NoError(t, c.Warm(context.Background()))
	runInBackground(t, c)

	// Источник меняет выборку под рефрешером: так снимки подменяются
	// по-настоящему, а не переставляются одни и те же указатели.
	churn := make(chan struct{})
	go func() {
		defer close(churn)
		for i := range 200 {
			repo.serve(stable, openPoll(fmt.Sprintf("extra-%d", i)))
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
				// Поля читаются, а не игнорируются: гонку ловит именно
				// доступ к содержимому снимка.
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

// Соль вывода voter_id — несущий элемент приватности: с пустым ключом HMAC
// даёт связуемый между опросами и подделываемый voterID. Кэш обязан сообщать
// о таком опросе, а не отдавать его молча.
func TestWarm_WarnsAboutPollWithoutSalt(t *testing.T) {
	t.Parallel()

	logs := &logCapture{}
	c := newCache(t, newFakeRepo(openPoll("final")), time.Hour, pollcfg.WithLogger(logs.logger()))
	require.NoError(t, c.Warm(context.Background()))

	assert.True(t, logs.contains(slog.LevelWarn, "final"),
		"опрос без соли обязан быть назван в логе")
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	t.Parallel()

	c := newCache(t, newFakeRepo(openPoll("final")), time.Millisecond)

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
