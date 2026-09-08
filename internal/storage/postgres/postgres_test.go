//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/storage/postgres"
)

// startPostgres поднимает базу и накатывает схему.
func startPostgres(t *testing.T) *postgres.PollRepo {
	t.Helper()
	polls, _, _ := startAll(t)
	return polls
}

func startAll(t *testing.T) (*postgres.PollRepo, *postgres.ResultRepo, *postgres.AdminRepo) {
	t.Helper()

	ctx := context.Background()
	container, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("televote"),
		tcpostgres.WithUsername("televote"),
		tcpostgres.WithPassword("televote"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(ctx) })

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	require.NoError(t, err)

	pool, err := postgres.NewPool(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	applySchema(ctx, t, pool)

	polls, err := postgres.NewPollRepo(pool)
	require.NoError(t, err)
	results, err := postgres.NewResultRepo(pool)
	require.NoError(t, err)
	admins, err := postgres.NewAdminRepo(pool)
	require.NoError(t, err)

	return polls, results, admins
}

func newPoll(slug string) *domain.Poll {
	now := time.Now().UTC().Truncate(time.Second)
	return &domain.Poll{
		ID:       uuid.New(),
		Slug:     slug,
		Question: "кто победит?",
		Type:     domain.PollTypeSingle,
		Options: []domain.Option{
			{Idx: 0, Text: "первый"}, {Idx: 1, Text: "второй"},
		},
		MinChoices: 1, MaxChoices: 1,
		Status:  domain.StatusScheduled,
		OpensAt: now.Add(time.Hour), ClosesAt: now.Add(time.Hour + time.Minute),
		ShardCount:         500,
		ExpectedAudience:   1_000_000,
		ExpectedConversion: 0.3,
		Version:            1,
	}
}

func TestCreate_PollAndOptionsAreStoredTogether(t *testing.T) {
	polls := startPostgres(t)
	ctx := context.Background()

	p := newPoll("final")
	require.NoError(t, polls.Create(ctx, p))

	got, err := polls.GetBySlug(ctx, "final")
	require.NoError(t, err)

	assert.Equal(t, p.ID, got.ID)
	assert.Equal(t, p.Question, got.Question)
	assert.Equal(t, p.Options, got.Options, "опции обязаны читаться вместе с опросом")
	assert.Equal(t, p.ShardCount, got.ShardCount, "shard_count берётся из строки опроса")
	assert.EqualValues(t, 1_000_000, got.ExpectedAudience)
}

// Соль генерирует сервер: принимать её извне значило бы позволить вызывающему
// подать слабую или общую для нескольких опросов.
func TestCreate_GeneratesUniqueSalt(t *testing.T) {
	polls := startPostgres(t)
	ctx := context.Background()

	first, second := newPoll("a"), newPoll("b")
	require.NoError(t, polls.Create(ctx, first))
	require.NoError(t, polls.Create(ctx, second))

	a, err := polls.GetBySlug(ctx, "a")
	require.NoError(t, err)
	b, err := polls.GetBySlug(ctx, "b")
	require.NoError(t, err)

	assert.NotEmpty(t, a.Salt)
	assert.Len(t, a.Salt, postgres.SaltLen)
	assert.NotEqual(t, a.Salt, b.Salt,
		"общая соль сделала бы voterID сопоставимыми между опросами")
}

// Слаг попадает в публичную ссылку, поэтому уникальность обеспечивает БД,
// а не пара SELECT+INSERT: между ними успевает вклиниться второй админ.
func TestCreate_DuplicateSlug(t *testing.T) {
	polls := startPostgres(t)
	ctx := context.Background()

	require.NoError(t, polls.Create(ctx, newPoll("final")))
	err := polls.Create(ctx, newPoll("final"))
	require.ErrorIs(t, err, postgres.ErrSlugTaken)
}

func TestGetBySlug_UnknownPoll(t *testing.T) {
	polls := startPostgres(t)

	_, err := polls.GetBySlug(context.Background(), "нет-такого")
	require.ErrorIs(t, err, postgres.ErrNotFound)
}

// Оптимистическая блокировка: два админа не должны затирать правки друг друга.
func TestTransition_VersionConflict(t *testing.T) {
	polls := startPostgres(t)
	ctx := context.Background()

	p := newPoll("final")
	require.NoError(t, polls.Create(ctx, p))

	require.NoError(t, polls.Transition(ctx, p.ID, domain.StatusOpen, p.Version))

	// Та же версия второй раз: строку уже изменили.
	err := polls.Transition(ctx, p.ID, domain.StatusClosed, p.Version)
	require.ErrorIs(t, err, postgres.ErrVersionConflict)
}

// ListActive отдаёт scheduled и open: приём должен знать конфигурацию опроса
// ещё до открытия, иначе первые голоса получат «неизвестный опрос».
func TestListActive_IncludesScheduledExcludesClosed(t *testing.T) {
	polls := startPostgres(t)
	ctx := context.Background()

	scheduled, closed := newPoll("scheduled"), newPoll("closed")
	require.NoError(t, polls.Create(ctx, scheduled))
	require.NoError(t, polls.Create(ctx, closed))
	require.NoError(t, polls.Transition(ctx, closed.ID, domain.StatusOpen, closed.Version))
	require.NoError(t, polls.Transition(ctx, closed.ID, domain.StatusClosed, closed.Version+1))

	active, err := polls.ListActive(ctx)
	require.NoError(t, err)

	slugs := map[string]bool{}
	for _, p := range active {
		slugs[p.Slug] = true
	}
	assert.True(t, slugs["scheduled"])
	assert.False(t, slugs["closed"], "закрытый опрос не имеет права остаться в кэше приёма")
}

// Снапшотер пишет абсолютные значения, а Redis после failover может подняться
// с меньшими счётчиками. Без GREATEST цифра в админке уменьшилась бы на глазах.
func TestUpsert_IsMonotonic(t *testing.T) {
	polls, results, _ := startAll(t)
	ctx := context.Background()

	p := newPoll("final")
	require.NoError(t, polls.Create(ctx, p))

	require.NoError(t, results.Upsert(ctx, p.ID,
		domain.Aggregate{Votes: map[uint8]int64{0: 1000, 1: 500}, Ballots: 1500}))

	// Потеря данных при failover: пришли меньшие значения.
	require.NoError(t, results.Upsert(ctx, p.ID,
		domain.Aggregate{Votes: map[uint8]int64{0: 900, 1: 600}, Ballots: 1490}))

	got, err := results.Get(ctx, p.ID)
	require.NoError(t, err)

	assert.Equal(t, map[uint8]int64{0: 1000, 1: 600}, got.Votes, "счётчик откатился назад")
	assert.EqualValues(t, 1500, got.Ballots)
}

// Монотонность делает безопасной одновременную работу двух снапшотеров:
// лидер-элекшн становится оптимизацией, а не требованием корректности.
func TestUpsert_ConcurrentSnapshottersAreSafe(t *testing.T) {
	polls, results, _ := startAll(t)
	ctx := context.Background()

	p := newPoll("final")
	require.NoError(t, polls.Create(ctx, p))

	done := make(chan error, 2)
	for i := range 2 {
		go func(shift int64) {
			var err error
			for n := int64(1); n <= 20; n++ {
				err = results.Upsert(ctx, p.ID, domain.Aggregate{
					Votes: map[uint8]int64{0: n * 10}, Ballots: n*10 + shift,
				})
				if err != nil {
					break
				}
			}
			done <- err
		}(int64(i))
	}
	require.NoError(t, <-done)
	require.NoError(t, <-done)

	got, err := results.Get(ctx, p.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 200, got.Votes[0], "итог обязан быть максимумом, а не последней записью")
}

// poll_results — журнал подсчёта и остаётся монотонным; исключения оператора
// применяются только к публикуемому результату.
func TestSaveAdjusted_DoesNotTouchRawResults(t *testing.T) {
	polls, results, _ := startAll(t)
	ctx := context.Background()

	p := newPoll("final")
	require.NoError(t, polls.Create(ctx, p))
	require.NoError(t, results.Upsert(ctx, p.ID,
		domain.Aggregate{Votes: map[uint8]int64{0: 500}, Ballots: 500}))

	require.NoError(t, results.SaveAdjusted(ctx, p.ID,
		domain.Aggregate{Votes: map[uint8]int64{0: 460}, Ballots: 460}, []string{"203.0.0.0/16"}))

	raw, err := results.Get(ctx, p.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 500, raw.Ballots, "сырой результат не трогается исключениями")

	adjusted, nets, err := results.GetAdjusted(ctx, p.ID)
	require.NoError(t, err)
	assert.EqualValues(t, 460, adjusted.Ballots)
	assert.Equal(t, []string{"203.0.0.0/16"}, nets)
}

func TestAdminRepo_EnsureAndAudit(t *testing.T) {
	_, _, admins := startAll(t)
	ctx := context.Background()

	created, err := admins.EnsureAdmin(ctx, postgres.Admin{
		Login: "admin", PasswordHash: "$argon2id$fake", Role: "admin",
	})
	require.NoError(t, err)
	assert.True(t, created)

	// Повторный вызов не должен ни падать, ни перезаписывать пароль.
	again, err := admins.EnsureAdmin(ctx, postgres.Admin{
		Login: "admin", PasswordHash: "$argon2id$other", Role: "admin",
	})
	require.NoError(t, err)
	assert.False(t, again)

	got, err := admins.ByLogin(ctx, "admin")
	require.NoError(t, err)
	assert.Equal(t, "$argon2id$fake", got.PasswordHash, "пароль существующего админа не перезаписывается")

	require.NoError(t, admins.Audit(ctx, got.ID.String(), "close_poll", "final", map[string]any{"to": "closed"}))

	entries, err := admins.ListAudit(ctx, "final", 10)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "close_poll", entries[0].Action)
}

func TestAdminRepo_UnknownLogin(t *testing.T) {
	_, _, admins := startAll(t)

	_, err := admins.ByLogin(context.Background(), "нет-такого")
	require.ErrorIs(t, err, postgres.ErrNotFound)
}
