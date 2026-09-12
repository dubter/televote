package polls_test

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/polls"
	"github.com/dubter/televote/internal/service/polls/mocks"
)

var (
	opensAt  = time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	closesAt = opensAt.Add(time.Minute)
	inWindow = opensAt.Add(15 * time.Second)
)

type fixture struct {
	svc     *polls.Service
	store   *mocks.MockStore
	results *mocks.MockResultStore
	audit   *mocks.MockAuditor
}

func newFixture(t *testing.T, minLeadTime time.Duration) fixture {
	t.Helper()

	ctrl := gomock.NewController(t)
	f := fixture{
		store:   mocks.NewMockStore(ctrl),
		results: mocks.NewMockResultStore(ctrl),
		audit:   mocks.NewMockAuditor(ctrl),
	}

	svc, err := polls.New(f.store, f.results, f.audit, func() time.Time { return inWindow }, minLeadTime,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)
	f.svc = svc
	return f
}

func spec(slug string, pollType domain.PollType, options ...string) domain.PollSpec {
	return domain.PollSpec{
		Slug: slug, Question: "кто победит?", Type: pollType, Options: options,
		MinChoices: 1, MaxChoices: 2,
		OpensAt: inWindow.Add(2 * time.Hour), ClosesAt: inWindow.Add(3 * time.Hour),
		ExpectedAudience: 1000, ExpectedConversion: 0.3,
	}
}

func TestCreate_StoresAScheduledPollAndAudits(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)

	var created *domain.Poll
	f.store.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p *domain.Poll) error { created = p; return nil },
	).Times(1)
	f.audit.EXPECT().Audit(gomock.Any(), "editor-1", "create_poll", "final", gomock.Any()).Return(nil).Times(1)

	poll, err := f.svc.Create(context.Background(), "editor-1", spec("final", domain.PollTypeSingle, "первый", "второй"))
	require.NoError(t, err)

	assert.Same(t, created, poll)
	assert.Equal(t, domain.StatusScheduled, poll.Status, "новый опрос обязан быть scheduled")
	assert.NotZero(t, poll.ShardCount, "нулевой shard_count дал бы деление на ноль на горячем пути")
}

func TestCreate_RejectsInvalidSpecsBeforeTheStore(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.store.EXPECT().Create(gomock.Any(), gomock.Any()).Times(0)

	for name, s := range map[string]domain.PollSpec{
		"неизвестный тип": spec("a", "ranking", "а", "б"),
		"один вариант":    spec("b", domain.PollTypeSingle, "а"),
		"пустые варианты": spec("c", domain.PollTypeSingle),
		"пустой вариант":  spec("d", domain.PollTypeSingle, "а", "  "),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := f.svc.Create(context.Background(), "editor-1", s)
			assert.ErrorIs(t, err, domain.ErrInvalidPoll)
		})
	}
}

func TestCreate_PassesSlugConflictThrough(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.store.EXPECT().Create(gomock.Any(), gomock.Any()).Return(domain.ErrSlugTaken)

	_, err := f.svc.Create(context.Background(), "editor-1", spec("final", domain.PollTypeSingle, "а", "б"))
	assert.ErrorIs(t, err, domain.ErrSlugTaken)
}

func TestCreate_RejectsTooShortLeadTime(t *testing.T) {
	t.Parallel()

	f := newFixture(t, time.Hour)
	f.store.EXPECT().Create(gomock.Any(), gomock.Any()).Times(0)

	s := spec("soon", domain.PollTypeSingle, "а", "б")
	s.OpensAt, s.ClosesAt = inWindow.Add(10*time.Minute), inWindow.Add(11*time.Minute)

	_, err := f.svc.Create(context.Background(), "editor-1", s)
	assert.ErrorIs(t, err, domain.ErrInvalidPoll)
}

func TestOpenAndClose_FollowTheFSM(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusScheduled,
		Options: []domain.Option{{Idx: 0, Text: "а"}}, Version: 1,
		OpensAt: opensAt, ClosesAt: closesAt,
	}
	f := newFixture(t, 0)

	f.store.EXPECT().GetBySlug(gomock.Any(), "final").Return(poll, nil).AnyTimes()
	f.store.EXPECT().Transition(gomock.Any(), poll.ID, domain.StatusOpen, poll.Version).Return(nil).Times(1)
	f.store.EXPECT().CloseNow(gomock.Any(), poll.ID, poll.Version).Return(nil).Times(1)
	gomock.InOrder(
		f.audit.EXPECT().Audit(gomock.Any(), "editor-1", "open_poll", "final", gomock.Any()).Return(nil),
		f.audit.EXPECT().Audit(gomock.Any(), "editor-1", "close_poll", "final", gomock.Any()).Return(nil),
	)

	opened, err := f.svc.Open(context.Background(), "editor-1", "final")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusOpen, opened.Status)

	_, err = f.svc.Open(context.Background(), "editor-1", "final")
	assert.ErrorIs(t, err, domain.ErrBadTransition, "открытый опрос нельзя открыть заново")

	closed, err := f.svc.Close(context.Background(), "editor-1", "final")
	require.NoError(t, err)
	assert.Equal(t, domain.StatusOpen, closed.Status,
		"ручное закрытие двигает closes_at, а не статус: closed выбросил бы опрос из выборки снапшотера")
	assert.Equal(t, inWindow, closed.ClosesAt)
	assert.False(t, closed.Window().IsOpenAt(inWindow), "после ручного закрытия приём закрыт")

	_, err = f.svc.Close(context.Background(), "editor-1", "final")
	assert.ErrorIs(t, err, domain.ErrBadTransition, "закрытый опрос нельзя закрыть дважды")
}

func TestOpen_UnknownPollIsNotFound(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.store.EXPECT().GetBySlug(gomock.Any(), "нет").Return(nil, domain.ErrNotFound)

	_, err := f.svc.Open(context.Background(), "editor-1", "нет")
	assert.ErrorIs(t, err, domain.ErrNotFound)
}

func TestResults_PercentagesAreOfBallotsNotVotes(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusOpen, Version: 1,
		Type:    domain.PollTypeMultiple,
		Options: []domain.Option{{Idx: 0, Text: "а"}, {Idx: 1, Text: "б"}, {Idx: 2, Text: "в"}},
	}
	f := newFixture(t, 0)
	f.store.EXPECT().GetBySlug(gomock.Any(), "final").Return(poll, nil)
	f.results.EXPECT().Get(gomock.Any(), poll.ID).
		Return(domain.NewAggregateFrom(map[uint8]int64{0: 80, 1: 60, 2: 20}, 100), nil)

	out, err := f.svc.Results(context.Background(), "final")
	require.NoError(t, err)

	assert.Same(t, poll, out.Poll)
	assert.EqualValues(t, 100, out.Aggregate.Ballots)
	assert.InDelta(t, 80.0, out.Aggregate.Percent(0), 1e-9)
	assert.InDelta(t, 60.0, out.Aggregate.Percent(1), 1e-9)
	assert.False(t, out.Final, "пока опрос открыт, подсчёт не окончен")
}

func TestResults_ClosedPollIsFinal(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{ID: uuid.New(), Slug: "final", Status: domain.StatusClosed, Version: 1}
	f := newFixture(t, 0)
	f.store.EXPECT().GetBySlug(gomock.Any(), "final").Return(poll, nil)
	f.results.EXPECT().Get(gomock.Any(), poll.ID).Return(domain.NewAggregateFrom(map[uint8]int64{0: 42}, 42), nil)

	out, err := f.svc.Results(context.Background(), "final")
	require.NoError(t, err)
	assert.True(t, out.Final)
}

func TestResults_UnknownPollIsNotFound(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.store.EXPECT().GetBySlug(gomock.Any(), "нет").Return(nil, domain.ErrNotFound)
	f.results.EXPECT().Get(gomock.Any(), gomock.Any()).Times(0)

	_, err := f.svc.Results(context.Background(), "нет")
	assert.ErrorIs(t, err, domain.ErrNotFound)
}

func TestAudit_FailureDoesNotFailTheOperation(t *testing.T) {
	t.Parallel()

	f := newFixture(t, 0)
	f.store.EXPECT().List(gomock.Any()).Return(nil, nil)
	f.store.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil)
	f.audit.EXPECT().Audit(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(assert.AnError)

	_, err := f.svc.Create(context.Background(), "editor-1", spec("final", domain.PollTypeSingle, "а", "б"))
	require.NoError(t, err, "аудит — не причина отказать редактору")

	_, err = f.svc.List(context.Background())
	require.NoError(t, err)
}
