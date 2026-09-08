package domain_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
)

func options(n int) []domain.Option {
	out := make([]domain.Option, 0, n)
	for i := range n {
		out = append(out, domain.Option{Idx: uint8(i), Text: fmt.Sprintf("вариант %d", i)})
	}
	return out
}

func singlePoll(optionCount int) *domain.Poll {
	return &domain.Poll{
		ID:      uuid.New(),
		Slug:    "single",
		Type:    domain.PollTypeSingle,
		Options: options(optionCount),
		Status:  domain.StatusOpen,
	}
}

func multiplePoll(optionCount int, minChoices, maxChoices uint8) *domain.Poll {
	return &domain.Poll{
		ID:         uuid.New(),
		Slug:       "multiple",
		Type:       domain.PollTypeMultiple,
		Options:    options(optionCount),
		MinChoices: minChoices,
		MaxChoices: maxChoices,
		Status:     domain.StatusOpen,
	}
}

func TestNewAggregate_IsReadyForWrites(t *testing.T) {
	t.Parallel()

	a := domain.NewAggregate()
	a.Add(2, 1)
	a.Add(2, 3)

	assert.Equal(t, map[uint8]int64{2: 4}, a.Votes)
}

func TestNewAggregateFrom_CopiesInput(t *testing.T) {
	t.Parallel()

	src := map[uint8]int64{0: 10, 1: 5}
	a := domain.NewAggregateFrom(src, 15)

	src[0] = 999
	delete(src, 1)

	assert.Equal(t, map[uint8]int64{0: 10, 1: 5}, a.Votes)
	assert.EqualValues(t, 15, a.Ballots)
}

func TestAggregate_MergeMaxNeverGoesBackwards(t *testing.T) {
	t.Parallel()

	prev := domain.NewAggregateFrom(map[uint8]int64{0: 100, 1: 50}, 140)
	fresh := domain.NewAggregateFrom(map[uint8]int64{0: 90, 1: 60}, 138)

	got := fresh.MergeMax(prev)

	assert.Equal(t, map[uint8]int64{0: 100, 1: 60}, got.Votes)
	assert.Equal(t, int64(140), got.Ballots)
}

func TestAggregate_MergeMaxKeepsOptionsMissingFromFreshRead(t *testing.T) {
	t.Parallel()

	prev := domain.NewAggregateFrom(map[uint8]int64{0: 5, 7: 3}, 8)
	fresh := domain.NewAggregateFrom(map[uint8]int64{0: 6}, 6)

	got := fresh.MergeMax(prev)

	assert.Equal(t, map[uint8]int64{0: 6, 7: 3}, got.Votes)
	assert.Equal(t, int64(8), got.Ballots)
}

func TestAggregate_MergeMaxIsIdempotent(t *testing.T) {
	t.Parallel()

	a := domain.NewAggregateFrom(map[uint8]int64{0: 4, 1: 9}, 12)

	once := a.MergeMax(a)
	twice := once.MergeMax(a)

	assert.Equal(t, a.Votes, once.Votes)
	assert.Equal(t, a.Ballots, once.Ballots)
	assert.Equal(t, once.Votes, twice.Votes)
}

func TestAggregate_PercentIsShareOfBallotsNotVotes(t *testing.T) {
	t.Parallel()

	a := domain.NewAggregateFrom(map[uint8]int64{0: 80, 1: 60, 2: 20}, 100)

	require.Greater(t, a.Votes[0]+a.Votes[1]+a.Votes[2], a.Ballots, "фикстура обязана быть множественным выбором")
	assert.InDelta(t, 80.0, a.Percent(0), 1e-9)
	assert.InDelta(t, 60.0, a.Percent(1), 1e-9)
	assert.InDelta(t, 20.0, a.Percent(2), 1e-9)
}

func TestAggregate_PercentOnEmptyPollIsZero(t *testing.T) {
	t.Parallel()

	assert.InDelta(t, 0.0, domain.NewAggregate().Percent(0), 1e-9)
	assert.InDelta(t, 0.0, domain.NewAggregateFrom(map[uint8]int64{0: 5}, 0).Percent(0), 1e-9)
	assert.InDelta(t, 0.0, domain.NewAggregateFrom(nil, 10).Percent(3), 1e-9)
}

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

func TestPoll_ExpectedVotes(t *testing.T) {
	t.Parallel()

	p := &domain.Poll{ExpectedAudience: 100_000_000, ExpectedConversion: 0.3}
	assert.EqualValues(t, 30_000_000, p.ExpectedVotes())

	assert.Zero(t, (&domain.Poll{}).ExpectedVotes())
	assert.Zero(t, (&domain.Poll{ExpectedAudience: -5, ExpectedConversion: 0.3}).ExpectedVotes(),
		"порча строки не должна давать отрицательную ёмкость")
}

func TestCanTransitionTo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		from domain.Status
		to   domain.Status
		want bool
	}{
		{name: "draft переходит в scheduled", from: domain.StatusDraft, to: domain.StatusScheduled, want: true},
		{name: "scheduled переходит в open", from: domain.StatusScheduled, to: domain.StatusOpen, want: true},
		{name: "open переходит в closed", from: domain.StatusOpen, to: domain.StatusClosed, want: true},
		{name: "closed переходит в archived", from: domain.StatusClosed, to: domain.StatusArchived, want: true},

		{name: "draft не открывается напрямую", from: domain.StatusDraft, to: domain.StatusOpen},
		{name: "draft не закрывается напрямую", from: domain.StatusDraft, to: domain.StatusClosed},
		{name: "закрытый опрос нельзя открыть заново", from: domain.StatusClosed, to: domain.StatusOpen},
		{name: "закрытый опрос нельзя вернуть в scheduled", from: domain.StatusClosed, to: domain.StatusScheduled},
		{name: "archived не переходит в open", from: domain.StatusArchived, to: domain.StatusOpen},
		{name: "archived не переходит в closed", from: domain.StatusArchived, to: domain.StatusClosed},
		{name: "archived не переходит в draft", from: domain.StatusArchived, to: domain.StatusDraft},
		{name: "scheduled не закрывается минуя open", from: domain.StatusScheduled, to: domain.StatusClosed},
		{name: "scheduled не возвращается в draft", from: domain.StatusScheduled, to: domain.StatusDraft},
		{name: "open не архивируется минуя closed", from: domain.StatusOpen, to: domain.StatusArchived},
		{name: "неизвестный статус никуда не переходит", from: domain.Status("paused"), to: domain.StatusOpen},
		{name: "переход в неизвестный статус запрещён", from: domain.StatusDraft, to: domain.Status("paused")},
		{name: "пустой статус никуда не переходит", from: domain.Status(""), to: domain.StatusDraft},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.from.CanTransitionTo(tc.to))
		})
	}
}

func TestShardCountFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		masters int
		want    uint16
	}{
		{name: "один мастер даёт 500 шардов", masters: 1, want: 500},
		{name: "три мастера дают 1500 шардов", masters: 3, want: 1500},
		{name: "шесть мастеров дают 3000 шардов", masters: 6, want: 3000},
		{name: "32 мастера дают 16000 шардов", masters: 32, want: 16000},
		{name: "33 мастера упираются в потолок", masters: 33, want: 16384},
		{name: "40 мастеров упираются в потолок", masters: 40, want: 16384},
		{name: "тысяча мастеров не переполняет uint16", masters: 1000, want: 16384},
		{name: "ноль мастеров даёт минимум, а не ноль", masters: 0, want: 500},
		{name: "отрицательное число мастеров даёт минимум", masters: -3, want: 500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, domain.ShardCountFor(tc.masters))
		})
	}
}

func TestShardCountFor_NeverReturnsZero(t *testing.T) {
	t.Parallel()

	for masters := -10; masters <= 200; masters++ {
		require.NotZero(t, domain.ShardCountFor(masters), "masters=%d", masters)
	}
}

func TestShardCountFor_IsMonotonicAndCapped(t *testing.T) {
	t.Parallel()

	prev := domain.ShardCountFor(-1)
	for masters := range 200 {
		got := domain.ShardCountFor(masters)
		require.GreaterOrEqual(t, got, prev, "masters=%d: число шардов не должно убывать", masters)
		require.LessOrEqual(t, got, uint16(domain.MaxShardCount), "masters=%d", masters)
		prev = got
	}
}

func TestShardCountFor_KeepsAtLeastFiveHundredShardsPerMaster(t *testing.T) {
	t.Parallel()

	for masters := 1; masters <= 32; masters++ {
		perMaster := int(domain.ShardCountFor(masters)) / masters
		assert.GreaterOrEqual(t, perMaster, 500, "masters=%d", masters)
	}
}

func TestPoll_OptionCount(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uint8(0), (&domain.Poll{}).OptionCount())
	assert.Equal(t, uint8(3), (&domain.Poll{Options: options(3)}).OptionCount())
	assert.Equal(t, uint8(255), (&domain.Poll{Options: make([]domain.Option, 255)}).OptionCount())
	assert.Equal(t, uint8(domain.MaxOptions), (&domain.Poll{Options: make([]domain.Option, 300)}).OptionCount(),
		"переполнение uint8 сделало бы из 300 опций 44")
}

func TestValidateChoices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		poll    *domain.Poll
		choices []uint8
		wantErr error
	}{
		{
			name:    "single принимает ровно один выбор",
			poll:    singlePoll(4),
			choices: []uint8{2},
		},
		{
			name:    "single отвергает два выбора",
			poll:    singlePoll(4),
			choices: []uint8{0, 1},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "single отвергает пустой выбор",
			poll:    singlePoll(4),
			choices: []uint8{},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "single игнорирует min и max опроса",
			poll:    &domain.Poll{Type: domain.PollTypeSingle, Options: options(3), MinChoices: 2, MaxChoices: 3},
			choices: []uint8{1},
		},
		{
			name:    "multiple принимает выбор внутри границ",
			poll:    multiplePoll(5, 2, 3),
			choices: []uint8{0, 4},
		},
		{
			name:    "multiple принимает ровно MinChoices",
			poll:    multiplePoll(5, 2, 3),
			choices: []uint8{1, 3},
		},
		{
			name:    "multiple принимает ровно MaxChoices",
			poll:    multiplePoll(5, 2, 3),
			choices: []uint8{1, 2, 3},
		},
		{
			name:    "multiple отвергает выбор ниже MinChoices",
			poll:    multiplePoll(5, 2, 3),
			choices: []uint8{1},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "multiple отвергает выбор выше MaxChoices",
			poll:    multiplePoll(5, 2, 3),
			choices: []uint8{0, 1, 2, 3},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "multiple отвергает пустой выбор",
			poll:    multiplePoll(5, 1, 3),
			choices: []uint8{},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "nil вместо списка выборов отвергается",
			poll:    multiplePoll(5, 1, 3),
			choices: nil,
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "индекс за пределами списка опций отвергается",
			poll:    singlePoll(3),
			choices: []uint8{3},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "далёкий индекс за пределами списка опций отвергается",
			poll:    multiplePoll(3, 1, 3),
			choices: []uint8{0, 200},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "дубль индекса отвергается",
			poll:    multiplePoll(5, 1, 3),
			choices: []uint8{2, 2},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "дубль индекса отвергается даже при верном размере набора",
			poll:    multiplePoll(5, 2, 2),
			choices: []uint8{4, 4},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "опрос без опций отвергает любой индекс",
			poll:    singlePoll(0),
			choices: []uint8{0},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "неизвестный тип опроса отвергает любой выбор",
			poll:    &domain.Poll{Type: domain.PollType("ranked"), Options: options(3), MaxChoices: 3},
			choices: []uint8{0},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "нулевой MaxChoices трактуется как отсутствие потолка",
			poll:    multiplePoll(4, 0, 0),
			choices: []uint8{0, 1, 2, 3},
		},
		{
			name:    "нулевой MinChoices всё равно требует хотя бы один выбор",
			poll:    multiplePoll(4, 0, 0),
			choices: []uint8{},
			wantErr: domain.ErrInvalidChoices,
		},
		{
			name:    "потолок не может превысить число опций",
			poll:    multiplePoll(2, 1, 200),
			choices: []uint8{0, 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.poll.ChoiceRules().Validate(tc.choices)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

func TestValidateChoices_DoesNotMutateInput(t *testing.T) {
	t.Parallel()

	p := multiplePoll(5, 1, 3)
	choices := []uint8{3, 1}
	require.NoError(t, p.ChoiceRules().Validate(choices))
	assert.Equal(t, []uint8{3, 1}, choices, "валидация не имеет права сортировать чужой срез")
}

func TestIsOpenAt(t *testing.T) {
	t.Parallel()

	opens := time.Date(2026, 9, 7, 20, 0, 0, 0, time.UTC)
	closes := opens.Add(time.Minute)

	openPoll := func() *domain.Poll {
		return &domain.Poll{Status: domain.StatusOpen, OpensAt: opens, ClosesAt: closes}
	}

	tests := []struct {
		name string
		poll *domain.Poll
		at   time.Time
		want bool
	}{
		{name: "до OpensAt закрыт", poll: openPoll(), at: opens.Add(-time.Nanosecond)},
		{name: "ровно в OpensAt открыт", poll: openPoll(), at: opens, want: true},
		{name: "внутри окна открыт", poll: openPoll(), at: opens.Add(30 * time.Second), want: true},
		{name: "за наносекунду до ClosesAt открыт", poll: openPoll(), at: closes.Add(-time.Nanosecond), want: true},
		{name: "ровно в ClosesAt закрыт", poll: openPoll(), at: closes},
		{name: "после ClosesAt закрыт", poll: openPoll(), at: closes.Add(time.Second)},
		{
			name: "статус draft закрыт внутри окна",
			poll: &domain.Poll{Status: domain.StatusDraft, OpensAt: opens, ClosesAt: closes},
			at:   opens.Add(time.Second),
		},
		{
			name: "статус scheduled закрыт внутри окна",
			poll: &domain.Poll{Status: domain.StatusScheduled, OpensAt: opens, ClosesAt: closes},
			at:   opens.Add(time.Second),
		},
		{
			name: "статус closed закрыт внутри окна",
			poll: &domain.Poll{Status: domain.StatusClosed, OpensAt: opens, ClosesAt: closes},
			at:   opens.Add(time.Second),
		},
		{
			name: "статус archived закрыт внутри окна",
			poll: &domain.Poll{Status: domain.StatusArchived, OpensAt: opens, ClosesAt: closes},
			at:   opens.Add(time.Second),
		},
		{
			name: "незаданный ClosesAt закрыт",
			poll: &domain.Poll{Status: domain.StatusOpen, OpensAt: opens},
			at:   opens.Add(time.Second),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.poll.IsOpenAt(tc.at))
		})
	}
}

func TestWindow_ContainsIgnoresStatus(t *testing.T) {
	t.Parallel()

	opens := time.Date(2026, 9, 7, 20, 0, 0, 0, time.UTC)
	w := (&domain.Poll{Status: domain.StatusClosed, OpensAt: opens, ClosesAt: opens.Add(time.Minute)}).Window()

	assert.True(t, w.Contains(opens.Add(30*time.Second)),
		"голос с 59-й секунды консьюмится на 300-й, когда опрос уже closed")
	assert.False(t, w.IsOpenAt(opens.Add(30*time.Second)), "приём смотрит и на статус тоже")
	assert.False(t, w.Contains(w.ClosesAt), "граница ClosesAt исключающая")
}

func TestIsOpenAt_IgnoresMonotonicClockReading(t *testing.T) {
	t.Parallel()

	opens := time.Now()
	p := &domain.Poll{Status: domain.StatusOpen, OpensAt: opens.Round(0), ClosesAt: opens.Round(0).Add(time.Minute)}

	assert.True(t, p.IsOpenAt(opens.Add(time.Second)))
}

func TestShouldOpenAt(t *testing.T) {
	t.Parallel()

	opens := time.Date(2026, 9, 7, 20, 0, 0, 0, time.UTC)
	closes := opens.Add(time.Minute)

	withStatus := func(s domain.Status) *domain.Poll {
		return &domain.Poll{Status: s, OpensAt: opens, ClosesAt: closes}
	}

	tests := []struct {
		name string
		poll *domain.Poll
		at   time.Time
		want bool
	}{
		{name: "scheduled до OpensAt ещё не открывается", poll: withStatus(domain.StatusScheduled), at: opens.Add(-time.Second)},
		{name: "scheduled ровно в OpensAt открывается", poll: withStatus(domain.StatusScheduled), at: opens, want: true},
		{name: "scheduled после OpensAt открывается", poll: withStatus(domain.StatusScheduled), at: opens.Add(time.Second), want: true},
		{
			name: "scheduled с прошедшим ClosesAt всё равно открывается",
			poll: withStatus(domain.StatusScheduled),
			at:   closes.Add(time.Hour),
			want: true,
		},
		{name: "draft не открывается по расписанию", poll: withStatus(domain.StatusDraft), at: opens.Add(time.Second)},
		{name: "уже открытый не открывается повторно", poll: withStatus(domain.StatusOpen), at: opens.Add(time.Second)},
		{name: "closed не открывается по расписанию", poll: withStatus(domain.StatusClosed), at: opens.Add(time.Second)},
		{name: "archived не открывается по расписанию", poll: withStatus(domain.StatusArchived), at: opens.Add(time.Second)},
		{
			name: "scheduled без OpensAt не открывается сам",
			poll: &domain.Poll{Status: domain.StatusScheduled, ClosesAt: closes},
			at:   opens.Add(time.Second),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.poll.ShouldOpenAt(tc.at))
		})
	}
}

func TestShouldOpenAt_AgreesWithFSM(t *testing.T) {
	t.Parallel()

	opens := time.Date(2026, 9, 7, 20, 0, 0, 0, time.UTC)
	all := []domain.Status{
		domain.StatusDraft, domain.StatusScheduled, domain.StatusOpen,
		domain.StatusClosed, domain.StatusArchived,
	}

	for _, s := range all {
		p := &domain.Poll{Status: s, OpensAt: opens, ClosesAt: opens.Add(time.Minute)}
		if p.ShouldOpenAt(opens.Add(time.Second)) {
			assert.True(t, s.CanTransitionTo(domain.StatusOpen),
				"ShouldOpenAt требует перехода, запрещённого FSM: %s→open", s)
		}
	}
}

func TestNewPoll_RejectsWhatDatabaseNoLongerChecks(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	base := func() domain.PollSpec {
		return domain.PollSpec{
			Slug: "final", Question: "кто?", Type: domain.PollTypeSingle,
			Options:  []string{"а", "б"},
			OpensAt:  now.Add(2 * time.Hour),
			ClosesAt: now.Add(3 * time.Hour),
		}
	}

	tests := []struct {
		name string
		spec func(domain.PollSpec) domain.PollSpec
	}{
		{"slug с заглавными", func(s domain.PollSpec) domain.PollSpec { s.Slug = "Final"; return s }},
		{"slug с пробелом", func(s domain.PollSpec) domain.PollSpec { s.Slug = "a b"; return s }},
		{"slug начинается с дефиса", func(s domain.PollSpec) domain.PollSpec { s.Slug = "-final"; return s }},
		{"slug длиннее предела", func(s domain.PollSpec) domain.PollSpec {
			s.Slug = strings.Repeat("a", domain.MaxSlugLen+1)
			return s
		}},
		{"вопрос длиннее предела", func(s domain.PollSpec) domain.PollSpec {
			s.Question = strings.Repeat("q", domain.MaxQuestionLen+1)
			return s
		}},
		{"вариант длиннее предела", func(s domain.PollSpec) domain.PollSpec {
			s.Options = []string{"а", strings.Repeat("б", domain.MaxOptionLen+1)}
			return s
		}},
		{"отрицательная аудитория", func(s domain.PollSpec) domain.PollSpec {
			s.ExpectedAudience = -1
			return s
		}},
		{"конверсия в процентах, а не долей", func(s domain.PollSpec) domain.PollSpec {
			s.ExpectedConversion = 30
			return s
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := domain.NewPoll(tt.spec(base()), now, time.Hour)
			require.ErrorIs(t, err, domain.ErrInvalidPoll)
		})
	}
}
