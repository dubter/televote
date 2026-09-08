package domain_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
)

// options строит n опций с плотными индексами 0..n-1 — ровно так их пишет
// админка, и на плотность опирается проверка «индекс за пределами списка».
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
			// MaxChoices=0 — не «голосовать нельзя», а «потолок не задан».
			// Жёсткая проверка len > 0 сделала бы опрос тихо неголосуемым.
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

			err := tc.poll.ValidateChoices(tc.choices)
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
}

// Горячий путь валидирует голос по HotConfig, а не по строке опроса.
// Правило обязано быть одно, иначе две копии разъедутся молча.
func TestChoiceRules_MatchPollValidation(t *testing.T) {
	t.Parallel()

	p := multiplePoll(5, 2, 3)
	rules := p.ChoiceRules()

	assert.Equal(t, domain.PollTypeMultiple, rules.Type)
	assert.Equal(t, uint8(5), rules.OptionCount)
	assert.Equal(t, uint8(2), rules.MinChoices)
	assert.Equal(t, uint8(3), rules.MaxChoices)

	require.NoError(t, rules.Validate([]uint8{0, 1}))
	require.ErrorIs(t, rules.Validate([]uint8{0}), domain.ErrInvalidChoices)
}

func TestValidateChoices_DoesNotMutateInput(t *testing.T) {
	t.Parallel()

	p := multiplePoll(5, 1, 3)
	choices := []uint8{3, 1}
	require.NoError(t, p.ValidateChoices(choices))
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
			// Пустой ClosesAt — незаполненный конфиг, а не «голосуем вечно».
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

// Момент ClosesAt принадлежит закрытому опросу. Правило зафиксировано планом
// (Task 2, шаг 6) и отдельным тестом, потому что «<» вместо «<=» не даёт
// ни ошибки компиляции, ни падения — только лишние голоса после эфира.
func TestIsOpenAt_VoteExactlyAtClosesAtIsRejected(t *testing.T) {
	t.Parallel()

	closes := time.Date(2026, 9, 7, 20, 1, 0, 0, time.UTC)
	p := &domain.Poll{Status: domain.StatusOpen, OpensAt: closes.Add(-time.Minute), ClosesAt: closes}

	assert.False(t, p.IsOpenAt(closes), "голос ровно в ClosesAt должен быть отвергнут")
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
			// Планировщик мог лежать дольше эфира. Открыть и дать финализатору
			// закрыть — единственный путь по FSM: scheduled→closed запрещён.
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
			// Расписания нет — открывать нечему; такой опрос открывает админ руками.
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

// Планировщик выполняет переход через тот же FSM. Если ShouldOpenAt скажет
// «пора», а CanTransitionTo откажет, опрос не откроется и никто не заметит.
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

func TestWindow_MatchesPollWindow(t *testing.T) {
	t.Parallel()

	opens := time.Date(2026, 9, 7, 20, 0, 0, 0, time.UTC)
	p := &domain.Poll{Status: domain.StatusOpen, OpensAt: opens, ClosesAt: opens.Add(time.Minute)}
	w := p.Window()

	assert.Equal(t, domain.StatusOpen, w.Status)
	assert.Equal(t, opens, w.OpensAt)
	assert.Equal(t, opens.Add(time.Minute), w.ClosesAt)
	assert.Equal(t, p.IsOpenAt(opens), w.IsOpenAt(opens))
	assert.False(t, w.IsOpenAt(w.ClosesAt))
}
