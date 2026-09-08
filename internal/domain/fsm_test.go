package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dubter/televote/internal/domain"
)

var allStatuses = []domain.Status{
	domain.StatusDraft,
	domain.StatusScheduled,
	domain.StatusOpen,
	domain.StatusClosed,
	domain.StatusArchived,
	domain.Status("paused"),
	domain.Status(""),
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

func TestCanTransitionTo_SelfTransitionIsRejected(t *testing.T) {
	t.Parallel()

	for _, s := range allStatuses {
		assert.False(t, s.CanTransitionTo(s), "%s→%s должен быть запрещён", s, s)
	}
}

func TestCanTransitionTo_ArchivedIsTerminal(t *testing.T) {
	t.Parallel()

	for _, to := range allStatuses {
		assert.False(t, domain.StatusArchived.CanTransitionTo(to),
			"archived→%s должен быть запрещён", to)
	}
}

func TestCanTransitionTo_OnlyFourTransitionsAreAllowed(t *testing.T) {
	t.Parallel()

	allowed := map[domain.Status]domain.Status{
		domain.StatusDraft:     domain.StatusScheduled,
		domain.StatusScheduled: domain.StatusOpen,
		domain.StatusOpen:      domain.StatusClosed,
		domain.StatusClosed:    domain.StatusArchived,
	}

	for _, from := range allStatuses {
		for _, to := range allStatuses {
			want := allowed[from] == to && allowed[from] != ""
			assert.Equal(t, want, from.CanTransitionTo(to), "%s→%s", from, to)
		}
	}
}

func TestStatus_Valid(t *testing.T) {
	t.Parallel()

	for _, s := range []domain.Status{
		domain.StatusDraft, domain.StatusScheduled, domain.StatusOpen,
		domain.StatusClosed, domain.StatusArchived,
	} {
		assert.True(t, s.Valid(), "%s должен быть валидным статусом", s)
	}
	assert.False(t, domain.Status("paused").Valid())
	assert.False(t, domain.Status("").Valid())
}
