package capacity_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/capacity"
	"github.com/dubter/televote/internal/domain"
)

var now = time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC)

type fakePolls []*domain.Poll

func (f fakePolls) ListActive(context.Context) ([]*domain.Poll, error) { return f, nil }

type fakeLag struct {
	n   int64
	err error
}

func (f fakeLag) Lag(context.Context) (int64, error) { return f.n, f.err }

func poll(status domain.Status, opensIn time.Duration) *domain.Poll {
	return &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: status,
		OpensAt: now.Add(opensIn), ClosesAt: now.Add(opensIn + time.Minute),
		ExpectedAudience: 100_000_000, ExpectedConversion: 0.3,
	}
}

func advise(t *testing.T, polls fakePolls, lag capacity.Lag) capacity.Advice {
	t.Helper()

	a, err := capacity.New(polls, lag, capacity.Config{
		DrainWindow: 5 * time.Minute,
		PrewarmLead: time.Hour,
		Now:         func() time.Time { return now },
	})
	require.NoError(t, err)

	adv, err := a.Advise(context.Background())
	require.NoError(t, err)
	return adv
}

func TestAdvise_Phases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		polls fakePolls
		lag   capacity.Lag
		want  capacity.Phase
	}{
		{name: "эфиров нет", polls: nil, lag: fakeLag{}, want: capacity.PhaseIdle},
		{
			name:  "ближайший эфир за горизонтом прогрева",
			polls: fakePolls{poll(domain.StatusScheduled, 5*time.Hour)},
			lag:   fakeLag{},
			want:  capacity.PhaseIdle,
		},
		{
			name:  "эфир через полчаса",
			polls: fakePolls{poll(domain.StatusScheduled, 30*time.Minute)},
			lag:   fakeLag{},
			want:  capacity.PhasePrewarm,
		},
		{
			name:  "идёт приём",
			polls: fakePolls{poll(domain.StatusOpen, -10*time.Second)},
			lag:   fakeLag{},
			want:  capacity.PhaseLive,
		},
		{
			name:  "приём закрыт, лаг ненулевой",
			polls: fakePolls{poll(domain.StatusOpen, -2*time.Hour)},
			lag:   fakeLag{n: 5000},
			want:  capacity.PhaseDrain,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, advise(t, tc.polls, tc.lag).Phase)
		})
	}
}

// Ёмкость выводится из ожидаемой аудитории опроса, а не из константы в коде:
// конверсия в ТЗ не задана, и допущение обязано быть параметром.
func TestAdvise_CapacityComesFromPoll(t *testing.T) {
	t.Parallel()

	big := advise(t, fakePolls{poll(domain.StatusOpen, -10*time.Second)}, fakeLag{})

	small := poll(domain.StatusOpen, -10*time.Second)
	small.ExpectedAudience = 10_000
	tiny := advise(t, fakePolls{small}, fakeLag{})

	assert.Greater(t, big.Desired.VoteAPI, tiny.Desired.VoteAPI)
	assert.Greater(t, big.Desired.RedisMasters, tiny.Desired.RedisMasters)
}

// Между эфирами Redis и консьюмеры не нужны вовсе: персистентного состояния
// у них нет, поэтому они уничтожаются, а не масштабируются вниз.
func TestAdvise_IdleDropsStatefulCapacityToZero(t *testing.T) {
	t.Parallel()

	adv := advise(t, nil, fakeLag{})

	assert.Zero(t, adv.Desired.RedisMasters)
	assert.Zero(t, adv.Desired.Consumers)
	assert.Positive(t, adv.Desired.VoteAPI, "админка обязана отвечать между эфирами")
}

// Ошибка чтения лага не имеет права уронить советчика: KEDA получила бы отказ
// и в худшем случае снесла бы ёмкость посреди дренажа.
func TestAdvise_SurvivesLagFailure(t *testing.T) {
	t.Parallel()

	adv := advise(t, fakePolls{poll(domain.StatusOpen, -2*time.Hour)}, fakeLag{err: errors.New("kafka недоступна")})

	assert.Equal(t, capacity.PhaseDrain, adv.Phase, "при неизвестном лаге считаем, что дренаж ещё идёт")
	assert.Positive(t, adv.Desired.RedisMasters)
}

func TestNew_RejectsMissingPolls(t *testing.T) {
	t.Parallel()

	_, err := capacity.New(nil, nil, capacity.Config{})
	require.Error(t, err)
}
