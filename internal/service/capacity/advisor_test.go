package capacity_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/capacity"
	"github.com/dubter/televote/internal/service/capacity/mocks"
)

var now = time.Date(2026, 9, 8, 20, 0, 0, 0, time.UTC)

func poll(status domain.Status, opensIn time.Duration) *domain.Poll {
	return &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: status,
		OpensAt: now.Add(opensIn), ClosesAt: now.Add(opensIn + time.Minute),
		ExpectedAudience: 100_000_000, ExpectedConversion: 0.3,
	}
}

func advise(t *testing.T, polls []*domain.Poll, lag int64, lagErr error) capacity.Advice {
	t.Helper()

	ctrl := gomock.NewController(t)

	repo := mocks.NewMockPolls(ctrl)
	repo.EXPECT().ListActive(gomock.Any()).Return(polls, nil)

	reader := mocks.NewMockLag(ctrl)
	reader.EXPECT().Lag(gomock.Any()).Return(lag, lagErr).Times(1)

	a, err := capacity.New(repo, reader, capacity.Config{
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
		polls []*domain.Poll
		lag   int64
		want  capacity.Phase
	}{
		{name: "эфиров нет", want: capacity.PhaseIdle},
		{
			name:  "ближайший эфир за горизонтом прогрева",
			polls: []*domain.Poll{poll(domain.StatusScheduled, 5*time.Hour)},
			want:  capacity.PhaseIdle,
		},
		{
			name:  "эфир через полчаса",
			polls: []*domain.Poll{poll(domain.StatusScheduled, 30*time.Minute)},
			want:  capacity.PhasePrewarm,
		},
		{
			name:  "идёт приём",
			polls: []*domain.Poll{poll(domain.StatusOpen, -10*time.Second)},
			want:  capacity.PhaseLive,
		},
		{
			name:  "приём закрыт, лаг ненулевой",
			polls: []*domain.Poll{poll(domain.StatusOpen, -2*time.Hour)},
			lag:   5000,
			want:  capacity.PhaseDrain,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, advise(t, tc.polls, tc.lag, nil).Phase)
		})
	}
}

func TestAdvise_CapacityComesFromPoll(t *testing.T) {
	t.Parallel()

	big := advise(t, []*domain.Poll{poll(domain.StatusOpen, -10*time.Second)}, 0, nil)

	small := poll(domain.StatusOpen, -10*time.Second)
	small.ExpectedAudience = 10_000
	tiny := advise(t, []*domain.Poll{small}, 0, nil)

	assert.Greater(t, big.Desired.VoteAPI, tiny.Desired.VoteAPI)
	assert.Greater(t, big.Desired.RedisMasters, tiny.Desired.RedisMasters)
}

func TestAdvise_SurvivesLagFailure(t *testing.T) {
	t.Parallel()

	adv := advise(t, []*domain.Poll{poll(domain.StatusOpen, -2*time.Hour)}, 0, errors.New("kafka недоступна"))

	assert.Equal(t, capacity.PhaseDrain, adv.Phase, "при неизвестном лаге считаем, что дренаж ещё идёт")
	assert.Positive(t, adv.Desired.RedisMasters)
}

func TestNew_RejectsMissingPolls(t *testing.T) {
	t.Parallel()

	_, err := capacity.New(nil, nil, capacity.Config{})
	require.Error(t, err)
}
