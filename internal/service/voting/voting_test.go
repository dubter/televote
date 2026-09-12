package voting_test

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
	"github.com/dubter/televote/internal/service/pollcfg"
	"github.com/dubter/televote/internal/service/voting"
	"github.com/dubter/televote/internal/service/voting/mocks"
)

var (
	testSalt = []byte("test-poll-salt-0123456789abcdef!")
	opensAt  = time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	closesAt = opensAt.Add(time.Minute)
	inWindow = opensAt.Add(15 * time.Second)
)

const testVoter = "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"

func hotConfig(t *testing.T, pollType domain.PollType, minChoices, maxChoices uint8) *pollcfg.HotConfig {
	t.Helper()

	p := &domain.Poll{
		ID:         uuid.New(),
		Slug:       "final",
		Type:       pollType,
		Options:    []domain.Option{{Idx: 0, Text: "первый"}, {Idx: 1, Text: "второй"}, {Idx: 2, Text: "третий"}},
		MinChoices: minChoices,
		MaxChoices: maxChoices,
		Status:     domain.StatusOpen,
		OpensAt:    opensAt,
		ClosesAt:   closesAt,
		ShardCount: 500,
		Salt:       testSalt,
	}
	return &pollcfg.HotConfig{
		ID: p.ID, Slug: p.Slug, Options: p.Options,
		Rules: p.ChoiceRules(), Window: p.Window(), ShardCount: p.ShardCount, Salt: p.Salt,
	}
}

type fixture struct {
	svc  *voting.Service
	sink *mocks.MockSink
	obs  *mocks.MockObserver
}

func newFixture(t *testing.T, cfg *pollcfg.HotConfig, now time.Time) fixture {
	t.Helper()

	ctrl := gomock.NewController(t)

	lookup := mocks.NewMockConfigLookup(ctrl)
	lookup.EXPECT().BySlug(cfg.Slug).Return(cfg, true).AnyTimes()
	lookup.EXPECT().BySlug(gomock.Not(cfg.Slug)).Return(nil, false).AnyTimes()

	sink := mocks.NewMockSink(ctrl)
	obs := mocks.NewMockObserver(ctrl)

	svc, err := voting.New(lookup, sink, obs, func() time.Time { return now })
	require.NoError(t, err)
	return fixture{svc: svc, sink: sink, obs: obs}
}

func TestAccept_DerivesVoterAndQueuesTheVote(t *testing.T) {
	t.Parallel()

	f := newFixture(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow)
	f.obs.EXPECT().ProduceSeconds(gomock.Any()).Times(1)
	f.obs.EXPECT().VoteAccepted().Times(1)

	var sent domain.VoteMessage
	f.sink.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, m domain.VoteMessage) error { sent = m; return nil },
	).Times(1)

	require.NoError(t, f.svc.Accept(context.Background(), "final", testVoter, []uint8{1}))

	assert.Equal(t, []uint8{1}, sent.Choices)
	assert.Len(t, sent.VoterID, 32, "в Kafka уезжает выведенный сервером ключ")
	assert.NotContains(t, sent.VoterID, "9b2f4c6e", "присланное клиентом значение не уезжает как есть")
	assert.Equal(t, inWindow.UTC(), sent.ProducedAt, "по этой метке консьюмер проверит окно")
}

func TestAccept_SameVoterYieldsSameVoterID(t *testing.T) {
	t.Parallel()

	f := newFixture(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow)
	f.obs.EXPECT().ProduceSeconds(gomock.Any()).Times(2)
	f.obs.EXPECT().VoteAccepted().Times(2)

	var sent []domain.VoteMessage
	f.sink.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, m domain.VoteMessage) error { sent = append(sent, m); return nil },
	).Times(2)

	require.NoError(t, f.svc.Accept(context.Background(), "final", testVoter, []uint8{1}))
	require.NoError(t, f.svc.Accept(context.Background(), "final", testVoter, []uint8{1}))

	require.Len(t, sent, 2, "приём не дедуплицирует — это делает консьюмер")
	assert.Equal(t, sent[0].VoterID, sent[1].VoterID)
}

func TestAccept_RejectsBadClientIDs(t *testing.T) {
	t.Parallel()

	for name, voter := range map[string]string{
		"пустой":            "",
		"константа клиента": "undefined",
		"нулевой uuid":      "00000000-0000-0000-0000-000000000000",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow)
			f.obs.EXPECT().VoteRejected("bad_voter").Times(1)

			err := f.svc.Accept(context.Background(), "final", voter, []uint8{1})
			assert.ErrorIs(t, err, domain.ErrBadClientID)
		})
	}
}

func TestAccept_RejectsChoicesOutsideRules(t *testing.T) {
	t.Parallel()

	for name, choices := range map[string][]uint8{
		"ниже MinChoices":     {0},
		"выше MaxChoices":     {0, 1, 2},
		"индекс за пределами": {0, 9},
		"дубль индекса":       {1, 1},
		"пустой выбор":        {},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, hotConfig(t, domain.PollTypeMultiple, 2, 2), inWindow)
			f.obs.EXPECT().VoteRejected("invalid_choices").Times(1)

			err := f.svc.Accept(context.Background(), "final", testVoter, choices)
			assert.ErrorIs(t, err, domain.ErrInvalidChoices)
		})
	}
}

func TestAccept_RejectsVotesOutsideTheWindow(t *testing.T) {
	t.Parallel()

	for name, now := range map[string]time.Time{
		"до открытия":      opensAt.Add(-time.Second),
		"ровно в закрытие": closesAt,
		"после закрытия":   closesAt.Add(time.Minute),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t, hotConfig(t, domain.PollTypeSingle, 1, 1), now)
			f.obs.EXPECT().VoteRejected("poll_closed").Times(1)

			err := f.svc.Accept(context.Background(), "final", testVoter, []uint8{1})
			assert.ErrorIs(t, err, domain.ErrPollClosed)
		})
	}
}

func TestAccept_UnknownPollIsNotFound(t *testing.T) {
	t.Parallel()

	f := newFixture(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow)
	f.obs.EXPECT().VoteRejected("unknown_poll").Times(1)

	err := f.svc.Accept(context.Background(), "нет-такого", testVoter, []uint8{1})
	assert.ErrorIs(t, err, domain.ErrNotFound)
}

func TestAccept_QueueDownIsReportedAsUnavailable(t *testing.T) {
	t.Parallel()

	f := newFixture(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow)
	f.obs.EXPECT().VoteRejected("unavailable").Times(1)
	f.obs.EXPECT().VoteAccepted().Times(0)
	f.sink.EXPECT().Send(gomock.Any(), gomock.Any()).Return(errors.New("брокеры недоступны"))

	err := f.svc.Accept(context.Background(), "final", testVoter, []uint8{1})
	assert.ErrorIs(t, err, domain.ErrQueueUnavailable, "врать клиенту нельзя: голос не положен в Kafka")
}

func TestNew_RejectsMissingDependencies(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	lookup := mocks.NewMockConfigLookup(ctrl)
	sink := mocks.NewMockSink(ctrl)
	obs := mocks.NewMockObserver(ctrl)

	_, err := voting.New(nil, sink, obs, time.Now)
	require.Error(t, err)
	_, err = voting.New(lookup, nil, obs, time.Now)
	require.Error(t, err)
	_, err = voting.New(lookup, sink, nil, time.Now)
	require.Error(t, err)
	_, err = voting.New(lookup, sink, obs, nil)
	require.Error(t, err)
}
