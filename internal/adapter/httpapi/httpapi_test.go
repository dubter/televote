package httpapi_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/adapter/httpapi"
	"github.com/dubter/televote/internal/adapter/httpapi/mocks"
	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/auth"
	"github.com/dubter/televote/internal/service/capacity"
	"github.com/dubter/televote/internal/service/pollcfg"
)

var (
	adminKey = []byte("admin-test-key-0123456789abcdef!")
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
		Question:   "кто победит?",
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
		ID: p.ID, Slug: p.Slug, Question: p.Question, Options: p.Options,
		Rules: p.ChoiceRules(), Window: p.Window(), ShardCount: p.ShardCount, Salt: p.Salt,
	}
}

func newPublic(
	t *testing.T, cfg *pollcfg.HotConfig, now time.Time, obs httpapi.Observer, maxBody int64,
) (http.Handler, *mocks.MockVoteSink) {
	t.Helper()

	ctrl := gomock.NewController(t)

	cache := mocks.NewMockConfigCache(ctrl)
	cache.EXPECT().BySlug(cfg.Slug).Return(cfg, true).AnyTimes()
	cache.EXPECT().BySlug(gomock.Not(cfg.Slug)).Return(nil, false).AnyTimes()

	sink := mocks.NewMockVoteSink(ctrl)

	h, err := httpapi.NewPublicHandler(cache, sink, obs, func() time.Time { return now }, maxBody)
	require.NoError(t, err)
	return h.Routes(), sink
}

func postVote(t *testing.T, h http.Handler, slug, body string) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/polls/"+slug+"/vote", strings.NewReader(body))
	r.Header.Set("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X)")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestFR3_VoteWithoutRegistration(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	obs := mocks.NewMockObserver(ctrl)
	obs.EXPECT().ProduceSeconds(gomock.Any()).Times(1)
	obs.EXPECT().VoteAccepted().Times(1)

	h, sink := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, obs, 0)

	var sent domain.VoteMessage
	sink.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, m domain.VoteMessage) error { sent = m; return nil },
	).Times(1)

	w := postVote(t, h, "final", `{"choices":[1],"voter":"`+testVoter+`"}`)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())

	var got struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "accepted", got.Status, "200 counted было бы враньём: голос ещё не посчитан")

	assert.Equal(t, []uint8{1}, sent.Choices)
	assert.Len(t, sent.VoterID, 32, "в Kafka уезжает выведенный сервером ключ")
	assert.NotContains(t, sent.VoterID, "9b2f4c6e", "присланное клиентом значение не уезжает как есть")
	assert.Equal(t, inWindow.UTC(), sent.ProducedAt, "по этой метке консьюмер проверит окно")
}

func TestFR3_RejectVoteWithoutVoter(t *testing.T) {
	t.Parallel()

	h, _ := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, nil, 0)

	cases := map[string]string{
		"пустой voter":      `{"choices":[1],"voter":""}`,
		"voter отсутствует": `{"choices":[1]}`,
		"константа клиента": `{"choices":[1],"voter":"undefined"}`,
		"нулевой uuid":      `{"choices":[1],"voter":"00000000-0000-0000-0000-000000000000"}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			w := postVote(t, h, "final", body)
			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
}

func TestFR1_2_RejectChoicesOutsideRules(t *testing.T) {
	t.Parallel()

	h, _ := newPublic(t, hotConfig(t, domain.PollTypeMultiple, 2, 2), inWindow, nil, 0)

	cases := map[string]string{
		"ниже MinChoices":     `{"choices":[0],"voter":"` + testVoter + `"}`,
		"выше MaxChoices":     `{"choices":[0,1,2],"voter":"` + testVoter + `"}`,
		"индекс за пределами": `{"choices":[0,9],"voter":"` + testVoter + `"}`,
		"дубль индекса":       `{"choices":[1,1],"voter":"` + testVoter + `"}`,
		"пустой выбор":        `{"choices":[],"voter":"` + testVoter + `"}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			w := postVote(t, h, "final", body)
			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
}

func TestFR8_VoteOutsideWindowIsRejected(t *testing.T) {
	t.Parallel()

	body := `{"choices":[1],"voter":"` + testVoter + `"}`

	cases := map[string]time.Time{
		"до открытия":      opensAt.Add(-time.Second),
		"ровно в закрытие": closesAt,
		"после закрытия":   closesAt.Add(time.Minute),
	}

	for name, now := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			h, _ := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), now, nil, 0)
			w := postVote(t, h, "final", body)

			assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		})
	}
}

func TestNFR9_BodySizeLimitEnforced(t *testing.T) {
	t.Parallel()

	h, _ := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, nil, 128)

	body := fmt.Sprintf(`{"choices":[1],"voter":%q}`, strings.Repeat("x", 256))
	w := postVote(t, h, "final", body)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestVote_KafkaDownReturns503(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	obs := mocks.NewMockObserver(ctrl)
	obs.EXPECT().VoteRejected("unavailable").Times(1)
	obs.EXPECT().VoteAccepted().Times(0)

	h, sink := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, obs, 0)
	sink.EXPECT().Send(gomock.Any(), gomock.Any()).Return(errors.New("брокеры недоступны"))

	w := postVote(t, h, "final", `{"choices":[1],"voter":"`+testVoter+`"}`)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.NotEmpty(t, w.Header().Get("Retry-After"))
}

func TestVote_UnknownPollIsNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, nil, 0)

	w := postVote(t, h, "нет-такого", `{"choices":[1],"voter":"`+testVoter+`"}`)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestPollConfig_ContainsServerTimeAndCacheHeader(t *testing.T) {
	t.Parallel()

	h, _ := newPublic(t, hotConfig(t, domain.PollTypeMultiple, 1, 2), inWindow, nil, 0)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/polls/final", nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Contains(t, w.Header().Get("Cache-Control"), "max-age",
		"конфиг одинаков для всех зрителей и обязан кэшироваться на CDN")

	var got struct {
		Question   string   `json:"question"`
		Options    []string `json:"options"`
		Min        uint8    `json:"min_choices"`
		Max        uint8    `json:"max_choices"`
		ServerTime string   `json:"server_time"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.Equal(t, "кто победит?", got.Question)
	assert.Equal(t, []string{"первый", "второй", "третий"}, got.Options)
	assert.EqualValues(t, 1, got.Min)
	assert.EqualValues(t, 2, got.Max)
	assert.Equal(t, inWindow.Format(time.RFC3339), got.ServerTime,
		"клиент работает по серверному времени: у зрителя часы могут врать")
}

func TestVote_SameVoterYieldsSameDedupKey(t *testing.T) {
	t.Parallel()

	h, sink := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, nil, 0)

	var sent []domain.VoteMessage
	sink.EXPECT().Send(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, m domain.VoteMessage) error { sent = append(sent, m); return nil },
	).Times(2)

	body := `{"choices":[1],"voter":"` + testVoter + `"}`
	require.Equal(t, http.StatusAccepted, postVote(t, h, "final", body).Code)
	require.Equal(t, http.StatusAccepted, postVote(t, h, "final", body).Code)

	require.Len(t, sent, 2, "приём не дедуплицирует — это делает консьюмер")
	assert.Equal(t, sent[0].VoterID, sent[1].VoterID)
}

type adminFixture struct {
	handler http.Handler
	polls   *mocks.MockPollStore
	results *mocks.MockResultStore
	admins  *mocks.MockAdminStore
	tokens  *auth.TokenService
	userID  uuid.UUID
	token   string
}

func newAdminFixture(t *testing.T, role auth.Role, minLeadTime time.Duration) adminFixture {
	t.Helper()

	ctrl := gomock.NewController(t)

	polls := mocks.NewMockPollStore(ctrl)
	results := mocks.NewMockResultStore(ctrl)
	admins := mocks.NewMockAdminStore(ctrl)

	tokens, err := auth.NewTokenService(adminKey, time.Hour)
	require.NoError(t, err)

	userID := uuid.New()
	token, err := tokens.Issue(userID, role)
	require.NoError(t, err)

	h, err := httpapi.NewAdminHandler(polls, results, admins, tokens,
		auth.NewLoginLimiter(3, time.Minute, 100), func() time.Time { return inWindow }, minLeadTime)
	require.NoError(t, err)

	return adminFixture{
		handler: h.Routes(), polls: polls, results: results, admins: admins,
		tokens: tokens, userID: userID, token: token,
	}
}

func (f adminFixture) do(t *testing.T, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}

	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	return w
}

func (f adminFixture) expectAdmin(t *testing.T, login, password string, times int) {
	t.Helper()

	hash, err := auth.HashPassword(password)
	require.NoError(t, err)

	f.admins.EXPECT().ByLogin(gomock.Any(), login).
		Return(&domain.Admin{ID: f.userID, Login: login, PasswordHash: hash, Role: string(auth.RoleAdmin)}, nil).
		Times(times)
}

func createBody(slug, pollType string, opts []string) string {
	opens := inWindow.Add(2 * time.Hour).Format(time.RFC3339)
	closes := inWindow.Add(3 * time.Hour).Format(time.RFC3339)

	payload, _ := json.Marshal(map[string]any{
		"slug": slug, "question": "кто победит?", "type": pollType, "options": opts,
		"min_choices": 1, "max_choices": 2,
		"opens_at": opens, "closes_at": closes,
		"expected_audience": 1000, "expected_conversion": 0.3,
	})
	return string(payload)
}

func TestFR7_AdminEndpointRequiresJWT(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin, 0)

	for _, path := range []string{"/polls", "/polls/final/results"} {
		assert.Equal(t, http.StatusUnauthorized, f.do(t, http.MethodGet, path, "", "").Code,
			"путь %s доступен без токена", path)
	}
	assert.Equal(t, http.StatusUnauthorized,
		f.do(t, http.MethodPost, "/polls", createBody("x", "single", []string{"а", "б"}), "").Code)

	assert.Equal(t, http.StatusUnauthorized,
		f.do(t, http.MethodGet, "/polls", "", "не-токен").Code)
}

func TestFR1_CreatePoll(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor, 0)

	var created *domain.Poll
	f.polls.EXPECT().Create(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p *domain.Poll) error { created = p; return nil },
	).Times(1)
	f.admins.EXPECT().Audit(gomock.Any(), gomock.Any(), "create_poll", "final", gomock.Any()).
		Return(nil).Times(1)

	w := f.do(t, http.MethodPost, "/polls", createBody("final", "single", []string{"первый", "второй"}), f.token)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())

	var got struct {
		Slug       string   `json:"slug"`
		Status     string   `json:"status"`
		Options    []string `json:"options"`
		ShardCount uint16   `json:"shard_count"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.Equal(t, "final", got.Slug)
	assert.Equal(t, string(domain.StatusScheduled), got.Status, "новый опрос обязан быть scheduled")
	assert.Equal(t, []string{"первый", "второй"}, got.Options)
	assert.NotZero(t, got.ShardCount, "нулевой shard_count дал бы деление на ноль на горячем пути")

	require.NotNil(t, created)
	assert.Equal(t, domain.StatusScheduled, created.Status)
}

func TestFR1_RejectsInvalidPolls(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor, 0)
	f.polls.EXPECT().Create(gomock.Any(), gomock.Any()).Times(0)

	cases := map[string]string{
		"неизвестный тип":   createBody("a", "ranking", []string{"а", "б"}),
		"один вариант":      createBody("b", "single", []string{"а"}),
		"пустые варианты":   createBody("c", "single", []string{}),
		"пустой вариант":    createBody("d", "single", []string{"а", "  "}),
		"мусор вместо тела": `{"slug":`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, http.StatusBadRequest, f.do(t, http.MethodPost, "/polls", body, f.token).Code)
		})
	}
}

func TestCreatePoll_DuplicateSlug(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor, 0)

	gomock.InOrder(
		f.polls.EXPECT().Create(gomock.Any(), gomock.Any()).Return(nil),
		f.polls.EXPECT().Create(gomock.Any(), gomock.Any()).Return(domain.ErrSlugTaken),
	)
	f.admins.EXPECT().Audit(gomock.Any(), gomock.Any(), "create_poll", "final", gomock.Any()).
		Return(nil).Times(1)

	body := createBody("final", "single", []string{"а", "б"})

	require.Equal(t, http.StatusCreated, f.do(t, http.MethodPost, "/polls", body, f.token).Code)
	assert.Equal(t, http.StatusConflict, f.do(t, http.MethodPost, "/polls", body, f.token).Code)
}

func TestCreatePoll_RejectsTooShortLeadTime(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor, time.Hour)
	f.polls.EXPECT().Create(gomock.Any(), gomock.Any()).Times(0)

	opens := inWindow.Add(10 * time.Minute).Format(time.RFC3339)
	closes := inWindow.Add(11 * time.Minute).Format(time.RFC3339)
	body, err := json.Marshal(map[string]any{
		"slug": "soon", "question": "?", "type": "single", "options": []string{"а", "б"},
		"opens_at": opens, "closes_at": closes,
	})
	require.NoError(t, err)

	w := f.do(t, http.MethodPost, "/polls", string(body), f.token)
	assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
}

func TestFR6_TransitionsFollowFSM(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusScheduled,
		Options: []domain.Option{{Idx: 0, Text: "а"}}, Version: 1,
		OpensAt: opensAt, ClosesAt: closesAt,
	}
	f := newAdminFixture(t, auth.RoleEditor, 0)

	f.polls.EXPECT().GetBySlug(gomock.Any(), "final").Return(poll, nil).AnyTimes()
	f.polls.EXPECT().Transition(gomock.Any(), poll.ID, domain.StatusOpen, poll.Version).Return(nil).Times(1)
	f.polls.EXPECT().CloseNow(gomock.Any(), poll.ID, poll.Version).Return(nil).Times(1)
	gomock.InOrder(
		f.admins.EXPECT().Audit(gomock.Any(), gomock.Any(), "open_poll", "final", gomock.Any()).Return(nil),
		f.admins.EXPECT().Audit(gomock.Any(), gomock.Any(), "close_poll", "final", gomock.Any()).Return(nil),
	)

	require.Equal(t, http.StatusOK, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)
	assert.Equal(t, domain.StatusOpen, poll.Status)

	assert.Equal(t, http.StatusConflict, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code,
		"открытый опрос нельзя открыть заново")

	require.Equal(t, http.StatusOK, f.do(t, http.MethodPost, "/polls/final/close", "", f.token).Code)
	assert.Equal(t, domain.StatusOpen, poll.Status,
		"ручное закрытие двигает closes_at, а не статус: closed выбросил бы опрос из выборки снапшотера")
	assert.Equal(t, inWindow, poll.ClosesAt)
	assert.False(t, poll.Window().IsOpenAt(inWindow), "после ручного закрытия приём закрыт")

	assert.Equal(t, http.StatusConflict, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)
}

func TestFR7_ViewerCannotWrite(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{ID: uuid.New(), Slug: "final", Status: domain.StatusScheduled, Version: 1}
	f := newAdminFixture(t, auth.RoleViewer, 0)

	f.polls.EXPECT().List(gomock.Any()).Return([]*domain.Poll{poll}, nil).Times(1)
	f.polls.EXPECT().Create(gomock.Any(), gomock.Any()).Times(0)
	f.polls.EXPECT().Transition(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	assert.Equal(t, http.StatusOK, f.do(t, http.MethodGet, "/polls", "", f.token).Code,
		"чтение доступно viewer")
	assert.Equal(t, http.StatusForbidden,
		f.do(t, http.MethodPost, "/polls", createBody("x", "single", []string{"а", "б"}), f.token).Code)
	assert.Equal(t, http.StatusForbidden,
		f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)
}

func TestFR5_PercentagesAreOfBallotsNotVotes(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusOpen, Version: 1,
		Type:    domain.PollTypeMultiple,
		Options: []domain.Option{{Idx: 0, Text: "а"}, {Idx: 1, Text: "б"}, {Idx: 2, Text: "в"}},
	}
	f := newAdminFixture(t, auth.RoleViewer, 0)

	f.polls.EXPECT().GetBySlug(gomock.Any(), "final").Return(poll, nil)
	f.results.EXPECT().Get(gomock.Any(), poll.ID).
		Return(domain.NewAggregateFrom(map[uint8]int64{0: 80, 1: 60, 2: 20}, 100), nil)

	w := f.do(t, http.MethodGet, "/polls/final/results", "", f.token)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got struct {
		Ballots int64 `json:"ballots"`
		Final   bool  `json:"final"`
		Options []struct {
			Votes   int64   `json:"votes"`
			Percent float64 `json:"percent"`
		} `json:"options"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	require.Len(t, got.Options, 3)
	assert.EqualValues(t, 100, got.Ballots)
	assert.InDelta(t, 80.0, got.Options[0].Percent, 1e-9)
	assert.InDelta(t, 60.0, got.Options[1].Percent, 1e-9)
	assert.False(t, got.Final, "пока опрос открыт, подсчёт не окончен")

	sum := got.Options[0].Percent + got.Options[1].Percent + got.Options[2].Percent
	assert.Greater(t, sum, 100.0)
}

func TestAdminLogin(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin, 0)
	f.expectAdmin(t, "admin", "secret", 2)
	f.admins.EXPECT().ByLogin(gomock.Any(), "нет").Return(nil, domain.ErrNotFound).Times(1)

	w := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"secret"}`, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got struct {
		Token string `json:"token"`
		Role  string `json:"role"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.NotEmpty(t, got.Token)
	assert.Equal(t, "admin", got.Role)

	claims, err := f.tokens.Parse(got.Token)
	require.NoError(t, err)
	assert.Equal(t, auth.RoleAdmin, claims.Role)

	wrongPass := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"нет"}`, "")
	wrongUser := f.do(t, http.MethodPost, "/login", `{"login":"нет","password":"secret"}`, "")
	assert.Equal(t, http.StatusUnauthorized, wrongPass.Code)
	assert.Equal(t, wrongPass.Code, wrongUser.Code)
	assert.Equal(t, wrongPass.Body.String(), wrongUser.Body.String(),
		"ответ не имеет права выдавать, существует ли логин")
}

func TestAdminLogin_IsRateLimited(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin, 0)
	f.expectAdmin(t, "admin", "secret", 3)

	for range 3 {
		f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"нет"}`, "")
	}
	w := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"secret"}`, "")

	assert.Equal(t, http.StatusUnauthorized, w.Code, "лимит попыток не сработал")
	assert.NotEmpty(t, w.Header().Get("Retry-After"))
}

func TestResults_UnknownPollIsNotFound(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleViewer, 0)
	f.polls.EXPECT().GetBySlug(gomock.Any(), "нет").Return(nil, domain.ErrNotFound)
	f.results.EXPECT().Get(gomock.Any(), gomock.Any()).Times(0)

	assert.Equal(t, http.StatusNotFound, f.do(t, http.MethodGet, "/polls/нет/results", "", f.token).Code)
}

func TestFR5_ClosedPollReturnsFinalResult(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusClosed, Version: 1,
		Options: []domain.Option{{Idx: 0, Text: "а"}},
	}
	f := newAdminFixture(t, auth.RoleViewer, 0)

	f.polls.EXPECT().GetBySlug(gomock.Any(), "final").Return(poll, nil)
	f.results.EXPECT().Get(gomock.Any(), poll.ID).
		Return(domain.NewAggregateFrom(map[uint8]int64{0: 42}, 42), nil)

	w := f.do(t, http.MethodGet, "/polls/final/results", "", f.token)
	require.Equal(t, http.StatusOK, w.Code)

	var got struct {
		Ballots int64 `json:"ballots"`
		Final   bool  `json:"final"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.EqualValues(t, 42, got.Ballots)
	assert.True(t, got.Final)
}

func TestCapacityHandler_ServesAdvice(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	advisor := mocks.NewMockCapacityAdvisor(ctrl)
	advisor.EXPECT().Advise(gomock.Any()).Return(capacity.Advice{
		Phase:    capacity.PhaseLive,
		Reason:   "votes are being accepted",
		PollSlug: "final",
		Desired:  domain.Capacity{VoteAPI: 12, Consumers: 3, RedisMasters: 7, KafkaPartitions: 3},
	}, nil)

	w := httptest.NewRecorder()
	httpapi.CapacityHandler(advisor).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/capacity", nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got struct {
		Phase   string `json:"phase"`
		Desired struct {
			RedisMasters int `json:"redis_masters"`
		} `json:"desired"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, string(capacity.PhaseLive), got.Phase)
	assert.Equal(t, 7, got.Desired.RedisMasters, "оператор масштабирует кластер по этому числу")
}

func fetchPage(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()

	w := httptest.NewRecorder()
	httpapi.StaticRoutes("https://vote.example").ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestFR2_ShortLinkServesVotingPage(t *testing.T) {
	t.Parallel()

	w := fetchPage(t, "/p/final")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Header().Get("Content-Type"), "text/html")
	assert.Contains(t, w.Body.String(), "<noscript>", "страница обязана быть осмысленной без JS")
}

func TestVotePage_UnderEightKilobytesGzipped(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := io.Copy(zw, bytes.NewReader(fetchPage(t, "/p/final").Body.Bytes()))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	assert.Less(t, buf.Len(), 8*1024, "страница голосования раздулась: %d байт gzip", buf.Len())
}

func TestVotePage_UsesLocalStorageNotSession(t *testing.T) {
	t.Parallel()

	body := fetchPage(t, "/p/final").Body.String()

	assert.Contains(t, body, "localStorage.getItem")
	assert.NotContains(t, body, "sessionStorage.",
		"идентификатор голосующего обязан переживать закрытие вкладки")
}

func TestVotePage_HasNoExternalResources(t *testing.T) {
	t.Parallel()

	body := fetchPage(t, "/p/final").Body.String()

	for _, marker := range []string{"src=\"http", "href=\"http", "//cdn", "googleapis"} {
		assert.NotContains(t, body, marker, "страница тянет внешний ресурс: %s", marker)
	}
}

func TestAdminPage_IsServedAndNotIndexed(t *testing.T) {
	t.Parallel()

	w := fetchPage(t, "/admin")

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "noindex",
		"админка не должна попадать в поисковый индекс")
}
