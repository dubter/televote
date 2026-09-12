package httpapi_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/platform/health"
	"github.com/dubter/televote/internal/service/auth"
	"github.com/dubter/televote/internal/service/capacity"
	"github.com/dubter/televote/internal/service/pollcfg"
	"github.com/dubter/televote/internal/service/polls"
	"github.com/dubter/televote/internal/transport/httpapi"
	"github.com/dubter/televote/internal/transport/httpapi/mocks"
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

func newPublicHandler(
	t *testing.T, cfg *pollcfg.HotConfig, now time.Time, maxBody int64,
) (*httpapi.PublicHandler, *mocks.MockVoting) {
	t.Helper()

	ctrl := gomock.NewController(t)

	cache := mocks.NewMockConfigLookup(ctrl)
	cache.EXPECT().BySlug(cfg.Slug).Return(cfg, true).AnyTimes()
	cache.EXPECT().BySlug(gomock.Not(cfg.Slug)).Return(nil, false).AnyTimes()

	voting := mocks.NewMockVoting(ctrl)

	h, err := httpapi.NewPublicHandler(voting, cache, func() time.Time { return now }, maxBody)
	require.NoError(t, err)
	return h, voting
}

func newPublic(t *testing.T, cfg *pollcfg.HotConfig, now time.Time, maxBody int64) (http.Handler, *mocks.MockVoting) {
	t.Helper()

	h, voting := newPublicHandler(t, cfg, now, maxBody)
	return httpapi.PublicRoutes(h), voting
}

func postVote(t *testing.T, h http.Handler, slug, body string) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/polls/"+slug+"/vote", strings.NewReader(body))
	r.Header.Set("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X)")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestVote_AcceptedIs202NotCounted(t *testing.T) {
	t.Parallel()

	h, voting := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, 0)
	voting.EXPECT().Accept(gomock.Any(), "final", testVoter, []uint8{1}).Return(nil)

	w := postVote(t, h, "final", `{"choices":[1],"voter":"`+testVoter+`"}`)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())

	var got struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "accepted", got.Status, "200 counted было бы враньём: голос ещё не посчитан")
}

func TestVote_MapsUseCaseErrorsToStatuses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		err    error
		status int
	}{
		{"нет опроса", domain.ErrNotFound, http.StatusNotFound},
		{"выбор вне правил", domain.ErrInvalidChoices, http.StatusBadRequest},
		{"плохой voter", domain.ErrBadClientID, http.StatusBadRequest},
		{"окно закрыто", domain.ErrPollClosed, http.StatusConflict},
		{"очередь недоступна", fmt.Errorf("%w: брокеры недоступны", domain.ErrQueueUnavailable), http.StatusServiceUnavailable},
		{"битая соль — вина сервера", domain.ErrBadSalt, http.StatusInternalServerError},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, voting := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, 0)
			voting.EXPECT().Accept(gomock.Any(), "final", testVoter, []uint8{1}).Return(tc.err)

			w := postVote(t, h, "final", `{"choices":[1],"voter":"`+testVoter+`"}`)
			assert.Equal(t, tc.status, w.Code, w.Body.String())
			if tc.status == http.StatusServiceUnavailable {
				assert.NotEmpty(t, w.Header().Get("Retry-After"), "клиенту нужно знать, когда повторить")
			}
		})
	}
}

func TestVote_MalformedBodyNeverReachesTheUseCase(t *testing.T) {
	t.Parallel()

	h, voting := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, 128)
	voting.EXPECT().Accept(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	for name, body := range map[string]string{
		"не JSON":         `{"choices":[1],`,
		"пустое тело":     ``,
		"слишком большое": fmt.Sprintf(`{"choices":[1],"voter":%q}`, strings.Repeat("x", 256)),
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, http.StatusBadRequest, postVote(t, h, "final", body).Code)
		})
	}
}

func TestPollConfig_IsCacheableAndCarriesNoPerRequestData(t *testing.T) {
	t.Parallel()

	h, _ := newPublic(t, hotConfig(t, domain.PollTypeMultiple, 1, 2), inWindow, 0)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/polls/final", nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Contains(t, w.Header().Get("Cache-Control"), "max-age",
		"конфиг одинаков для всех зрителей и обязан кэшироваться на CDN")

	var got map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.Equal(t, "кто победит?", got["question"])
	assert.Equal(t, []any{"первый", "второй", "третий"}, got["options"])
	assert.EqualValues(t, 1, got["min_choices"])
	assert.EqualValues(t, 2, got["max_choices"])
	assert.NotContains(t, got, "server_time",
		"время в кэшируемом ответе устаревает вместе с кэшем и ломает поправку часов у клиента")
}

func TestServerTime_IsNeverCachedAndReflectsTheClock(t *testing.T) {
	t.Parallel()

	h, _ := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, 0)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/time", nil))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, "no-store", w.Header().Get("Cache-Control"),
		"серверное время индивидуально для каждого запроса: ни CDN, ни браузер кэшировать не должны")

	var got struct {
		ServerTime string `json:"server_time"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, inWindow.Format(time.RFC3339), got.ServerTime,
		"клиент работает по серверному времени: у зрителя часы могут врать")
}

type adminFixture struct {
	handler http.Handler
	admin   *httpapi.AdminHandler
	polls   *mocks.MockPollManager
	auth    *mocks.MockAuthenticator
	token   string
}

func newAdminFixture(t *testing.T, role auth.Role) adminFixture {
	t.Helper()

	ctrl := gomock.NewController(t)
	polls := mocks.NewMockPollManager(ctrl)
	authn := mocks.NewMockAuthenticator(ctrl)

	tokens, err := auth.NewTokenService(adminKey, time.Hour)
	require.NoError(t, err)
	authn.EXPECT().Parse(gomock.Any()).DoAndReturn(tokens.Parse).AnyTimes()

	token, err := tokens.Issue(uuid.New(), role)
	require.NoError(t, err)

	h, err := httpapi.NewAdminHandler(polls, authn)
	require.NoError(t, err)

	return adminFixture{handler: httpapi.AdminRoutes(h), admin: h, polls: polls, auth: authn, token: token}
}

func (f adminFixture) do(t *testing.T, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	f.handler.ServeHTTP(w, r)
	return w
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

func samplePoll(status domain.Status) *domain.Poll {
	return &domain.Poll{
		ID: uuid.New(), Slug: "final", Question: "кто победит?", Type: domain.PollTypeSingle,
		Options: []domain.Option{{Idx: 0, Text: "первый"}, {Idx: 1, Text: "второй"}},
		Status:  status, OpensAt: opensAt, ClosesAt: closesAt, ShardCount: 500, Version: 1,
	}
}

func TestFR7_AdminEndpointRequiresJWT(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin)

	for _, path := range []string{"/polls", "/polls/final/results"} {
		assert.Equal(t, http.StatusUnauthorized, f.do(t, http.MethodGet, path, "", "").Code,
			"путь %s доступен без токена", path)
	}
	assert.Equal(t, http.StatusUnauthorized,
		f.do(t, http.MethodPost, "/polls", createBody("x", "single", []string{"а", "б"}), "").Code)
	assert.Equal(t, http.StatusUnauthorized,
		f.do(t, http.MethodGet, "/polls", "", "не-токен").Code)
}

func TestFR7_ViewerCannotWrite(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleViewer)
	f.polls.EXPECT().List(gomock.Any()).Return([]*domain.Poll{samplePoll(domain.StatusScheduled)}, nil).Times(1)
	f.polls.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	f.polls.EXPECT().Open(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	assert.Equal(t, http.StatusOK, f.do(t, http.MethodGet, "/polls", "", f.token).Code, "чтение доступно viewer")
	assert.Equal(t, http.StatusForbidden,
		f.do(t, http.MethodPost, "/polls", createBody("x", "single", []string{"а", "б"}), f.token).Code)
	assert.Equal(t, http.StatusForbidden, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)
}

func TestCreatePoll_DecodesTheSpecAndPassesTheActor(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor)

	var gotSpec domain.PollSpec
	f.polls.EXPECT().Create(gomock.Any(), gomock.Not(""), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, spec domain.PollSpec) (*domain.Poll, error) {
			gotSpec = spec
			return samplePoll(domain.StatusScheduled), nil
		})

	w := f.do(t, http.MethodPost, "/polls", createBody("final", "single", []string{"первый", "второй"}), f.token)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())

	assert.Equal(t, "final", gotSpec.Slug)
	assert.Equal(t, domain.PollTypeSingle, gotSpec.Type)
	assert.Equal(t, []string{"первый", "второй"}, gotSpec.Options)
	assert.Equal(t, inWindow.Add(2*time.Hour), gotSpec.OpensAt)

	var got struct {
		Slug       string   `json:"slug"`
		Status     string   `json:"status"`
		Options    []string `json:"options"`
		ShardCount uint16   `json:"shard_count"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "final", got.Slug)
	assert.Equal(t, string(domain.StatusScheduled), got.Status)
	assert.Equal(t, []string{"первый", "второй"}, got.Options)
	assert.EqualValues(t, 500, got.ShardCount)
}

func TestCreatePoll_RejectsUnparsableBodiesBeforeTheUseCase(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor)
	f.polls.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	for name, body := range map[string]string{
		"мусор вместо тела": `{"slug":`,
		"дата не RFC3339":   `{"slug":"a","type":"single","options":["а","б"],"opens_at":"завтра","closes_at":"послезавтра"}`,
	} {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, http.StatusBadRequest, f.do(t, http.MethodPost, "/polls", body, f.token).Code)
		})
	}
}

func TestAdmin_MapsUseCaseErrorsToStatuses(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		err    error
		status int
	}{
		{"невалидный опрос", domain.ErrInvalidPoll, http.StatusBadRequest},
		{"slug занят", domain.ErrSlugTaken, http.StatusConflict},
		{"переход не по FSM", domain.ErrBadTransition, http.StatusConflict},
		{"нет опроса", domain.ErrNotFound, http.StatusNotFound},
		{"конфликт версий", domain.ErrVersionConflict, http.StatusConflict},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newAdminFixture(t, auth.RoleEditor)
			f.polls.EXPECT().Create(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, tc.err)
			f.polls.EXPECT().Open(gomock.Any(), gomock.Any(), "final").Return(nil, tc.err)
			f.polls.EXPECT().Close(gomock.Any(), gomock.Any(), "final").Return(nil, tc.err)
			f.polls.EXPECT().Results(gomock.Any(), "final").Return(polls.Outcome{}, tc.err)

			assert.Equal(t, tc.status, f.do(t, http.MethodPost, "/polls", createBody("final", "single", []string{"а", "б"}), f.token).Code)
			assert.Equal(t, tc.status, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)
			assert.Equal(t, tc.status, f.do(t, http.MethodPost, "/polls/final/close", "", f.token).Code)
			assert.Equal(t, tc.status, f.do(t, http.MethodGet, "/polls/final/results", "", f.token).Code)
		})
	}
}

func TestOpenAndClose_RenderTheReturnedPoll(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor)
	opened := samplePoll(domain.StatusOpen)
	closed := samplePoll(domain.StatusOpen)
	closed.ClosesAt = inWindow
	f.polls.EXPECT().Open(gomock.Any(), gomock.Not(""), "final").Return(opened, nil)
	f.polls.EXPECT().Close(gomock.Any(), gomock.Not(""), "final").Return(closed, nil)

	w := f.do(t, http.MethodPost, "/polls/final/open", "", f.token)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `"status":"open"`)

	w = f.do(t, http.MethodPost, "/polls/final/close", "", f.token)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Contains(t, w.Body.String(), `"closes_at":"`+inWindow.Format(time.RFC3339)+`"`)
}

func TestResults_RendersPercentagesAndFinality(t *testing.T) {
	t.Parallel()

	poll := samplePoll(domain.StatusOpen)
	poll.Type = domain.PollTypeMultiple
	poll.Options = append(poll.Options, domain.Option{Idx: 2, Text: "третий"})

	f := newAdminFixture(t, auth.RoleViewer)
	f.polls.EXPECT().Results(gomock.Any(), "final").Return(polls.Outcome{
		Poll:      poll,
		Aggregate: domain.NewAggregateFrom(map[uint8]int64{0: 80, 1: 60, 2: 20}, 100),
		Final:     false,
	}, nil)

	w := f.do(t, http.MethodGet, "/polls/final/results", "", f.token)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got struct {
		Ballots int64 `json:"ballots"`
		Final   bool  `json:"final"`
		Options []struct {
			Text    string  `json:"text"`
			Votes   int64   `json:"votes"`
			Percent float64 `json:"percent"`
		} `json:"options"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	require.Len(t, got.Options, 3)
	assert.EqualValues(t, 100, got.Ballots)
	assert.EqualValues(t, 80, got.Options[0].Votes)
	assert.InDelta(t, 80.0, got.Options[0].Percent, 1e-9)
	assert.InDelta(t, 60.0, got.Options[1].Percent, 1e-9)
	assert.False(t, got.Final)
}

func TestAdminLogin(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin)
	f.auth.EXPECT().Login(gomock.Any(), "admin", "secret").Return(auth.Session{Token: "jwt", Role: auth.RoleAdmin}, nil)
	f.auth.EXPECT().Login(gomock.Any(), "admin", "нет").Return(auth.Session{}, auth.ErrInvalidCredentials)
	f.auth.EXPECT().Login(gomock.Any(), "нет", "secret").Return(auth.Session{}, auth.ErrInvalidCredentials)

	w := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"secret"}`, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.JSONEq(t, `{"token":"jwt","role":"admin"}`, w.Body.String())

	wrongPass := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"нет"}`, "")
	wrongUser := f.do(t, http.MethodPost, "/login", `{"login":"нет","password":"secret"}`, "")
	assert.Equal(t, http.StatusUnauthorized, wrongPass.Code)
	assert.Equal(t, wrongPass.Code, wrongUser.Code)
	assert.Equal(t, wrongPass.Body.String(), wrongUser.Body.String(),
		"ответ не имеет права выдавать, существует ли логин")

	assert.Equal(t, http.StatusBadRequest, f.do(t, http.MethodPost, "/login", `{"login":`, "").Code)
}

func okHealth() *health.Probes {
	return health.New(nil, nil)
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func liveAdvisor(t *testing.T) *mocks.MockCapacityAdvisor {
	t.Helper()

	advisor := mocks.NewMockCapacityAdvisor(gomock.NewController(t))
	advisor.EXPECT().Advise(gomock.Any()).Return(capacity.Advice{
		Phase:    capacity.PhaseLive,
		Reason:   "votes are being accepted",
		PollSlug: "final",
		Desired:  domain.Capacity{VoteAPI: 12, Consumers: 3, RedisMasters: 7, KafkaPartitions: 3},
	}, nil).AnyTimes()
	return advisor
}

func TestSnapshotRouter_ServesCapacityAdviceAndProbes(t *testing.T) {
	t.Parallel()

	r := httpapi.SnapshotRouter(okHealth(), liveAdvisor(t))

	w := get(t, r, "/internal/capacity")
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

	assert.Equal(t, http.StatusOK, get(t, r, "/readyz").Code)
	assert.Equal(t, http.StatusOK, get(t, r, "/livez").Code)
	assert.Equal(t, http.StatusOK, get(t, r, "/metrics").Code)
	assert.Equal(t, http.StatusNotFound, get(t, r, "/api/v1/time").Code, "снапшотер не принимает голоса")
}

func TestConsumerRouter_ExposesOnlyProbesAndMetrics(t *testing.T) {
	t.Parallel()

	r := httpapi.ConsumerRouter(okHealth())

	assert.Equal(t, http.StatusOK, get(t, r, "/readyz").Code)
	assert.Equal(t, http.StatusOK, get(t, r, "/livez").Code)
	assert.Equal(t, http.StatusOK, get(t, r, "/metrics").Code)
	assert.Equal(t, http.StatusNotFound, get(t, r, "/internal/capacity").Code)
	assert.Equal(t, http.StatusNotFound, get(t, r, "/api/v1/time").Code)
	assert.Equal(t, http.StatusNotFound, get(t, r, "/p/final").Code)
}

func TestAPIRouter_ServesProbesOutsideRequestMetrics(t *testing.T) {
	t.Parallel()

	seen := &routeLog{}
	public, _ := newPublicHandler(t, hotConfig(t, domain.PollTypeSingle, 1, 1), inWindow, 0)
	r := httpapi.APIRouter(okHealth(), public, newAdminFixture(t, auth.RoleAdmin).admin, httpapi.NewPages("https://vote.example"), httpapi.RouterConfig{
		VoteRateLimit:   100,
		AdminRateLimit:  100,
		RateWindow:      time.Minute,
		RequestObserver: seen,
		ServiceName:     "televote",
	})

	assert.Equal(t, http.StatusOK, get(t, r, "/readyz").Code)
	assert.Equal(t, http.StatusOK, get(t, r, "/metrics").Code)
	assert.Equal(t, http.StatusOK, get(t, r, "/api/v1/time").Code)
	assert.Equal(t, http.StatusOK, get(t, r, "/p/final").Code)
	assert.Equal(t, http.StatusNotFound, get(t, r, "/internal/capacity").Code, "ёмкость считает снапшотер, не API")

	assert.NotContains(t, seen.routes, "/readyz", "пробы живут вне middleware и не попадают в метрики запросов")
	assert.NotContains(t, seen.routes, "/metrics")
	assert.Contains(t, seen.routes, "/api/v1/time")
}

type routeLog struct {
	mu     sync.Mutex
	routes []string
}

func (l *routeLog) HTTPRequest(_, route, _ string, _ float64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.routes = append(l.routes, route)
}

func TestDebugRouter_ServesPprof(t *testing.T) {
	t.Parallel()

	assert.Equal(t, http.StatusOK, get(t, httpapi.DebugRouter(), "/debug/pprof/").Code)
	assert.Equal(t, http.StatusOK, get(t, httpapi.DebugRouter(), "/debug/pprof/cmdline").Code)
}

func fetchPage(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()

	w := httptest.NewRecorder()
	httpapi.PageRoutes(httpapi.NewPages("https://vote.example")).ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
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

func TestStatic_FaviconIsAnsweredWithoutHittingTheAPI(t *testing.T) {
	t.Parallel()

	w := fetchPage(t, "/favicon.ico")

	assert.Equal(t, http.StatusNoContent, w.Code, "браузер просит favicon на каждой загрузке: 404 — это 100 млн лишних ошибок в метриках")
	assert.Contains(t, w.Header().Get("Cache-Control"), "max-age", "ответ обязан кэшироваться, чтобы браузер не спрашивал снова")
}
