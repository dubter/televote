package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/auth"
	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/httpapi"
	"github.com/dubter/televote/internal/storage/postgres"
)

var adminKey = []byte("admin-test-key-0123456789abcdef!")

type fakePolls struct {
	mu        sync.Mutex
	bySlug    map[string]*domain.Poll
	hasVotes  bool
	createErr error
}

func newFakePolls(polls ...*domain.Poll) *fakePolls {
	f := &fakePolls{bySlug: map[string]*domain.Poll{}}
	for _, p := range polls {
		f.bySlug[p.Slug] = p
	}
	return f
}

func (f *fakePolls) Create(_ context.Context, p *domain.Poll) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createErr != nil {
		return f.createErr
	}
	if _, exists := f.bySlug[p.Slug]; exists {
		return postgres.ErrSlugTaken
	}
	f.bySlug[p.Slug] = p
	return nil
}

func (f *fakePolls) GetBySlug(_ context.Context, slug string) (*domain.Poll, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	p, ok := f.bySlug[slug]
	if !ok {
		return nil, postgres.ErrNotFound
	}
	return p, nil
}

func (f *fakePolls) ListActive(context.Context) ([]*domain.Poll, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]*domain.Poll, 0, len(f.bySlug))
	for _, p := range f.bySlug {
		out = append(out, p)
	}
	return out, nil
}

func (f *fakePolls) Transition(_ context.Context, id uuid.UUID, to domain.Status, _ uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, p := range f.bySlug {
		if p.ID == id {
			p.Status = to
			return nil
		}
	}
	return postgres.ErrNotFound
}

func (f *fakePolls) HasCountedVotes(context.Context, uuid.UUID) (bool, error) {
	return f.hasVotes, nil
}

type fakeResults struct {
	agg      domain.Aggregate
	adjusted domain.Aggregate
	excluded []string
}

func (f *fakeResults) Get(context.Context, uuid.UUID) (domain.Aggregate, error) {
	return f.agg, nil
}

func (f *fakeResults) GetAdjusted(context.Context, uuid.UUID) (domain.Aggregate, []string, error) {
	return f.adjusted, f.excluded, nil
}

type fakeAdmins struct {
	mu    sync.Mutex
	admin *postgres.Admin
	audit []string
}

func (f *fakeAdmins) ByLogin(_ context.Context, login string) (*postgres.Admin, error) {
	if f.admin == nil || f.admin.Login != login {
		return nil, postgres.ErrNotFound
	}
	return f.admin, nil
}

func (f *fakeAdmins) Audit(_ context.Context, _, action, entity string, _ any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.audit = append(f.audit, action+":"+entity)
	return nil
}

func (f *fakeAdmins) actions() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.audit))
	copy(out, f.audit)
	return out
}

type adminFixture struct {
	handler http.Handler
	polls   *fakePolls
	results *fakeResults
	admins  *fakeAdmins
	token   string
}

func newAdminFixture(t *testing.T, role auth.Role, polls *fakePolls, results *fakeResults) adminFixture {
	t.Helper()

	hash, err := auth.HashPassword("secret")
	require.NoError(t, err)

	userID := uuid.New()
	admins := &fakeAdmins{admin: &postgres.Admin{ID: userID, Login: "admin", PasswordHash: hash, Role: string(role)}}

	tokens, err := auth.NewTokenService(adminKey, time.Hour)
	require.NoError(t, err)
	token, err := tokens.Issue(userID, role)
	require.NoError(t, err)

	h, err := httpapi.NewAdminHandler(polls, results, admins, tokens,
		auth.NewLoginLimiter(3, time.Minute, 100), func() time.Time { return inWindow }, 0)
	require.NoError(t, err)

	return adminFixture{handler: h.Routes(), polls: polls, results: results, admins: admins, token: token}
}

func (f adminFixture) do(t *testing.T, method, path, body, token string) *httptest.ResponseRecorder {
	t.Helper()

	var rdr *strings.Reader
	if body == "" {
		rdr = strings.NewReader("")
	} else {
		rdr = strings.NewReader(body)
	}

	r := httptest.NewRequest(method, path, rdr)
	r.Header.Set("Content-Type", "application/json")
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

func TestFR7_AdminEndpointRequiresJWT(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin, newFakePolls(), &fakeResults{})

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

	f := newAdminFixture(t, auth.RoleEditor, newFakePolls(), &fakeResults{})

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
	assert.Contains(t, f.admins.actions(), "create_poll:final", "создание обязано попасть в аудит")
}

func TestFR1_RejectsInvalidPolls(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleEditor, newFakePolls(), &fakeResults{})

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

	f := newAdminFixture(t, auth.RoleEditor, newFakePolls(), &fakeResults{})
	body := createBody("final", "single", []string{"а", "б"})

	require.Equal(t, http.StatusCreated, f.do(t, http.MethodPost, "/polls", body, f.token).Code)
	assert.Equal(t, http.StatusConflict, f.do(t, http.MethodPost, "/polls", body, f.token).Code)
}

func TestCreatePoll_RejectsTooShortLeadTime(t *testing.T) {
	t.Parallel()

	tokens, err := auth.NewTokenService(adminKey, time.Hour)
	require.NoError(t, err)
	userID := uuid.New()
	token, err := tokens.Issue(userID, auth.RoleEditor)
	require.NoError(t, err)

	hash, err := auth.HashPassword("secret")
	require.NoError(t, err)
	admins := &fakeAdmins{admin: &postgres.Admin{ID: userID, Login: "admin", PasswordHash: hash, Role: "editor"}}

	h, err := httpapi.NewAdminHandler(newFakePolls(), &fakeResults{}, admins, tokens,
		auth.NewLoginLimiter(3, time.Minute, 100), func() time.Time { return inWindow }, time.Hour)
	require.NoError(t, err)

	opens := inWindow.Add(10 * time.Minute).Format(time.RFC3339)
	closes := inWindow.Add(11 * time.Minute).Format(time.RFC3339)
	body, _ := json.Marshal(map[string]any{
		"slug": "soon", "question": "?", "type": "single", "options": []string{"а", "б"},
		"opens_at": opens, "closes_at": closes,
	})

	r := httptest.NewRequest(http.MethodPost, "/polls", strings.NewReader(string(body)))
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	h.Routes().ServeHTTP(w, r)

	assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
}

func TestFR6_TransitionsFollowFSM(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusScheduled,
		Options: []domain.Option{{Idx: 0, Text: "а"}}, Version: 1,
	}
	f := newAdminFixture(t, auth.RoleEditor, newFakePolls(poll), &fakeResults{})

	require.Equal(t, http.StatusOK, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)
	assert.Equal(t, domain.StatusOpen, poll.Status)

	assert.Equal(t, http.StatusConflict, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)

	require.Equal(t, http.StatusOK, f.do(t, http.MethodPost, "/polls/final/close", "", f.token).Code)
	assert.Equal(t, domain.StatusClosed, poll.Status)

	assert.Equal(t, http.StatusConflict, f.do(t, http.MethodPost, "/polls/final/open", "", f.token).Code)

	assert.Equal(t, []string{"open_poll:final", "close_poll:final"}, f.admins.actions())
}

func TestFR7_ViewerCannotWrite(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{ID: uuid.New(), Slug: "final", Status: domain.StatusScheduled, Version: 1}
	f := newAdminFixture(t, auth.RoleViewer, newFakePolls(poll), &fakeResults{})

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
	results := &fakeResults{agg: domain.Aggregate{
		Votes: map[uint8]int64{0: 80, 1: 60, 2: 20}, Ballots: 100,
	}}
	f := newAdminFixture(t, auth.RoleViewer, newFakePolls(poll), results)

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

func TestFR5_ClosedPollReturnsAdjustedResult(t *testing.T) {
	t.Parallel()

	poll := &domain.Poll{
		ID: uuid.New(), Slug: "final", Status: domain.StatusClosed, Version: 1,
		Options: []domain.Option{{Idx: 0, Text: "а"}},
	}
	results := &fakeResults{
		agg:      domain.Aggregate{Votes: map[uint8]int64{0: 500}, Ballots: 500},
		adjusted: domain.Aggregate{Votes: map[uint8]int64{0: 460}, Ballots: 460},
		excluded: []string{"203.0.0.0/16"},
	}
	f := newAdminFixture(t, auth.RoleViewer, newFakePolls(poll), results)

	w := f.do(t, http.MethodGet, "/polls/final/results", "", f.token)
	require.Equal(t, http.StatusOK, w.Code)

	var got struct {
		Ballots      int64    `json:"ballots"`
		Final        bool     `json:"final"`
		ExcludedNets []string `json:"excluded_nets"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))

	assert.EqualValues(t, 460, got.Ballots)
	assert.True(t, got.Final)
	assert.Equal(t, []string{"203.0.0.0/16"}, got.ExcludedNets)
}

func TestAdminLogin(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin, newFakePolls(), &fakeResults{})

	w := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"secret"}`, "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())

	var got struct {
		Token string `json:"token"`
		Role  string `json:"role"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.NotEmpty(t, got.Token)
	assert.Equal(t, "admin", got.Role)

	wrongPass := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"нет"}`, "")
	wrongUser := f.do(t, http.MethodPost, "/login", `{"login":"нет","password":"secret"}`, "")
	assert.Equal(t, http.StatusUnauthorized, wrongPass.Code)
	assert.Equal(t, wrongPass.Code, wrongUser.Code)
	assert.Equal(t, wrongPass.Body.String(), wrongUser.Body.String())
}

func TestAdminLogin_IsRateLimited(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleAdmin, newFakePolls(), &fakeResults{})

	for range 3 {
		f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"нет"}`, "")
	}
	w := f.do(t, http.MethodPost, "/login", `{"login":"admin","password":"secret"}`, "")

	assert.Equal(t, http.StatusUnauthorized, w.Code, "лимит попыток не сработал")
	assert.NotEmpty(t, w.Header().Get("Retry-After"))
}

func TestResults_UnknownPollIsNotFound(t *testing.T) {
	t.Parallel()

	f := newAdminFixture(t, auth.RoleViewer, newFakePolls(), &fakeResults{})
	assert.Equal(t, http.StatusNotFound, f.do(t, http.MethodGet, "/polls/нет/results", "", f.token).Code)
}
