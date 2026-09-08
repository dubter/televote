package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/dubter/televote/internal/auth"
	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/storage/postgres"
)

// PollStore — то, что админке нужно от хранилища опросов.
type PollStore interface {
	Create(ctx context.Context, p *domain.Poll) error
	GetBySlug(ctx context.Context, slug string) (*domain.Poll, error)
	ListActive(ctx context.Context) ([]*domain.Poll, error)
	Transition(ctx context.Context, id uuid.UUID, to domain.Status, version uint32) error
	HasCountedVotes(ctx context.Context, id uuid.UUID) (bool, error)
}

// ResultStore отдаёт результаты опроса.
type ResultStore interface {
	Get(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, error)
	GetAdjusted(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, []string, error)
}

// AdminStore — учётные записи и аудит.
type AdminStore interface {
	ByLogin(ctx context.Context, login string) (*postgres.Admin, error)
	Audit(ctx context.Context, actor, action, entity string, payload any) error
}

// AdminHandler обслуживает control plane: создание опросов и просмотр
// результатов. На горячем пути не участвует.
type AdminHandler struct {
	polls   PollStore
	results ResultStore
	admins  AdminStore
	tokens  *auth.TokenService
	limiter *auth.LoginLimiter
	now     func() time.Time

	// minLeadTime — насколько заранее обязан создаваться опрос: ёмкость под
	// эфир поднимается по расписанию и раньше просто не успеет. На стенде
	// ёмкость уже поднята, поэтому там значение нулевое.
	minLeadTime time.Duration
}

// NewAdminHandler собирает админский обработчик.
func NewAdminHandler(
	polls PollStore, results ResultStore, admins AdminStore,
	tokens *auth.TokenService, limiter *auth.LoginLimiter, now func() time.Time,
	minLeadTime time.Duration,
) (*AdminHandler, error) {
	switch {
	case polls == nil, results == nil, admins == nil:
		return nil, errors.New("httpapi: админке не хватает зависимостей")
	case tokens == nil:
		return nil, errors.New("httpapi: не задан сервис токенов")
	}
	if limiter == nil {
		limiter = auth.NewLoginLimiter(5, time.Minute, 10_000)
	}
	if now == nil {
		now = time.Now
	}
	return &AdminHandler{polls: polls, results: results, admins: admins,
		tokens: tokens, limiter: limiter, now: now, minLeadTime: minLeadTime}, nil
}

// Routes отдаёт админские маршруты. Логин открыт, всё остальное под токеном.
func (h *AdminHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Post("/login", h.login)

	r.Group(func(protected chi.Router) {
		protected.Use(h.requireRole(auth.RoleViewer))
		protected.Get("/polls", h.listPolls)
		protected.Get("/polls/{slug}/results", h.pollResults)
	})

	r.Group(func(editor chi.Router) {
		editor.Use(h.requireRole(auth.RoleEditor))
		editor.Post("/polls", h.createPoll)
		editor.Post("/polls/{slug}/open", h.openPoll)
		editor.Post("/polls/{slug}/close", h.closePoll)
	})
	return r
}

type claimsKeyType int

const claimsKey claimsKeyType = 0

// requireRole пропускает запрос только с токеном нужного уровня.
func (h *AdminHandler) requireRole(required auth.Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if raw == "" || raw == r.Header.Get("Authorization") {
				WriteError(w, r, errUnauthorized)
				return
			}

			claims, err := h.tokens.Parse(raw)
			if err != nil {
				WriteError(w, r, errUnauthorized)
				return
			}
			if !claims.Role.AtLeast(required) {
				WriteError(w, r, errForbidden)
				return
			}

			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey, claims)))
		})
	}
}

func claimsFrom(ctx context.Context) *auth.Claims {
	claims, ok := ctx.Value(claimsKey).(*auth.Claims)
	if !ok {
		return nil
	}
	return claims
}

type loginRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
	Role  string `json:"role"`
}

// login выдаёт токен.
//
// Ответ на неверный логин и на неверный пароль одинаков: разные ответы
// подсказали бы перебору, какие логины существуют.
func (h *AdminHandler) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		WriteError(w, r, fmt.Errorf("%w: тело запроса", errBadRequest))
		return
	}

	// Лимит до проверки пароля: argon2id стоит десятки миллисекунд CPU и
	// десятки мегабайт, и без лимита логин становится усилителем DoS.
	if !h.limiter.Allow(req.Login) {
		w.Header().Set("Retry-After", "60")
		WriteError(w, r, errUnauthorized)
		return
	}

	admin, err := h.admins.ByLogin(r.Context(), req.Login)
	if err != nil {
		WriteError(w, r, errUnauthorized)
		return
	}
	ok, err := auth.VerifyPassword(admin.PasswordHash, req.Password)
	if err != nil || !ok {
		WriteError(w, r, errUnauthorized)
		return
	}

	token, err := h.tokens.Issue(admin.ID, auth.Role(admin.Role))
	if err != nil {
		WriteError(w, r, err)
		return
	}
	h.limiter.Reset(req.Login)

	writeJSON(w, http.StatusOK, loginResponse{Token: token, Role: admin.Role})
}

type createPollRequest struct {
	Slug               string   `json:"slug"`
	Question           string   `json:"question"`
	Type               string   `json:"type"`
	Options            []string `json:"options"`
	MinChoices         uint8    `json:"min_choices"`
	MaxChoices         uint8    `json:"max_choices"`
	OpensAt            string   `json:"opens_at"`
	ClosesAt           string   `json:"closes_at"`
	ExpectedAudience   int64    `json:"expected_audience"`
	ExpectedConversion float64  `json:"expected_conversion"`
	RedisMasters       int      `json:"redis_masters"`
}

type pollResponse struct {
	ID         string   `json:"id"`
	Slug       string   `json:"slug"`
	Question   string   `json:"question"`
	Type       string   `json:"type"`
	Options    []string `json:"options"`
	Status     string   `json:"status"`
	OpensAt    string   `json:"opens_at"`
	ClosesAt   string   `json:"closes_at"`
	ShardCount uint16   `json:"shard_count"`
}

func (h *AdminHandler) createPoll(w http.ResponseWriter, r *http.Request) {
	var req createPollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64*1024)).Decode(&req); err != nil {
		WriteError(w, r, fmt.Errorf("%w: тело запроса", errBadRequest))
		return
	}

	spec, err := req.toSpec()
	if err != nil {
		WriteError(w, r, err)
		return
	}

	poll, err := domain.NewPoll(spec, h.now(), h.minLeadTime)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	if err := h.polls.Create(r.Context(), poll); err != nil {
		if errors.Is(err, postgres.ErrSlugTaken) {
			WriteError(w, r, errSlugTaken)
			return
		}
		WriteError(w, r, err)
		return
	}
	h.audit(r, "create_poll", poll.Slug, map[string]any{"question": poll.Question})

	writeJSON(w, http.StatusCreated, toPollResponse(poll))
}

// toSpec переводит запрос в спецификацию домена. Правила проверяет домен —
// здесь только перевод формата.
func (r createPollRequest) toSpec() (domain.PollSpec, error) {
	opensAt, err := time.Parse(time.RFC3339, r.OpensAt)
	if err != nil {
		return domain.PollSpec{}, fmt.Errorf("%w: opens_at не RFC3339", errBadRequest)
	}
	closesAt, err := time.Parse(time.RFC3339, r.ClosesAt)
	if err != nil {
		return domain.PollSpec{}, fmt.Errorf("%w: closes_at не RFC3339", errBadRequest)
	}

	return domain.PollSpec{
		Slug:               r.Slug,
		Question:           r.Question,
		Type:               domain.PollType(r.Type),
		Options:            r.Options,
		MinChoices:         r.MinChoices,
		MaxChoices:         r.MaxChoices,
		OpensAt:            opensAt,
		ClosesAt:           closesAt,
		ExpectedAudience:   r.ExpectedAudience,
		ExpectedConversion: r.ExpectedConversion,
		RedisMasters:       r.RedisMasters,
	}, nil
}

func (h *AdminHandler) listPolls(w http.ResponseWriter, r *http.Request) {
	polls, err := h.polls.ListActive(r.Context())
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := make([]pollResponse, 0, len(polls))
	for _, p := range polls {
		out = append(out, toPollResponse(p))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *AdminHandler) openPoll(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, domain.StatusOpen, "open_poll")
}

func (h *AdminHandler) closePoll(w http.ResponseWriter, r *http.Request) {
	h.transition(w, r, domain.StatusClosed, "close_poll")
}

// transition переводит опрос в новый статус через доменный FSM.
//
// Проверка идёт по автомату, а не по сравнению дат: вычисляемый статус зависел
// бы от часов машины, и опрос на инстансе с уехавшим временем принимал бы
// голоса вне эфира.
func (h *AdminHandler) transition(w http.ResponseWriter, r *http.Request, to domain.Status, action string) {
	slug := chi.URLParam(r, "slug")

	poll, err := h.polls.GetBySlug(r.Context(), slug)
	if err != nil {
		WriteError(w, r, errNotFound)
		return
	}
	if !poll.Status.CanTransitionTo(to) {
		WriteError(w, r, fmt.Errorf("%w: %s → %s", domain.ErrBadTransition, poll.Status, to))
		return
	}
	if err := h.polls.Transition(r.Context(), poll.ID, to, poll.Version); err != nil {
		WriteError(w, r, err)
		return
	}
	h.audit(r, action, slug, map[string]any{"from": string(poll.Status), "to": string(to)})

	poll.Status = to
	writeJSON(w, http.StatusOK, toPollResponse(poll))
}

type optionResult struct {
	Idx     uint8   `json:"idx"`
	Text    string  `json:"text"`
	Votes   int64   `json:"votes"`
	Percent float64 `json:"percent"`
}

type resultsResponse struct {
	Slug    string         `json:"slug"`
	Status  string         `json:"status"`
	Ballots int64          `json:"ballots"`
	Options []optionResult `json:"options"`
	// Final отличает промежуточный подсчёт от итогового: пока идёт дренаж,
	// цифры ещё растут, и админ обязан это видеть.
	Final        bool     `json:"final"`
	ExcludedNets []string `json:"excluded_nets,omitempty"`
}

func (h *AdminHandler) pollResults(w http.ResponseWriter, r *http.Request) {
	poll, err := h.polls.GetBySlug(r.Context(), chi.URLParam(r, "slug"))
	if err != nil {
		WriteError(w, r, errNotFound)
		return
	}

	final := poll.Status == domain.StatusClosed || poll.Status == domain.StatusArchived

	var (
		agg      domain.Aggregate
		excluded []string
	)
	if final {
		agg, excluded, err = h.results.GetAdjusted(r.Context(), poll.ID)
	} else {
		agg, err = h.results.Get(r.Context(), poll.ID)
	}
	if err != nil {
		WriteError(w, r, err)
		return
	}

	options := make([]optionResult, 0, len(poll.Options))
	for _, o := range poll.Options {
		options = append(options, optionResult{
			Idx:   o.Idx,
			Text:  o.Text,
			Votes: agg.Votes[o.Idx],
			// Процент считается от БЮЛЛЕТЕНЕЙ: при множественном выборе сумма
			// голосов больше числа проголосовавших, и деление на неё дало бы
			// цифры, которые нельзя показать в эфире.
			Percent: agg.Percent(o.Idx),
		})
	}

	writeJSON(w, http.StatusOK, resultsResponse{
		Slug:    poll.Slug,
		Status:  string(poll.Status),
		Ballots: agg.Ballots,
		Options: options,
		Final:   final, ExcludedNets: excluded,
	})
}

// audit пишет действие администратора. Ошибка записи не отменяет само
// действие, но и не остаётся незамеченной.
func (h *AdminHandler) audit(r *http.Request, action, entity string, payload any) {
	claims := claimsFrom(r.Context())
	actor := "unknown"
	if claims != nil {
		actor = claims.Subject
	}
	// Ошибка записи аудита не отменяет само действие, но и не остаётся
	// незамеченной: без записи в журнале действие выглядит несовершённым.
	if err := h.admins.Audit(r.Context(), actor, action, entity, payload); err != nil { //nolint:errcheck // ниже логируется
		slog.ErrorContext(r.Context(), "не удалось записать действие в аудит",
			slog.String("action", action), slog.String("entity", entity),
			slog.String("error", err.Error()))
	}
}

func toPollResponse(p *domain.Poll) pollResponse {
	options := make([]string, 0, len(p.Options))
	for _, o := range p.Options {
		options = append(options, o.Text)
	}
	return pollResponse{
		ID: p.ID.String(), Slug: p.Slug, Question: p.Question,
		Type: string(p.Type), Options: options, Status: string(p.Status),
		OpensAt:    p.OpensAt.UTC().Format(time.RFC3339),
		ClosesAt:   p.ClosesAt.UTC().Format(time.RFC3339),
		ShardCount: p.ShardCount,
	}
}
