package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/OWNER/televote/internal/auth"
	"github.com/OWNER/televote/internal/domain"
	"github.com/OWNER/televote/internal/storage/postgres"
)

// PollStore — то, что админке нужно от хранилища опросов.
type PollStore interface {
	CreateRow(ctx context.Context, row *postgres.PollRow) error
	GetRowBySlug(ctx context.Context, slug string) (*postgres.PollRow, error)
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
func (h *AdminHandler) requireRole(min auth.Role) func(http.Handler) http.Handler {
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
			if !claims.Role.AtLeast(min) {
				WriteError(w, r, errForbidden)
				return
			}

			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), claimsKey, claims)))
		})
	}
}

func claimsFrom(ctx context.Context) *auth.Claims {
	claims, _ := ctx.Value(claimsKey).(*auth.Claims)
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

	row, err := h.buildPollRow(req)
	if err != nil {
		WriteError(w, r, err)
		return
	}

	if err := h.polls.CreateRow(r.Context(), row); err != nil {
		if errors.Is(err, postgres.ErrSlugTaken) {
			WriteError(w, r, errSlugTaken)
			return
		}
		WriteError(w, r, err)
		return
	}
	h.audit(r, "create_poll", row.Slug, map[string]any{"question": row.Question})

	writeJSON(w, http.StatusCreated, toPollResponse(&row.Poll))
}

// buildPollRow проверяет запрос и собирает строку опроса.
//
// Вынесено из хендлера: здесь одна ответственность — валидация и сборка, и
// её видно целиком, не продираясь через HTTP.
func (h *AdminHandler) buildPollRow(req createPollRequest) (*postgres.PollRow, error) {
	pollType := domain.PollType(req.Type)
	if !pollType.Valid() {
		return nil, fmt.Errorf("%w: неизвестный тип опроса %q", errBadRequest, req.Type)
	}
	if req.Slug == "" || req.Question == "" {
		return nil, fmt.Errorf("%w: slug и question обязательны", errBadRequest)
	}
	if len(req.Options) < 2 {
		return nil, fmt.Errorf("%w: нужно минимум два варианта", errBadRequest)
	}
	if len(req.Options) > domain.MaxOptions {
		return nil, fmt.Errorf("%w: вариантов больше %d", errBadRequest, domain.MaxOptions)
	}

	opensAt, err := time.Parse(time.RFC3339, req.OpensAt)
	if err != nil {
		return nil, fmt.Errorf("%w: opens_at не RFC3339", errBadRequest)
	}
	closesAt, err := time.Parse(time.RFC3339, req.ClosesAt)
	if err != nil {
		return nil, fmt.Errorf("%w: closes_at не RFC3339", errBadRequest)
	}
	if !closesAt.After(opensAt) {
		return nil, fmt.Errorf("%w: closes_at не позже opens_at", errBadRequest)
	}
	if h.minLeadTime > 0 && opensAt.Sub(h.now()) < h.minLeadTime {
		return nil, fmt.Errorf("%w: опрос открывается раньше чем через %s — ёмкость не успеет подняться",
			errBadRequest, h.minLeadTime)
	}

	options := make([]domain.Option, 0, len(req.Options))
	for i, text := range req.Options {
		if strings.TrimSpace(text) == "" {
			return nil, fmt.Errorf("%w: пустой вариант на позиции %d", errBadRequest, i)
		}
		options = append(options, domain.Option{Idx: uint8(i), Text: text})
	}

	minChoices, maxChoices := req.MinChoices, req.MaxChoices
	if pollType == domain.PollTypeSingle {
		minChoices, maxChoices = 1, 1
	}

	masters := req.RedisMasters
	if masters <= 0 {
		masters = 3
	}

	return &postgres.PollRow{
		Poll: domain.Poll{
			ID:         uuid.New(),
			Slug:       req.Slug,
			Question:   req.Question,
			Type:       pollType,
			Options:    options,
			MinChoices: minChoices,
			MaxChoices: maxChoices,
			Status:     domain.StatusScheduled,
			OpensAt:    opensAt,
			ClosesAt:   closesAt,
			ShardCount: domain.ShardCountFor(masters),
			Version:    1,
		},
		ExpectedAudience:   req.ExpectedAudience,
		ExpectedConversion: req.ExpectedConversion,
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

	row, err := h.polls.GetRowBySlug(r.Context(), slug)
	if err != nil {
		WriteError(w, r, errNotFound)
		return
	}
	if !row.Status.CanTransitionTo(to) {
		WriteError(w, r, fmt.Errorf("%w: %s → %s", domain.ErrBadTransition, row.Status, to))
		return
	}
	if err := h.polls.Transition(r.Context(), row.ID, to, row.Version); err != nil {
		WriteError(w, r, err)
		return
	}
	h.audit(r, action, slug, map[string]any{"from": string(row.Status), "to": string(to)})

	row.Status = to
	writeJSON(w, http.StatusOK, toPollResponse(&row.Poll))
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
	row, err := h.polls.GetRowBySlug(r.Context(), chi.URLParam(r, "slug"))
	if err != nil {
		WriteError(w, r, errNotFound)
		return
	}

	final := row.Status == domain.StatusClosed || row.Status == domain.StatusArchived

	var (
		agg      domain.Aggregate
		excluded []string
	)
	if final {
		agg, excluded, err = h.results.GetAdjusted(r.Context(), row.ID)
	} else {
		agg, err = h.results.Get(r.Context(), row.ID)
	}
	if err != nil {
		WriteError(w, r, err)
		return
	}

	options := make([]optionResult, 0, len(row.Options))
	for _, o := range row.Options {
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
		Slug:    row.Slug,
		Status:  string(row.Status),
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
	if err := h.admins.Audit(r.Context(), actor, action, entity, payload); err != nil {
		WriteError(nopWriter{}, r, err)
	}
}

// nopWriter нужен, чтобы переиспользовать логирование WriteError там, где
// ответ клиенту уже не отправляется.
type nopWriter struct{}

func (nopWriter) Header() http.Header         { return http.Header{} }
func (nopWriter) Write(b []byte) (int, error) { return len(b), nil }
func (nopWriter) WriteHeader(int)             {}

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
