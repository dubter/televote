package httpapi

//go:generate mockgen -source=admin.go -destination=mocks/admin.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/auth"
	"github.com/dubter/televote/internal/service/polls"
)

type PollManager interface {
	Create(ctx context.Context, actor string, spec domain.PollSpec) (*domain.Poll, error)
	List(ctx context.Context) ([]*domain.Poll, error)
	Open(ctx context.Context, actor, slug string) (*domain.Poll, error)
	Close(ctx context.Context, actor, slug string) (*domain.Poll, error)
	Results(ctx context.Context, slug string) (polls.Outcome, error)
}

type Authenticator interface {
	Login(ctx context.Context, login, password string) (auth.Session, error)
	Parse(raw string) (*auth.Claims, error)
}

const (
	maxLoginBody      = 4 * 1024
	maxCreatePollBody = 64 * 1024
)

type AdminHandler struct {
	polls PollManager
	auth  Authenticator
}

func NewAdminHandler(manager PollManager, authn Authenticator) (*AdminHandler, error) {
	switch {
	case manager == nil:
		return nil, errors.New("httpapi: poll manager is required")
	case authn == nil:
		return nil, errors.New("httpapi: authenticator is required")
	}
	return &AdminHandler{polls: manager, auth: authn}, nil
}

type claimsKeyType int

const claimsKey claimsKeyType = 0

func (h *AdminHandler) requireRole(required auth.Role) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || raw == "" {
				WriteError(w, r, errUnauthorized)
				return
			}

			claims, err := h.auth.Parse(raw)
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

func actorFrom(ctx context.Context) string {
	claims, ok := ctx.Value(claimsKey).(*auth.Claims)
	if !ok {
		return "unknown"
	}
	return claims.Subject
}

type loginRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

type loginResponse struct {
	Token string `json:"token"`
	Role  string `json:"role"`
}

func (h *AdminHandler) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxLoginBody)).Decode(&req); err != nil {
		WriteError(w, r, fmt.Errorf("%w: request body", errBadRequest))
		return
	}

	session, err := h.auth.Login(r.Context(), req.Login, req.Password)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, loginResponse{Token: session.Token, Role: string(session.Role)})
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

func (r createPollRequest) toSpec() (domain.PollSpec, error) {
	opensAt, err := time.Parse(time.RFC3339, r.OpensAt)
	if err != nil {
		return domain.PollSpec{}, fmt.Errorf("%w: opens_at is not RFC3339", errBadRequest)
	}
	closesAt, err := time.Parse(time.RFC3339, r.ClosesAt)
	if err != nil {
		return domain.PollSpec{}, fmt.Errorf("%w: closes_at is not RFC3339", errBadRequest)
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

func (h *AdminHandler) createPoll(w http.ResponseWriter, r *http.Request) {
	var req createPollRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxCreatePollBody)).Decode(&req); err != nil {
		WriteError(w, r, fmt.Errorf("%w: request body", errBadRequest))
		return
	}
	spec, err := req.toSpec()
	if err != nil {
		WriteError(w, r, err)
		return
	}

	poll, err := h.polls.Create(r.Context(), actorFrom(r.Context()), spec)
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, toPollResponse(poll))
}

func (h *AdminHandler) listPolls(w http.ResponseWriter, r *http.Request) {
	list, err := h.polls.List(r.Context())
	if err != nil {
		WriteError(w, r, err)
		return
	}

	out := make([]pollResponse, 0, len(list))
	for _, p := range list {
		out = append(out, toPollResponse(p))
	}
	writeJSON(w, http.StatusOK, out)
}

func (h *AdminHandler) openPoll(w http.ResponseWriter, r *http.Request) {
	poll, err := h.polls.Open(r.Context(), actorFrom(r.Context()), chi.URLParam(r, "slug"))
	if err != nil {
		WriteError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toPollResponse(poll))
}

func (h *AdminHandler) closePoll(w http.ResponseWriter, r *http.Request) {
	poll, err := h.polls.Close(r.Context(), actorFrom(r.Context()), chi.URLParam(r, "slug"))
	if err != nil {
		WriteError(w, r, err)
		return
	}
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
	Final   bool           `json:"final"`
}

func (h *AdminHandler) pollResults(w http.ResponseWriter, r *http.Request) {
	out, err := h.polls.Results(r.Context(), chi.URLParam(r, "slug"))
	if err != nil {
		WriteError(w, r, err)
		return
	}

	options := make([]optionResult, 0, len(out.Poll.Options))
	for _, o := range out.Poll.Options {
		options = append(options, optionResult{
			Idx:     o.Idx,
			Text:    o.Text,
			Votes:   out.Aggregate.Votes[o.Idx],
			Percent: out.Aggregate.Percent(o.Idx),
		})
	}

	writeJSON(w, http.StatusOK, resultsResponse{
		Slug:    out.Poll.Slug,
		Status:  string(out.Poll.Status),
		Ballots: out.Aggregate.Ballots,
		Options: options,
		Final:   out.Final,
	})
}
