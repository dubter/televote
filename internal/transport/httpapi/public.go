package httpapi

//go:generate mockgen -source=public.go -destination=mocks/public.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/dubter/televote/internal/domain"

	"github.com/dubter/televote/internal/service/pollcfg"
)

type ConfigCache interface {
	BySlug(slug string) (*pollcfg.HotConfig, bool)
}

type VoteSink interface {
	Send(ctx context.Context, m domain.VoteMessage) error
}

type Observer interface {
	VoteAccepted()
	VoteRejected(reason string)
	ProduceSeconds(d float64)
}

const defaultMaxVoteBody = 1024

type PublicHandler struct {
	cache   ConfigCache
	sink    VoteSink
	obs     Observer
	now     func() time.Time
	maxBody int64
}

func NewPublicHandler(
	cache ConfigCache, sink VoteSink, obs Observer, now func() time.Time, maxBody int64,
) (*PublicHandler, error) {
	if cache == nil {
		return nil, errors.New("httpapi: poll config cache is required")
	}
	if sink == nil {
		return nil, errors.New("httpapi: vote sink is required")
	}
	if now == nil {
		now = time.Now
	}
	if obs == nil {
		obs = noopObserver{}
	}
	if maxBody <= 0 {
		maxBody = defaultMaxVoteBody
	}
	return &PublicHandler{cache: cache, sink: sink, obs: obs, now: now, maxBody: maxBody}, nil
}

func (h *PublicHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Get("/time", h.serverTime)
	r.Get("/polls/{slug}", h.pollConfig)
	r.Post("/polls/{slug}/vote", h.castVote)
	return r
}

type serverTimeResponse struct {
	ServerTime string `json:"server_time"`
}

func (h *PublicHandler) serverTime(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, serverTimeResponse{ServerTime: h.now().UTC().Format(time.RFC3339)})
}

type pollConfigResponse struct {
	Slug     string   `json:"slug"`
	Question string   `json:"question"`
	Options  []string `json:"options"`
	Type     string   `json:"type"`
	Min      uint8    `json:"min_choices"`
	Max      uint8    `json:"max_choices"`
	OpensAt  string   `json:"opens_at"`
	ClosesAt string   `json:"closes_at"`
}

func (h *PublicHandler) pollConfig(w http.ResponseWriter, r *http.Request) {
	cfg, ok := h.cache.BySlug(chi.URLParam(r, "slug"))
	if !ok {
		WriteError(w, r, errNotFound)
		return
	}

	options := make([]string, 0, len(cfg.Options))
	for _, o := range cfg.Options {
		options = append(options, o.Text)
	}

	w.Header().Set("Cache-Control", "public, max-age=10")

	writeJSON(w, http.StatusOK, pollConfigResponse{
		Slug:     cfg.Slug,
		Question: cfg.Question,
		Options:  options,
		Type:     string(cfg.Rules.Type),
		Min:      cfg.Rules.MinChoices,
		Max:      cfg.Rules.MaxChoices,
		OpensAt:  cfg.Window.OpensAt.UTC().Format(time.RFC3339),
		ClosesAt: cfg.Window.ClosesAt.UTC().Format(time.RFC3339),
	})
}

type voteRequest struct {
	Choices []uint8 `json:"choices"`
	Voter   string  `json:"voter"`
}

type voteResponse struct {
	Status string `json:"status"`
}

func (h *PublicHandler) castVote(w http.ResponseWriter, r *http.Request) {
	cfg, ok := h.cache.BySlug(chi.URLParam(r, "slug"))
	if !ok {
		h.obs.VoteRejected(reasonUnknownPoll)
		WriteError(w, r, errNotFound)
		return
	}

	var req voteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxBody)).Decode(&req); err != nil {
		h.obs.VoteRejected(reasonMalformed)
		if errors.Is(err, io.EOF) || errors.As(err, new(*http.MaxBytesError)) {
			WriteError(w, r, fmt.Errorf("%w: request body", errBadRequest))
			return
		}
		WriteError(w, r, fmt.Errorf("%w: %w", errBadRequest, err))
		return
	}

	if err := cfg.Rules.Validate(req.Choices); err != nil {
		h.obs.VoteRejected(reasonInvalidChoices)
		WriteError(w, r, err)
		return
	}
	if !cfg.Window.IsOpenAt(h.now()) {
		h.obs.VoteRejected(reasonPollClosed)
		WriteError(w, r, domain.ErrPollClosed)
		return
	}

	voterID, err := domain.DeriveVoterID(cfg.Salt, req.Voter)
	if err != nil {
		h.obs.VoteRejected(reasonBadVoter)
		WriteError(w, r, err)
		return
	}

	msg := domain.VoteMessage{
		PollID:     cfg.ID,
		VoterID:    voterID.Hex(),
		Choices:    req.Choices,
		ProducedAt: h.now().UTC(),
	}

	start := h.now()
	if err := h.sink.Send(r.Context(), msg); err != nil {
		h.obs.VoteRejected(reasonUnavailable)
		w.Header().Set("Retry-After", "1")
		WriteError(w, r, fmt.Errorf("%w: %w", errUnavailable, err))
		return
	}

	h.obs.ProduceSeconds(h.now().Sub(start).Seconds())
	h.obs.VoteAccepted()

	writeJSON(w, http.StatusAccepted, voteResponse{Status: "accepted"})
}

const (
	reasonUnknownPoll    = "unknown_poll"
	reasonMalformed      = "malformed"
	reasonInvalidChoices = "invalid_choices"
	reasonPollClosed     = "poll_closed"
	reasonBadVoter       = "bad_voter"
	reasonUnavailable    = "unavailable"
)

type noopObserver struct{}

func (noopObserver) VoteAccepted()          {}
func (noopObserver) VoteRejected(string)    {}
func (noopObserver) ProduceSeconds(float64) {}
