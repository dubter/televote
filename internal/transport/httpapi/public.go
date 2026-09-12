package httpapi

//go:generate mockgen -source=public.go -destination=mocks/public.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/dubter/televote/internal/service/pollcfg"
)

type Voting interface {
	Accept(ctx context.Context, slug, clientID string, choices []uint8) error
}

type ConfigLookup interface {
	BySlug(slug string) (*pollcfg.HotConfig, bool)
}

const defaultMaxVoteBody = 1024

type PublicHandler struct {
	voting  Voting
	configs ConfigLookup
	now     func() time.Time
	maxBody int64
}

func NewPublicHandler(voting Voting, configs ConfigLookup, now func() time.Time, maxBody int64) (*PublicHandler, error) {
	switch {
	case voting == nil:
		return nil, errors.New("httpapi: voting use case is required")
	case configs == nil:
		return nil, errors.New("httpapi: poll config lookup is required")
	case now == nil:
		return nil, errors.New("httpapi: clock is required")
	}
	if maxBody <= 0 {
		maxBody = defaultMaxVoteBody
	}
	return &PublicHandler{voting: voting, configs: configs, now: now, maxBody: maxBody}, nil
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
	cfg, ok := h.configs.BySlug(chi.URLParam(r, "slug"))
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
	var req voteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, h.maxBody)).Decode(&req); err != nil {
		WriteError(w, r, fmt.Errorf("%w: request body", errBadRequest))
		return
	}

	if err := h.voting.Accept(r.Context(), chi.URLParam(r, "slug"), req.Voter, req.Choices); err != nil {
		WriteError(w, r, err)
		return
	}

	writeJSON(w, http.StatusAccepted, voteResponse{Status: "accepted"})
}
