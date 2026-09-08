package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/OWNER/televote/internal/domain"
	"github.com/OWNER/televote/pkg/httpx"

	"github.com/OWNER/televote/internal/pollcfg"
	"github.com/OWNER/televote/internal/producer"
	"github.com/OWNER/televote/internal/vote"
)

// maxVoteBody — голос это десятки байт. Всё, что заметно больше, либо мусор,
// либо попытка занять память приёма.
const maxVoteBody = 1024

// ConfigCache — источник конфига опроса. Интерфейс объявлен здесь, у
// потребителя, и нарочно узкий.
type ConfigCache interface {
	BySlug(slug string) (*pollcfg.HotConfig, bool)
}

// VoteSink принимает голос к обработке.
type VoteSink interface {
	Send(ctx context.Context, m producer.VoteMessage) error
}

// PublicHandler обслуживает зрителя: выдаёт конфиг опроса и принимает голоса.
//
// На этом пути нет ни Redis, ни Postgres. Всё, что нужно голосу, лежит в
// памяти процесса, а сам голос уезжает в Kafka — поэтому падение хранилищ
// задерживает результат, но не останавливает приём.
type PublicHandler struct {
	cache ConfigCache
	sink  VoteSink
	now   func() time.Time
}

// NewPublicHandler собирает обработчик приёма.
func NewPublicHandler(cache ConfigCache, sink VoteSink, now func() time.Time) (*PublicHandler, error) {
	if cache == nil {
		return nil, errors.New("httpapi: не задан кэш конфигов")
	}
	if sink == nil {
		return nil, errors.New("httpapi: не задан приёмник голосов")
	}
	if now == nil {
		now = time.Now
	}
	return &PublicHandler{cache: cache, sink: sink, now: now}, nil
}

// Routes отдаёт публичные маршруты.
func (h *PublicHandler) Routes() chi.Router {
	r := chi.NewRouter()
	r.Get("/polls/{slug}", h.pollConfig)
	r.Post("/polls/{slug}/vote", h.castVote)
	return r
}

// pollConfigResponse — то, что видит страница голосования.
type pollConfigResponse struct {
	Slug     string   `json:"slug"`
	Question string   `json:"question"`
	Options  []string `json:"options"`
	Type     string   `json:"type"`
	Min      uint8    `json:"min_choices"`
	Max      uint8    `json:"max_choices"`
	OpensAt  string   `json:"opens_at"`
	ClosesAt string   `json:"closes_at"`
	// ServerTime нужен клиенту, чтобы работать по нашему времени: у зрителя
	// часы могут врать, и без этого он либо не сможет проголосовать, либо
	// попробует до открытия.
	ServerTime string `json:"server_time"`
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

	// Конфиг одинаков для всех 30 млн зрителей, поэтому кэшируется на CDN.
	// Без этого 30 млн запросов за одним и тем же JSON придут на бэкенд и
	// убьют его раньше, чем голоса.
	w.Header().Set("Cache-Control", "public, max-age=10")

	writeJSON(w, http.StatusOK, pollConfigResponse{
		Slug:       cfg.Slug,
		Question:   cfg.Question,
		Options:    options,
		Type:       string(cfg.Rules.Type),
		Min:        cfg.Rules.MinChoices,
		Max:        cfg.Rules.MaxChoices,
		OpensAt:    cfg.Window.OpensAt.UTC().Format(time.RFC3339),
		ClosesAt:   cfg.Window.ClosesAt.UTC().Format(time.RFC3339),
		ServerTime: h.now().UTC().Format(time.RFC3339),
	})
}

// voteRequest — тело голоса. Voter генерит клиент, ключ дедупа выводит сервер.
type voteRequest struct {
	Choices []uint8 `json:"choices"`
	Voter   string  `json:"voter"`
}

// voteResponse — ответ приёма.
//
// Статус accepted, а не counted: голос принят к обработке, а посчитает его
// консьюмер. Ответить «посчитан» за то, что ещё лежит в Kafka, — это соврать
// клиенту, и на этом мы уже обжигались с серверным буфером.
type voteResponse struct {
	Status string `json:"status"`
}

func (h *PublicHandler) castVote(w http.ResponseWriter, r *http.Request) {
	cfg, ok := h.cache.BySlug(chi.URLParam(r, "slug"))
	if !ok {
		WriteError(w, r, errNotFound)
		return
	}

	var req voteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxVoteBody)).Decode(&req); err != nil {
		if errors.Is(err, io.EOF) || errors.As(err, new(*http.MaxBytesError)) {
			WriteError(w, r, fmt.Errorf("%w: тело запроса", errBadRequest))
			return
		}
		WriteError(w, r, fmt.Errorf("%w: %w", errBadRequest, err))
		return
	}

	if err := cfg.Rules.Validate(req.Choices); err != nil {
		WriteError(w, r, err)
		return
	}
	if !cfg.Window.IsOpenAt(h.now()) {
		WriteError(w, r, domain.ErrPollClosed)
		return
	}

	voterID, err := vote.DeriveVoterID(cfg.Salt, req.Voter)
	if err != nil {
		WriteError(w, r, err)
		return
	}

	addr := httpx.IPFromContext(r.Context())
	msg := producer.VoteMessage{
		PollID:     cfg.ID,
		VoterID:    voterID.Hex(),
		Choices:    req.Choices,
		Net16:      httpx.Net16(addr),
		UAClass:    httpx.UAClass(r.UserAgent()),
		ProducedAt: h.now().UTC(),
	}

	if err := h.sink.Send(r.Context(), msg); err != nil {
		// Kafka недоступна — единственный отказ, останавливающий приём.
		// Честный 503 с Retry-After, а не 200 за неучтённый голос.
		w.Header().Set("Retry-After", "1")
		WriteError(w, r, fmt.Errorf("%w: %w", errUnavailable, err))
		return
	}

	writeJSON(w, http.StatusAccepted, voteResponse{Status: "accepted"})
}
