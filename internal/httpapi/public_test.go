package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/httpapi"
	"github.com/dubter/televote/internal/pollcfg"
	"github.com/dubter/televote/internal/producer"
	"github.com/dubter/televote/pkg/httpx"
)

var testSalt = []byte("test-poll-salt-0123456789abcdef!")

type stubCache map[string]*pollcfg.HotConfig

func (s stubCache) BySlug(slug string) (*pollcfg.HotConfig, bool) {
	cfg, ok := s[slug]
	return cfg, ok
}

type stubSink struct {
	mu   sync.Mutex
	sent []producer.VoteMessage
	err  error
}

func (s *stubSink) Send(_ context.Context, m producer.VoteMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.err != nil {
		return s.err
	}
	s.sent = append(s.sent, m)
	return nil
}

func (s *stubSink) messages() []producer.VoteMessage {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]producer.VoteMessage, len(s.sent))
	copy(out, s.sent)
	return out
}

var (
	opensAt  = time.Date(2026, 9, 8, 20, 47, 30, 0, time.UTC)
	closesAt = opensAt.Add(time.Minute)
	inWindow = opensAt.Add(15 * time.Second)
)

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

func newPublic(t *testing.T, cfg *pollcfg.HotConfig, sink *stubSink, now time.Time) http.Handler {
	t.Helper()

	h, err := httpapi.NewPublicHandler(stubCache{cfg.Slug: cfg}, sink, nil, func() time.Time { return now })
	require.NoError(t, err)
	return httpx.ClientIP(nil)(h.Routes())
}

func postVote(t *testing.T, h http.Handler, slug, body string) *httptest.ResponseRecorder {
	t.Helper()

	r := httptest.NewRequest(http.MethodPost, "/polls/"+slug+"/vote", strings.NewReader(body))
	r.RemoteAddr = "203.0.113.42:1111"
	r.Header.Set("User-Agent", "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X)")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestFR3_VoteWithoutRegistration(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), sink, inWindow)

	w := postVote(t, h, "final", `{"choices":[1],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())

	var got struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &got))
	assert.Equal(t, "accepted", got.Status)

	sent := sink.messages()
	require.Len(t, sent, 1)
	assert.Equal(t, []uint8{1}, sent[0].Choices)
	assert.Len(t, sent[0].VoterID, 32, "в Kafka уезжает выведенный сервером ключ")
	assert.NotContains(t, sent[0].VoterID, "9b2f4c6e", "присланное клиентом значение не уезжает как есть")
	assert.Equal(t, "203.0.0.0/16", sent[0].Net16, "адрес не уезжает, уезжает подсеть")
	assert.Equal(t, "iOS 18", sent[0].UAClass)
	assert.False(t, sent[0].ProducedAt.IsZero(), "по этой метке консьюмер проверит окно")
}

func TestFR3_RejectVoteWithoutVoter(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), sink, inWindow)

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
	assert.Empty(t, sink.messages(), "негодный голос не имеет права попасть в Kafka")
}

func TestFR1_2_RejectChoicesOutsideRules(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newPublic(t, hotConfig(t, domain.PollTypeMultiple, 2, 2), sink, inWindow)

	cases := map[string]string{
		"ниже MinChoices":     `{"choices":[0],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`,
		"выше MaxChoices":     `{"choices":[0,1,2],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`,
		"индекс за пределами": `{"choices":[0,9],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`,
		"дубль индекса":       `{"choices":[1,1],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`,
		"пустой выбор":        `{"choices":[],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`,
	}

	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			w := postVote(t, h, "final", body)
			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		})
	}
	assert.Empty(t, sink.messages())
}

func TestFR8_VoteOutsideWindowIsRejected(t *testing.T) {
	t.Parallel()

	const body = `{"choices":[1],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`

	cases := map[string]time.Time{
		"до открытия":      opensAt.Add(-time.Second),
		"ровно в закрытие": closesAt,
		"после закрытия":   closesAt.Add(time.Minute),
	}

	for name, now := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			sink := &stubSink{}
			w := postVote(t, newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), sink, now), "final", body)

			assert.Equal(t, http.StatusConflict, w.Code, w.Body.String())
			assert.Empty(t, sink.messages())
		})
	}
}

func TestNFR9_BodySizeLimitEnforced(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), sink, inWindow)

	body := fmt.Sprintf(`{"choices":[1],"voter":%q}`, strings.Repeat("x", 4096))
	w := postVote(t, h, "final", body)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Empty(t, sink.messages())
}

func TestVote_KafkaDownReturns503(t *testing.T) {
	t.Parallel()

	sink := &stubSink{err: errors.New("брокеры недоступны")}
	h := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), sink, inWindow)

	w := postVote(t, h, "final", `{"choices":[1],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`)

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.NotEmpty(t, w.Header().Get("Retry-After"))
}

func TestVote_UnknownPollIsNotFound(t *testing.T) {
	t.Parallel()

	sink := &stubSink{}
	h := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), sink, inWindow)

	w := postVote(t, h, "нет-такого", `{"choices":[1],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`)
	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestPollConfig_ContainsServerTimeAndCacheHeader(t *testing.T) {
	t.Parallel()

	h := newPublic(t, hotConfig(t, domain.PollTypeMultiple, 1, 2), &stubSink{}, inWindow)

	r := httptest.NewRequest(http.MethodGet, "/polls/final", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

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

	sink := &stubSink{}
	h := newPublic(t, hotConfig(t, domain.PollTypeSingle, 1, 1), sink, inWindow)

	const body = `{"choices":[1],"voter":"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"}`
	require.Equal(t, http.StatusAccepted, postVote(t, h, "final", body).Code)
	require.Equal(t, http.StatusAccepted, postVote(t, h, "final", body).Code)

	sent := sink.messages()
	require.Len(t, sent, 2, "приём не дедуплицирует — это делает консьюмер")
	assert.Equal(t, sent[0].VoterID, sent[1].VoterID)
}
