// Package capacity сообщает, сколько ресурсов нужно под ближайший эфир.
//
// Права на кластер остаются у KEDA: здесь только чистая функция от данных и
// HTTP-шов, ни одного вызова Kubernetes API. Голосующий путь не должен иметь
// возможности что-либо масштабировать.
package capacity

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dubter/televote/internal/domain"
)

// Phase — что сейчас происходит с ближайшим опросом.
type Phase string

const (
	// PhaseIdle — ближайший эфир дальше горизонта прогрева.
	PhaseIdle Phase = "idle"
	// PhasePrewarm — ёмкость поднимается заранее.
	PhasePrewarm Phase = "prewarm"
	// PhaseLive — идёт приём голосов.
	PhaseLive Phase = "live"
	// PhaseDrain — приём закрыт, подсчёт продолжается.
	PhaseDrain Phase = "drain"
)

// Polls — источник расписания. Календарь эфиров уже лежит в базе, поэтому
// отдельный планировщик не нужен.
type Polls interface {
	ListActive(ctx context.Context) ([]*domain.Poll, error)
}

// Lag сообщает, сколько сообщений ещё не обработано.
type Lag interface {
	Lag(ctx context.Context) (int64, error)
}

// Advisor считает желаемую ёмкость.
type Advisor struct {
	polls    Polls
	lag      Lag
	drain    time.Duration
	prewarm  time.Duration
	now      func() time.Time
	baseline domain.Capacity
}

// Config — параметры советчика.
type Config struct {
	// DrainWindow — за сколько мы согласны досчитать голоса. Главный рычаг
	// «стоимость против задержки результата».
	DrainWindow time.Duration
	// PrewarmLead — за сколько до открытия поднимать ёмкость.
	PrewarmLead time.Duration
	Now         func() time.Time
}

// New собирает советчика.
func New(polls Polls, lag Lag, cfg Config) (*Advisor, error) {
	if polls == nil {
		return nil, errors.New("capacity: не задан источник опросов")
	}
	if cfg.DrainWindow <= 0 {
		cfg.DrainWindow = 5 * time.Minute
	}
	if cfg.PrewarmLead <= 0 {
		cfg.PrewarmLead = time.Hour
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Advisor{
		polls: polls, lag: lag,
		drain: cfg.DrainWindow, prewarm: cfg.PrewarmLead, now: cfg.Now,
		baseline: domain.CapacityFor(0, cfg.DrainWindow),
	}, nil
}

// Advice — ответ советчика. Его читает external scaler KEDA.
type Advice struct {
	Phase    Phase           `json:"phase"`
	Reason   string          `json:"reason"`
	PollSlug string          `json:"poll_slug,omitempty"`
	OpensAt  string          `json:"opens_at,omitempty"`
	Lag      int64           `json:"consumer_lag"`
	Desired  domain.Capacity `json:"desired"`
}

// Advise считает желаемую ёмкость на текущий момент.
func (a *Advisor) Advise(ctx context.Context) (Advice, error) {
	polls, err := a.polls.ListActive(ctx)
	if err != nil {
		return Advice{}, err
	}

	var lag int64
	if a.lag != nil {
		// Ошибка чтения лага не должна ронять советчика: KEDA получит
		// консервативный ответ и не станет сносить ёмкость раньше времени.
		if v, lagErr := a.lag.Lag(ctx); lagErr == nil {
			lag = v
		} else {
			lag = 1
		}
	}

	now := a.now()
	advice := Advice{Phase: PhaseIdle, Reason: "ближайший эфир далеко", Lag: lag, Desired: a.baseline}

	for _, p := range polls {
		want := domain.CapacityFor(p.ExpectedVotes(), a.drain)

		switch {
		case p.Status == domain.StatusOpen && p.IsOpenAt(now):
			return Advice{
				Phase: PhaseLive, Reason: "идёт приём голосов",
				PollSlug: p.Slug, OpensAt: p.OpensAt.UTC().Format(time.RFC3339),
				Lag: lag, Desired: want,
			}, nil

		case p.Status == domain.StatusOpen && lag > 0:
			return Advice{
				Phase: PhaseDrain, Reason: "приём закрыт, идёт подсчёт",
				PollSlug: p.Slug, Lag: lag, Desired: want,
			}, nil

		case now.Add(a.prewarm).After(p.OpensAt) && now.Before(p.ClosesAt):
			advice = Advice{
				Phase: PhasePrewarm, Reason: "эфир скоро, поднимаем ёмкость",
				PollSlug: p.Slug, OpensAt: p.OpensAt.UTC().Format(time.RFC3339),
				Lag: lag, Desired: want,
			}
		}
	}
	return advice, nil
}

// Handler отдаёт совет по HTTP. Не публичный маршрут: его читает только KEDA.
func (a *Advisor) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		advice, err := a.Advise(r.Context())
		if err != nil {
			http.Error(w, `{"error":"internal"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		_ = json.NewEncoder(w).Encode(advice) //nolint:errcheck,errchkjson
	}
}
