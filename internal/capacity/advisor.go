package capacity

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/dubter/televote/internal/domain"
)

type Phase string

const (
	PhaseIdle    Phase = "idle"
	PhasePrewarm Phase = "prewarm"
	PhaseLive    Phase = "live"
	PhaseDrain   Phase = "drain"
)

type Polls interface {
	ListActive(ctx context.Context) ([]*domain.Poll, error)
}

type Lag interface {
	Lag(ctx context.Context) (int64, error)
}

type Advisor struct {
	polls    Polls
	lag      Lag
	drain    time.Duration
	prewarm  time.Duration
	now      func() time.Time
	baseline domain.Capacity
}

type Config struct {
	DrainWindow time.Duration
	PrewarmLead time.Duration
	Now         func() time.Time
}

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

type Advice struct {
	Phase    Phase           `json:"phase"`
	Reason   string          `json:"reason"`
	PollSlug string          `json:"poll_slug,omitempty"`
	OpensAt  string          `json:"opens_at,omitempty"`
	Lag      int64           `json:"consumer_lag"`
	Desired  domain.Capacity `json:"desired"`
}

func (a *Advisor) Advise(ctx context.Context) (Advice, error) {
	polls, err := a.polls.ListActive(ctx)
	if err != nil {
		return Advice{}, err
	}

	var lag int64
	if a.lag != nil {
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
