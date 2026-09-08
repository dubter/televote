package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const defaultCheckTimeout = 2 * time.Second

type Checker func(context.Context) error

var ErrNotAcceptingTraffic = errors.New("instance is not accepting traffic: warming up or shutting down")

type Option func(*health)

func WithTimeout(d time.Duration) Option {
	return func(h *health) {
		if d > 0 {
			h.timeout = d
		}
	}
}

type health struct {
	live    []Checker
	ready   []Checker
	timeout time.Duration
}

func Handler(live, ready []Checker, opts ...Option) http.Handler {
	h := &health{live: live, ready: ready, timeout: defaultCheckTimeout}
	for _, opt := range opts {
		opt(h)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, r *http.Request) {
		h.serve(w, r, h.live)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		h.serve(w, r, h.ready)
	})
	return mux
}

type Gate struct {
	ready atomic.Bool
}

func NewGate() *Gate { return &Gate{} }

func (g *Gate) SetReady(ready bool) { g.ready.Store(ready) }

func (g *Gate) Checker() Checker {
	return func(context.Context) error {
		if g.ready.Load() {
			return nil
		}
		return ErrNotAcceptingTraffic
	}
}

type healthResponse struct {
	Status string   `json:"status"`
	Errors []string `json:"errors,omitempty"`
}

func (h *health) serve(w http.ResponseWriter, r *http.Request, checkers []Checker) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, healthResponse{
			Status: "method_not_allowed",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()

	failures := runChecks(ctx, checkers)
	if len(failures) == 0 {
		writeJSON(w, http.StatusOK, healthResponse{Status: "ok"})
		return
	}

	reasons := make([]string, 0, len(failures))
	for _, err := range failures {
		reasons = append(reasons, err.Error())
	}
	writeJSON(w, http.StatusServiceUnavailable, healthResponse{
		Status: "unavailable",
		Errors: reasons,
	})
}

func runChecks(ctx context.Context, checkers []Checker) []error {
	if len(checkers) == 0 {
		return nil
	}

	results := make([]error, len(checkers))
	var wg sync.WaitGroup
	wg.Add(len(checkers))
	for i, check := range checkers {
		go func() {
			defer wg.Done()
			if check == nil {
				return
			}
			results[i] = check(ctx)
		}()
	}
	wg.Wait()

	failures := make([]error, 0, len(results))
	for _, err := range results {
		if err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}

func writeJSON(w http.ResponseWriter, status int, body healthResponse) {
	payload, err := json.Marshal(body)
	if err != nil {
		payload, status = []byte(`{"status":"unavailable"}`), http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)

	if _, err := w.Write(payload); err != nil {
		return
	}
}
