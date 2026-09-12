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

type Probes struct {
	live    []Checker
	ready   []Checker
	timeout time.Duration
}

func New(live, ready []Checker) *Probes {
	return &Probes{live: live, ready: ready, timeout: defaultCheckTimeout}
}

func (p *Probes) Live(w http.ResponseWriter, r *http.Request) { p.serve(w, r, p.live) }

func (p *Probes) Ready(w http.ResponseWriter, r *http.Request) { p.serve(w, r, p.ready) }

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

func (p *Probes) serve(w http.ResponseWriter, r *http.Request, checkers []Checker) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		writeJSON(w, http.StatusMethodNotAllowed, healthResponse{
			Status: "method_not_allowed",
		})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), p.timeout)
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
