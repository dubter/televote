// Package observability собирает телеметрию сервиса. Этот файл — только
// health-эндпоинты: они обязаны работать даже когда всё остальное сломано,
// поэтому не зависят ни от OTel, ни от логгера.
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

// defaultCheckTimeout — общий бюджет на все readiness-проверки.
// Два внешних PING (Redis и Postgres) обязаны укладываться в него вместе,
// а не каждый по отдельности: kubelet ждёт ответ, а не наши таймауты.
const defaultCheckTimeout = 2 * time.Second

// Checker — одна проверка. Возвращает nil, если зависимость отвечает.
// Обязан уважать ctx: без этого зависший PING держит /readyz до победного.
type Checker func(context.Context) error

// ErrNotAcceptingTraffic — инстанс сознательно снят с балансировки:
// либо ещё не прогрет, либо уже гасится.
var ErrNotAcceptingTraffic = errors.New("instance is not accepting traffic: warming up or shutting down")

// Option настраивает health-хендлер.
type Option func(*health)

// WithTimeout задаёт общий бюджет на readiness-проверки.
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

// Handler отдаёт http.Handler с /livez и /readyz.
//
// Разделение принципиальное: liveness отвечает на вопрос «процесс жив»,
// readiness — «можно слать трафик». Если повесить проверку Redis на liveness,
// падение Redis приведёт к перезапуску всех подов разом — ровно в тот момент,
// когда они нужнее всего.
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

// Gate — переключатель готовности инстанса. Готовность снимается до
// Shutdown, чтобы балансировщик увёл трафик раньше, чем сервер начнёт
// закрывать соединения; и до Warm, чтобы холодный кэш конфига не принимал голоса.
type Gate struct {
	ready atomic.Bool
}

// NewGate создаёт переключатель в состоянии «не готов».
func NewGate() *Gate { return &Gate{} }

// SetReady переключает готовность.
func (g *Gate) SetReady(ready bool) { g.ready.Store(ready) }

// Ready сообщает текущее состояние.
func (g *Gate) Ready() bool { return g.ready.Load() }

// Checker отдаёт проверку для /readyz.
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

// runChecks гоняет проверки параллельно: пять зависимостей по секунде каждая
// не имеют права сложиться в пятисекундный ответ.
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

	// Возвращаем все отказы: первая ошибка не должна скрывать остальные —
	// иначе дежурный чинит Redis, не зная, что лежит ещё и Postgres.
	failures := make([]error, 0, len(results))
	for _, err := range results {
		if err != nil {
			failures = append(failures, err)
		}
	}
	return failures
}

func writeJSON(w http.ResponseWriter, status int, body healthResponse) {
	// Тело сериализуем до WriteHeader: иначе отказ Marshal оставит клиенту
	// 200 с обрезанным телом, а это худший из возможных ответов health-ручки.
	payload, err := json.Marshal(body)
	if err != nil {
		payload, status = []byte(`{"status":"unavailable"}`), http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	// Кэшированный ответ health-эндпоинта показывает готовность мёртвого пода.
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)

	if _, err := w.Write(payload); err != nil {
		return // клиент отвалился: писать больше некуда и незачем
	}
}
