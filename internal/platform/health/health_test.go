package health_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/platform/health"
)

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec
}

func okChecker(context.Context) error { return nil }

func failingChecker(msg string) health.Checker {
	return func(context.Context) error { return errors.New(msg) }
}

func TestLivez_AlwaysOKWithoutCheckers(t *testing.T) {
	t.Parallel()

	h := health.Handler(nil, []health.Checker{failingChecker("redis down")})

	rec := get(t, h, "/livez")

	assert.Equal(t, http.StatusOK, rec.Code,
		"liveness не зависит от зависимостей: иначе оркестратор перезапустит здоровый под из-за чужого отказа")
}

func TestLivez_FailsWhenLivenessCheckerFails(t *testing.T) {
	t.Parallel()

	h := health.Handler([]health.Checker{failingChecker("deadlocked")}, nil)

	rec := get(t, h, "/livez")

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "deadlocked")
}

func TestReadyz_OKWhenEveryCheckerPasses(t *testing.T) {
	t.Parallel()

	h := health.Handler(nil, []health.Checker{okChecker, okChecker})

	rec := get(t, h, "/readyz")

	require.Equal(t, http.StatusOK, rec.Code)

	var body struct {
		Status string `json:"status"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ok", body.Status)
}

func TestReadyz_FailsWhenCheckerFails(t *testing.T) {
	t.Parallel()

	h := health.Handler(nil, []health.Checker{okChecker, failingChecker("postgres unreachable")})

	rec := get(t, h, "/readyz")

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "postgres unreachable")
}

func TestReadyz_ReportsEveryFailingChecker(t *testing.T) {
	t.Parallel()

	h := health.Handler(nil, []health.Checker{
		failingChecker("redis unreachable"),
		okChecker,
		failingChecker("postgres unreachable"),
	})

	rec := get(t, h, "/readyz")

	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	var body struct {
		Status string   `json:"status"`
		Errors []string `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "unavailable", body.Status)
	assert.Len(t, body.Errors, 2, "первая ошибка не должна скрывать остальные")
}

func TestReadyz_RunsCheckersConcurrently(t *testing.T) {
	t.Parallel()

	const n = 5
	slow := func(context.Context) error {
		time.Sleep(80 * time.Millisecond)
		return nil
	}
	checkers := make([]health.Checker, n)
	for i := range checkers {
		checkers[i] = slow
	}

	h := health.Handler(nil, checkers)

	start := time.Now()
	rec := get(t, h, "/readyz")
	elapsed := time.Since(start)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Less(t, elapsed, n*80*time.Millisecond/2, "чекеры выполнены последовательно")
}

func TestReadyz_TimesOutSlowChecker(t *testing.T) {
	t.Parallel()

	blocked := make(chan struct{})
	t.Cleanup(func() { close(blocked) })

	hang := func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-blocked:
			return nil
		}
	}

	h := health.Handler(nil, []health.Checker{hang}, health.WithTimeout(50*time.Millisecond))

	start := time.Now()
	rec := get(t, h, "/readyz")

	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Less(t, time.Since(start), time.Second, "зависший чекер обязан прерваться по таймауту")
}

func TestReadyz_PassesRequestContextToCheckers(t *testing.T) {
	t.Parallel()

	var deadlineSet atomic.Bool
	probe := func(ctx context.Context) error {
		_, ok := ctx.Deadline()
		deadlineSet.Store(ok)
		return nil
	}

	h := health.Handler(nil, []health.Checker{probe}, health.WithTimeout(time.Second))

	rec := get(t, h, "/readyz")

	require.Equal(t, http.StatusOK, rec.Code)
	assert.True(t, deadlineSet.Load(), "внешний вызов обязан идти с таймаутом")
}

func TestHandler_ResponsesAreNotCacheable(t *testing.T) {
	t.Parallel()

	h := health.Handler(nil, nil)

	for _, path := range []string{"/livez", "/readyz"} {
		rec := get(t, h, path)

		assert.Equal(t, http.StatusOK, rec.Code, path)
		assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"), path)
		assert.Equal(t, "application/json; charset=utf-8", rec.Header().Get("Content-Type"), path)
	}
}

func TestHandler_UnknownPathIsNotFound(t *testing.T) {
	t.Parallel()

	h := health.Handler(nil, nil)

	rec := get(t, h, "/healthz")

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_RejectsWriteMethods(t *testing.T) {
	t.Parallel()

	h := health.Handler(nil, nil)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/readyz", nil))

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	assert.Equal(t, "GET, HEAD", rec.Header().Get("Allow"))
}

func TestReadyz_GateFlipsToUnavailable(t *testing.T) {
	t.Parallel()

	gate := health.NewGate()
	h := health.Handler(nil, []health.Checker{gate.Checker()})

	assert.Equal(t, http.StatusServiceUnavailable, get(t, h, "/readyz").Code,
		"пока Warm не прошёл, инстанс не готов")

	gate.SetReady(true)
	assert.Equal(t, http.StatusOK, get(t, h, "/readyz").Code)

	gate.SetReady(false)
	rec := get(t, h, "/readyz")
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "shutting down")
}

func TestGate_IsRaceFree(t *testing.T) {
	t.Parallel()

	gate := health.NewGate()
	h := health.Handler(nil, []health.Checker{gate.Checker()})

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range 200 {
			gate.SetReady(i%2 == 0)
		}
	}()
	for range 200 {
		_ = get(t, h, "/readyz")
	}
	<-done
}
