package httpx

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"net/netip"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/go-chi/httprate"
)

// RateLimit ограничивает ЧАСТОТУ запросов, а не их количество.
//
// Разница принципиальная: за одним IP мобильного оператора (CGNAT) сидят сотни
// тысяч зрителей, а зритель ТВ со смартфоном — это и есть целевая аудитория.
// Лимит количества отрезал бы именно её.
//
// Ключ считает LimitKey: для IPv6 это префикс /64, иначе защиты нет вовсе.
func RateLimit(perWindow int, window time.Duration) func(http.Handler) http.Handler {
	if window <= 0 {
		window = time.Minute
	}
	retryAfter := strconv.Itoa(int(window.Seconds()))

	return httprate.LimitBy(
		perWindow,
		window,
		func(r *http.Request) (string, error) {
			// Ключ считает LimitKey поверх адреса, который положил ClientIP:
			// готовые key-функции httprate берут заголовки как есть.
			return LimitKey(IPFromContext(r.Context())), nil
		},
		httprate.WithLimitHandler(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", retryAfter)
			writeError(w, http.StatusTooManyRequests, "rate_limited")
		}),
	)
}

// BlockDatacenterASN отвергает запросы из датацентровых диапазонов.
//
// Зритель ТВ голосует с мобильного или домашнего адреса; голос с AWS, Hetzner
// или DigitalOcean по определению не зритель. Правило категориальное, а не
// статистическое, поэтому ложных срабатываний по региону быть не может —
// в отличие от порога «подсеть дала слишком много голосов», который отсёк бы
// крупнейшего легального оператора.
func BlockDatacenterASN(ranges []netip.Prefix) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		if len(ranges) == 0 {
			return next
		}
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if addr := IPFromContext(r.Context()); addr.IsValid() {
				for _, prefix := range ranges {
					if prefix.Contains(addr) {
						writeError(w, http.StatusForbidden, "blocked")
						return
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Recovery превращает панику в 500 и структурированную запись.
//
// Без неё паника на одном голосе роняет весь инстанс, а при 2M RPS это
// заметная доля приёма.
//
//nolint:contextcheck // контекст берётся из самого запроса, он здесь и нужен
func Recovery(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.ErrorContext(r.Context(), "паника в обработчике",
						slog.Any("panic", rec),
						slog.String("path", r.URL.Path),
						slog.String("stack", string(debug.Stack())))
					writeError(w, http.StatusInternalServerError, "internal")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// SecurityHeaders выставляет заголовки, которые дешевле поставить всегда,
// чем вспоминать, где именно они нужны.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		h.Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
		next.ServeHTTP(w, r)
	})
}

// writeError отдаёт отказ в том же формате, что и остальной API.
//
// Пакет не зависит от прикладного слоя намеренно: middleware обязано работать
// и там, где обработчиков ещё нет — например, до монтирования роутера.
func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code}) //nolint:errcheck,errchkjson // заголовки отправлены
}
