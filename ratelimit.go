package httpapi

import (
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
func RateLimit(perMin int, window time.Duration) func(http.Handler) http.Handler {
	if window <= 0 {
		window = time.Minute
	}
	return httprate.Limit(
		perMin,
		window,
		httprate.WithKeyFuncs(func(r *http.Request) (string, error) {
			return LimitKey(IPFromContext(r.Context())), nil
		}),
		httprate.WithLimitHandler(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", strconv.Itoa(int(window.Seconds())))
			writeJSONError(w, http.StatusTooManyRequests, "rate_limited")
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
			addr := IPFromContext(r.Context())
			if addr.IsValid() {
				for _, prefix := range ranges {
					if prefix.Contains(addr) {
						writeJSONError(w, http.StatusForbidden, "blocked")
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
func Recovery(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.ErrorContext(r.Context(), "паника в обработчике",
						slog.Any("panic", rec),
						slog.String("path", r.URL.Path),
						slog.String("stack", string(debug.Stack())))
					writeJSONError(w, http.StatusInternalServerError, "internal")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
