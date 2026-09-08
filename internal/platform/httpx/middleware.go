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

func RateLimit(perWindow int, window time.Duration) func(http.Handler) http.Handler {
	if window <= 0 {
		window = time.Minute
	}
	retryAfter := strconv.Itoa(int(window.Seconds()))

	return httprate.LimitBy(
		perWindow,
		window,
		func(r *http.Request) (string, error) {
			return LimitKey(IPFromContext(r.Context())), nil
		},
		httprate.WithLimitHandler(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Retry-After", retryAfter)
			writeError(w, http.StatusTooManyRequests, "rate_limited")
		}),
	)
}

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

//nolint:contextcheck // context comes from the request itself, which is what we want
func Recovery(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.ErrorContext(r.Context(), "panic in handler",
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

func writeError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": code}) //nolint:errcheck,errchkjson // headers are already sent
}
