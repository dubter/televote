package httpapi

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/vote"
)

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSONError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{Error: code}) //nolint:errcheck,errchkjson // см. комментарий выше
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body) //nolint:errcheck,errchkjson // заголовки отправлены, обрыв — событие клиента
}

func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := classify(err)

	if status >= http.StatusInternalServerError {
		slog.ErrorContext(r.Context(), "запрос завершился ошибкой",
			slog.String("path", r.URL.Path),
			slog.String("error", err.Error()))
	}
	writeJSONError(w, status, code)
}

func classify(err error) (status int, code string) {
	switch {
	case err == nil:
		return http.StatusOK, ""

	case errors.Is(err, domain.ErrInvalidChoices),
		errors.Is(err, vote.ErrInvalidArgs),
		errors.Is(err, vote.ErrBadClientID),
		errors.Is(err, domain.ErrInvalidPoll),
		errors.Is(err, errBadRequest):
		return http.StatusBadRequest, "invalid_choices"

	case errors.Is(err, domain.ErrPollClosed):
		return http.StatusConflict, "poll_closed"
	case errors.Is(err, domain.ErrOptionsImmutable):
		return http.StatusConflict, "options_immutable"
	case errors.Is(err, domain.ErrBadTransition):
		return http.StatusConflict, "bad_transition"
	case errors.Is(err, errSlugTaken):
		return http.StatusConflict, "slug_taken"

	case errors.Is(err, errNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, errUnauthorized):
		return http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, errForbidden):
		return http.StatusForbidden, "forbidden"

	case errors.Is(err, errUnavailable):
		return http.StatusServiceUnavailable, "unavailable"

	default:
		return http.StatusInternalServerError, "internal"
	}
}

var (
	errBadRequest   = errors.New("bad_request")
	errNotFound     = errors.New("not_found")
	errUnauthorized = errors.New("unauthorized")
	errForbidden    = errors.New("forbidden")
	errUnavailable  = errors.New("unavailable")
	errSlugTaken    = errors.New("slug_taken")
)
