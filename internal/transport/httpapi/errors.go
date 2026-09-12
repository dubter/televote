package httpapi

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/platform/httpx"
)

type errorResponse struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	httpx.WriteJSON(w, status, body)
}

func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	status, code := classify(err)

	if status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}

	if status >= http.StatusInternalServerError {
		slog.ErrorContext(r.Context(), "request failed",
			slog.String("path", r.URL.Path),
			slog.String("error", err.Error()))
	}
	writeJSON(w, status, errorResponse{Error: code})
}

func classify(err error) (status int, code string) {
	switch {
	case err == nil:
		return http.StatusOK, ""

	case errors.Is(err, domain.ErrInvalidChoices),
		errors.Is(err, domain.ErrInvalidVote),
		errors.Is(err, domain.ErrBadClientID),
		errors.Is(err, domain.ErrInvalidPoll),
		errors.Is(err, errBadRequest):
		return http.StatusBadRequest, "invalid_choices"

	case errors.Is(err, domain.ErrPollClosed):
		return http.StatusConflict, "poll_closed"
	case errors.Is(err, domain.ErrBadTransition):
		return http.StatusConflict, "bad_transition"
	case errors.Is(err, errSlugTaken):
		return http.StatusConflict, "slug_taken"

	case errors.Is(err, errNotFound), errors.Is(err, domain.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, domain.ErrVersionConflict):
		return http.StatusConflict, "version_conflict"
	case errors.Is(err, errUnauthorized):
		return http.StatusUnauthorized, "unauthorized"
	case errors.Is(err, errForbidden):
		return http.StatusForbidden, "forbidden"

	case errors.Is(err, domain.ErrQueueUnavailable), errors.Is(err, domain.ErrStoreUnavailable):
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
	errSlugTaken    = errors.New("slug_taken")
)
