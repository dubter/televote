package domain

import "errors"

// Доменные ошибки. Текст каждой совпадает с кодом ответа API — расхождение
// маппинга (internal/httpapi/errors.go) видно глазами. Сравнивать через errors.Is.
var (
	// ErrInvalidChoices — выбор нарушает правила опроса. HTTP 400.
	ErrInvalidChoices = errors.New("invalid_choices")

	ErrPollClosed = errors.New("poll_closed")

	// ErrOptionsImmutable — правка опций опроса, по которому уже голосовали.
	ErrOptionsImmutable = errors.New("options_immutable")

	// ErrBadTransition — переход статуса запрещён автоматом.
	ErrBadTransition = errors.New("bad_transition")
)
