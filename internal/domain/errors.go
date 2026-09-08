package domain

import "errors"

var (
	ErrInvalidChoices = errors.New("invalid_choices")

	ErrPollClosed = errors.New("poll_closed")

	ErrOptionsImmutable = errors.New("options_immutable")

	ErrBadTransition = errors.New("bad_transition")
)
