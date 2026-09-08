package domain

import "errors"

var (
	ErrInvalidChoices = errors.New("invalid_choices")

	ErrPollClosed = errors.New("poll_closed")

	ErrBadTransition = errors.New("bad_transition")

	ErrNotFound = errors.New("not_found")

	ErrSlugTaken = errors.New("slug_taken")

	ErrVersionConflict = errors.New("version_conflict")
)
