package domain

import "errors"

// Доменные ошибки. Текст каждой — это код ответа из таблицы design.md §7:
// единственная точка маппинга (internal/httpapi/errors.go) переводит их в
// HTTP-статусы, и совпадение текста с кодом делает расхождение заметным.
//
// Сравнивать только через errors.Is: домен оставляет за собой право обернуть
// сентинел деталями, не ломая вызывающий код.
var (
	// ErrInvalidChoices — выбор нарушает правила опроса: неизвестный индекс,
	// дубль, не тот размер набора. HTTP 400.
	ErrInvalidChoices = errors.New("invalid_choices")

	// ErrPollClosed — голос пришёл вне окна голосования либо опрос не в статусе
	// open. HTTP 409 (не 400: запрос корректен, состояние — нет).
	ErrPollClosed = errors.New("poll_closed")

	// ErrOptionsImmutable — попытка изменить опции опроса, по которому уже
	// проголосовали: правка меняет смысл поданных голосов. HTTP 409.
	ErrOptionsImmutable = errors.New("options_immutable")

	// ErrBadTransition — переход статуса запрещён конечным автоматом
	// (см. CanTransitionTo). HTTP 409.
	ErrBadTransition = errors.New("bad_transition")
)
