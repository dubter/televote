package domain

// transitions — исчерпывающая таблица: разрешены четыре пары, остальное
// запрещено. Таблицей, а не цепочкой if: забытый выход из archived виден глазами.
var transitions = map[Status]Status{
	StatusDraft:     StatusScheduled,
	StatusScheduled: StatusOpen,
	StatusOpen:      StatusClosed,
	StatusClosed:    StatusArchived,
}

// CanTransitionTo сообщает, разрешён ли переход s → next.
//
// Переход в собственный статус запрещён: «открыть открытый» — не смена
// состояния, а лишняя запись в аудите. Неизвестный статус никуда не переходит.
func (s Status) CanTransitionTo(next Status) bool {
	allowed, ok := transitions[s]
	return ok && allowed != "" && allowed == next
}
