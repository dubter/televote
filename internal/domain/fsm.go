package domain

// transitions — исчерпывающая таблица переходов статуса. Разрешено ровно
// четыре пары, всё остальное запрещено по умолчанию.
//
// Таблица, а не цепочка if: расширение автомата требует правки одного места,
// и забытый выход из archived здесь виден глазами, а не выводится из ветвлений.
var transitions = map[Status]Status{
	StatusDraft:     StatusScheduled,
	StatusScheduled: StatusOpen,
	StatusOpen:      StatusClosed,
	StatusClosed:    StatusArchived,
}

// CanTransitionTo сообщает, разрешён ли переход s → next.
//
// Переход в собственный статус не разрешён: «открыть открытый» — это не смена
// состояния, а лишняя запись в admin_audit на пустом месте.
//
// Неизвестный статус (значение из базы приходит строкой) никуда не переходит:
// отсутствие ключа в таблице даёт нулевое значение Status, и сравнение с ним
// отсекается явной проверкой на пустоту.
func (s Status) CanTransitionTo(next Status) bool {
	allowed, ok := transitions[s]
	return ok && allowed != "" && allowed == next
}
