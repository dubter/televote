package domain

import "time"

// ChoiceRules — правила выбора для горячего пути. Правило одно на весь сервис:
// две копии проверки разъехались бы молча.
type ChoiceRules struct {
	Type        PollType
	OptionCount uint8
	MinChoices  uint8
	MaxChoices  uint8
}

// ChoiceRules собирает правила выбора для горячего пути.
func (p *Poll) ChoiceRules() ChoiceRules {
	return ChoiceRules{
		Type:        p.Type,
		OptionCount: p.OptionCount(),
		MinChoices:  p.MinChoices,
		MaxChoices:  p.MaxChoices,
	}
}

// ValidateChoices проверяет выбор по правилам опроса.
func (p *Poll) ValidateChoices(choices []uint8) error {
	return p.ChoiceRules().Validate(choices)
}

// Validate проверяет набор выбранных индексов.
//
// single принимает ровно один вариант; multiple — от MinChoices до MaxChoices,
// нулевой MaxChoices означает отсутствие потолка. Пустой набор отвергается
// всегда: «воздержался» — это отдельная опция, а не отсутствие голоса.
// Дубль индекса отвергается даже при верном размере набора.
//
// Входной срез не изменяется.
func (r ChoiceRules) Validate(choices []uint8) error {
	if len(choices) == 0 {
		return ErrInvalidChoices
	}

	switch r.Type {
	case PollTypeSingle:
		if len(choices) != 1 {
			return ErrInvalidChoices
		}
	case PollTypeMultiple:
		n := len(choices)
		if n < int(r.MinChoices) {
			return ErrInvalidChoices
		}
		// MaxChoices=0 означает «потолок не задан», а не «голосовать нельзя».
		// Жёсткое n > 0 сделало бы такой опрос тихо неголосуемым: админка
		// создала бы его без ошибки, а каждый голос получал бы 400.
		if r.MaxChoices > 0 && n > int(r.MaxChoices) {
			return ErrInvalidChoices
		}
	default:
		// Неизвестный тип из базы: тихая деградация исказила бы результат.
		return ErrInvalidChoices
	}

	// Битовая карта на стеке: не аллоцирует на горячем пути.
	var seen [MaxOptions + 1]bool
	for _, idx := range choices {
		if idx >= r.OptionCount {
			return ErrInvalidChoices
		}
		if seen[idx] {
			return ErrInvalidChoices
		}
		seen[idx] = true
	}
	return nil
}

// Window — окно голосования и статус опроса, вырезанные для горячего пути.
type Window struct {
	Status   Status
	OpensAt  time.Time
	ClosesAt time.Time
}

// Window собирает окно голосования для горячего пути.
func (p *Poll) Window() Window {
	return Window{Status: p.Status, OpensAt: p.OpensAt, ClosesAt: p.ClosesAt}
}

// IsOpenAt сообщает, принимает ли опрос голоса в момент t.
func (p *Poll) IsOpenAt(t time.Time) bool {
	return p.Window().IsOpenAt(t)
}

// IsOpenAt сообщает, принимает ли окно голоса в момент t.
//
// Границы несимметричны: OpensAt включается, ClosesAt исключается — иначе окно
// длилось бы на наносекунду дольше заявленного. Незаданный ClosesAt означает
// незаполненный конфиг, а не «голосуем вечно». Статус проверяется отдельно
// от дат: опрос может быть внутри окна и при этом закрыт вручную.
func (w Window) IsOpenAt(t time.Time) bool {
	if w.Status != StatusOpen || w.ClosesAt.IsZero() {
		return false
	}
	return !t.Before(w.OpensAt) && t.Before(w.ClosesAt)
}

// ShouldOpenAt сообщает, должен ли планировщик открыть опрос в момент t.
//
// Прошедший ClosesAt открытию не мешает: FSM запрещает scheduled → closed
// напрямую, поэтому опоздавший опрос открывается и закрывается финализатором.
// Голосов это не добавит — IsOpenAt всё равно вернёт false вне окна.
func (p *Poll) ShouldOpenAt(t time.Time) bool {
	if p.Status != StatusScheduled || p.OpensAt.IsZero() {
		return false
	}
	return !t.Before(p.OpensAt)
}
