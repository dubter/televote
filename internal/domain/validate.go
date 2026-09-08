package domain

import "time"

// ChoiceRules — правила выбора, вырезанные из опроса. Горячий путь валидирует
// голос по кэшированному конфигу, а не по строке из Postgres, но правило должно
// оставаться одно: две копии проверки разъезжаются молча, и расхождение
// обнаруживается уже на разборе результатов эфира.
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
// Правила: single принимает ровно один вариант и игнорирует Min/MaxChoices —
// они относятся только к множественному выбору; multiple принимает от
// MinChoices до MaxChoices включительно, причём нулевой MaxChoices означает
// отсутствие потолка. Пустой набор отвергается всегда:
// «воздержался» — это отдельная опция опроса, а не отсутствие голоса.
//
// Индексы проверяются на принадлежность списку опций и на уникальность.
// Дубль отвергается даже когда размер набора верен: {4, 4} при MinChoices=2 —
// это попытка отдать два голоса за один вариант.
//
// Входной срез не изменяется: он принадлежит вызывающему, и сортировка ради
// поиска дублей испортила бы порядок, который тот может использовать дальше.
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
		// Неизвестный тип пришёл из базы строкой. Отвергаем, а не голосуем
		// по умолчанию: тихая деградация здесь исказила бы результат.
		return ErrInvalidChoices
	}

	// Опций не больше 255, поэтому битовая карта на стеке дешевле карты и не
	// аллоцирует на горячем пути.
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
// Границы несимметричны намеренно: OpensAt включается, ClosesAt исключается.
// Голос ровно в ClosesAt отвергается — иначе «окно 60 секунд» на деле длилось бы
// 60 секунд плюс одну наносекунду, и граница зависела бы от разрешения часов.
//
// Незаданный ClosesAt означает незаполненный конфиг, а не «голосуем вечно»:
// опрос без конца эфира голосов не принимает.
//
// Статус проверяется отдельно от дат: опрос может быть внутри окна и при этом
// быть черновиком или уже закрытым вручную.
func (w Window) IsOpenAt(t time.Time) bool {
	if w.Status != StatusOpen || w.ClosesAt.IsZero() {
		return false
	}
	return !t.Before(w.OpensAt) && t.Before(w.ClosesAt)
}

// ShouldOpenAt сообщает, должен ли планировщик открыть опрос в момент t.
//
// Открывается только scheduled и только при заданном OpensAt: опрос без
// расписания открывает админ руками.
//
// Прошедший ClosesAt открытию не мешает. Планировщик мог лежать дольше эфира,
// а FSM запрещает переход scheduled → closed напрямую: единственный законный
// путь — открыть и дать финализатору закрыть. Голосов это не добавит, потому
// что IsOpenAt всё равно вернёт false за пределами окна.
func (p *Poll) ShouldOpenAt(t time.Time) bool {
	if p.Status != StatusScheduled || p.OpensAt.IsZero() {
		return false
	}
	return !t.Before(p.OpensAt)
}
