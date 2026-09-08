package domain

// Aggregate — обезличенный результат опроса. Отдельные голоса не хранятся
// нигде, поэтому связи «человек → выбор» здесь нет и появиться не может.
//
// Создаётся только через NewAggregate: нулевое значение с nil-картой паникует
// при записи, а лениво инициализировать её в каждом методе — значит размазать
// инвариант по всему типу.
type Aggregate struct {
	Votes   map[uint8]int64
	Ballots int64
}

func NewAggregate() Aggregate {
	return Aggregate{Votes: make(map[uint8]int64)}
}

// NewAggregateFrom собирает агрегат из готовых счётчиков, копируя карту:
// вызывающий не должен иметь возможности изменить её после создания.
func NewAggregateFrom(votes map[uint8]int64, ballots int64) Aggregate {
	a := Aggregate{Votes: make(map[uint8]int64, len(votes)), Ballots: ballots}
	for idx, n := range votes {
		a.Votes[idx] = n
	}
	return a
}

func (a *Aggregate) Add(idx uint8, n int64) {
	a.Votes[idx] += n
}

// MergeMax берёт поэлементный максимум — единственное место, где обеспечивается
// монотонность результата. Redis теряет часть данных при failover и поднимается
// с меньшими счётчиками; без максимума цифра в админке уменьшилась бы на глазах.
func (a Aggregate) MergeMax(prev Aggregate) Aggregate {
	out := Aggregate{
		Votes:   make(map[uint8]int64, len(a.Votes)+len(prev.Votes)),
		Ballots: max(a.Ballots, prev.Ballots),
	}
	for idx, n := range a.Votes {
		out.Votes[idx] = n
	}
	for idx, n := range prev.Votes {
		out.Votes[idx] = max(out.Votes[idx], n)
	}
	return out
}

// Percent — доля опции от числа БЮЛЛЕТЕНЕЙ. Знаменатель именно Ballots: при
// множественном выборе сумма голосов больше числа проголосовавших, и проценты
// не сошлись бы ни с чем.
func (a Aggregate) Percent(idx uint8) float64 {
	if a.Ballots <= 0 {
		return 0
	}
	return float64(a.Votes[idx]) / float64(a.Ballots) * 100
}
