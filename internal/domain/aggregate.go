package domain

// Aggregate — обезличенный результат опроса. Отдельные голоса не хранятся
// нигде, поэтому связи «человек → выбор» здесь нет и появиться не может.
//
// Ballots считается отдельно от суммы голосов: при множественном выборе один
// бюллетень даёт несколько голосов.
type Aggregate struct {
	Votes   map[uint8]int64 // по индексу опции
	Ballots int64           // знаменатель для процентов
}

// Add прибавляет n голосов опции idx. Нулевой Aggregate готов к использованию.
func (a *Aggregate) Add(idx uint8, n int64) {
	if a.Votes == nil {
		a.Votes = make(map[uint8]int64)
	}
	a.Votes[idx] += n
}

// TotalVotes — сумма по всем опциям. Для процентов не используется, см. Percent.
func (a Aggregate) TotalVotes() int64 {
	var total int64
	for _, v := range a.Votes {
		total += v
	}
	return total
}

// Merge складывает агрегаты: так снапшотер сворачивает шарды. Операнды не изменяются.
func (a Aggregate) Merge(other Aggregate) Aggregate {
	out := Aggregate{
		Votes:   make(map[uint8]int64, len(a.Votes)+len(other.Votes)),
		Ballots: a.Ballots + other.Ballots,
	}
	for idx, v := range a.Votes {
		out.Votes[idx] += v
	}
	for idx, v := range other.Votes {
		out.Votes[idx] += v
	}
	return out
}

// MergeMax берёт поэлементный максимум — единственное место, где обеспечивается
// монотонность результата.
//
// Redis теряет часть данных при failover и поднимается с меньшими счётчиками;
// без максимума цифра в админке уменьшилась бы на глазах. Идемпотентна,
// поэтому дубль тика снапшотера безвреден.
func (a Aggregate) MergeMax(prev Aggregate) Aggregate {
	out := Aggregate{
		Votes:   make(map[uint8]int64, len(a.Votes)+len(prev.Votes)),
		Ballots: max(a.Ballots, prev.Ballots),
	}
	for idx, v := range a.Votes {
		out.Votes[idx] = v
	}
	for idx, v := range prev.Votes {
		if v > out.Votes[idx] {
			out.Votes[idx] = v
		}
	}
	return out
}

// Percent — доля опции от числа БЮЛЛЕТЕНЕЙ.
//
// Знаменатель именно Ballots: при множественном выборе сумма голосов больше
// числа проголосовавших, и проценты не сошлись бы ни с чем.
func (a Aggregate) Percent(idx uint8) float64 {
	if a.Ballots <= 0 {
		return 0
	}
	return float64(a.Votes[idx]) / float64(a.Ballots) * 100
}
