package domain

// Aggregate — обезличенный результат опроса. Отдельные голоса не хранятся
// нигде, поэтому связи «человек → выбор» здесь нет и появиться не может.
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

// MergeMax берёт поэлементный максимум — единственное место, где обеспечивается
// монотонность результата.
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
func (a Aggregate) Percent(idx uint8) float64 {
	if a.Ballots <= 0 {
		return 0
	}
	return float64(a.Votes[idx]) / float64(a.Ballots) * 100
}
