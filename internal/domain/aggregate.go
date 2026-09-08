package domain

type Aggregate struct {
	Votes   map[uint8]int64 // по индексу опции
	Ballots int64           // знаменатель для процентов
}

func (a *Aggregate) Add(idx uint8, n int64) {
	if a.Votes == nil {
		a.Votes = make(map[uint8]int64)
	}
	a.Votes[idx] += n
}

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

func (a Aggregate) Percent(idx uint8) float64 {
	if a.Ballots <= 0 {
		return 0
	}
	return float64(a.Votes[idx]) / float64(a.Ballots) * 100
}
