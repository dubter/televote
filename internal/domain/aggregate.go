package domain

import "maps"

type Aggregate struct {
	Votes   map[uint8]int64
	Ballots int64
}

func NewAggregate() Aggregate {
	return Aggregate{Votes: make(map[uint8]int64)}
}

func NewAggregateFrom(votes map[uint8]int64, ballots int64) Aggregate {
	a := Aggregate{Votes: make(map[uint8]int64, len(votes)), Ballots: ballots}
	maps.Copy(a.Votes, votes)
	return a
}

func (a *Aggregate) Add(idx uint8, n int64) {
	a.Votes[idx] += n
}

func (a Aggregate) MergeMax(prev Aggregate) Aggregate {
	out := Aggregate{
		Votes:   make(map[uint8]int64, len(a.Votes)+len(prev.Votes)),
		Ballots: max(a.Ballots, prev.Ballots),
	}
	maps.Copy(out.Votes, a.Votes)
	for idx, n := range prev.Votes {
		out.Votes[idx] = max(out.Votes[idx], n)
	}
	return out
}

func (a Aggregate) Percent(idx uint8) float64 {
	if a.Ballots <= 0 {
		return 0
	}
	return float64(a.Votes[idx]) / float64(a.Ballots) * 100
}
