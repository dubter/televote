package domain

// Aggregate — обезличенный результат опроса: сколько голосов у каждой опции и
// сколько всего подано бюллетеней. Отдельные голоса не хранятся нигде, поэтому
// связи «человек → выбор» в агрегате нет и появиться не может.
//
// Ballots считается отдельно от суммы голосов: при множественном выборе один
// бюллетень даёт несколько голосов, и проценты обязаны считаться от бюллетеней.
type Aggregate struct {
	// Votes — голоса по индексу опции. Индекс, а не UUID: он же поле хэша
	// счётчиков в Redis.
	Votes map[uint8]int64
	// Ballots — число проголосовавших, знаменатель для процентов.
	Ballots int64
}

// Add прибавляет n голосов опции idx, создавая карту при необходимости.
// Нулевое значение Aggregate пригодно к использованию сразу.
func (a *Aggregate) Add(idx uint8, n int64) {
	if a.Votes == nil {
		a.Votes = make(map[uint8]int64)
	}
	a.Votes[idx] += n
}

// TotalVotes — сумма голосов по всем опциям. Для процентов НЕ используется:
// знаменателем служит Ballots, см. Percent.
func (a Aggregate) TotalVotes() int64 {
	var total int64
	for _, v := range a.Votes {
		total += v
	}
	return total
}

// Merge складывает два агрегата — так снапшотер сворачивает шарды в один
// результат. Операнды не изменяются: вызывающий держит их в срезе и переиспользует.
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

// MergeMax берёт поэлементный максимум a и prev — единственное место, где
// обеспечивается монотонность результата на весь сервис.
//
// Redis теряет часть данных при failover и может подняться с меньшими
// счётчиками. Без максимума снапшот записал бы в Postgres откат назад, и цифра
// в эфире уменьшилась бы на глазах у зрителей. Опции, пропавшие из свежего
// чтения, сохраняются из предыдущего снимка по той же причине.
//
// Операция идемпотентна: повторный вызов с тем же аргументом ничего не меняет,
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

// Percent — доля опции от числа БЮЛЛЕТЕНЕЙ, в процентах.
//
// Знаменатель именно Ballots: при множественном выборе сумма голосов больше
// числа проголосовавших, и деление на неё дало бы проценты, не сходящиеся ни с
// чем — такую цифру нельзя показать в эфире.
//
// Пустой опрос даёт ноль, а не деление на ноль.
func (a Aggregate) Percent(idx uint8) float64 {
	if a.Ballots <= 0 {
		return 0
	}
	return float64(a.Votes[idx]) / float64(a.Ballots) * 100
}
