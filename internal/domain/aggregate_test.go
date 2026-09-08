package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
)

func TestAggregate_AddInitialisesVotesMap(t *testing.T) {
	t.Parallel()

	var a domain.Aggregate
	require.NotPanics(t, func() { a.Add(2, 1) })

	a.Add(2, 3)
	assert.Equal(t, map[uint8]int64{2: 4}, a.Votes)
}

func TestAggregate_TotalVotes(t *testing.T) {
	t.Parallel()

	assert.Equal(t, int64(0), domain.Aggregate{}.TotalVotes())
	assert.Equal(t, int64(9), domain.Aggregate{Votes: map[uint8]int64{0: 4, 1: 5}, Ballots: 6}.TotalVotes())
}

// Свёртка шардов: снапшотер складывает HGETALL со всех шардов в один агрегат.
func TestAggregate_MergeSumsShards(t *testing.T) {
	t.Parallel()

	shardA := domain.Aggregate{Votes: map[uint8]int64{0: 10, 1: 2}, Ballots: 11}
	shardB := domain.Aggregate{Votes: map[uint8]int64{1: 5, 3: 7}, Ballots: 9}

	got := shardA.Merge(shardB)

	assert.Equal(t, map[uint8]int64{0: 10, 1: 7, 3: 7}, got.Votes)
	assert.Equal(t, int64(20), got.Ballots)
}

func TestAggregate_MergeDoesNotMutateOperands(t *testing.T) {
	t.Parallel()

	a := domain.Aggregate{Votes: map[uint8]int64{0: 1}, Ballots: 1}
	b := domain.Aggregate{Votes: map[uint8]int64{0: 2}, Ballots: 2}

	_ = a.Merge(b)

	assert.Equal(t, map[uint8]int64{0: 1}, a.Votes)
	assert.Equal(t, map[uint8]int64{0: 2}, b.Votes)
}

func TestAggregate_MergeHandlesZeroValues(t *testing.T) {
	t.Parallel()

	got := domain.Aggregate{}.Merge(domain.Aggregate{Votes: map[uint8]int64{1: 3}, Ballots: 3})

	assert.Equal(t, map[uint8]int64{1: 3}, got.Votes)
	assert.Equal(t, int64(3), got.Ballots)
	assert.NotNil(t, domain.Aggregate{}.Merge(domain.Aggregate{}).Votes)
}

// Монотонность — одно место на весь сервис (design.md §6). Redis теряет данные
// при failover; без max Postgres откатился бы назад, и результат в эфире упал бы.
func TestAggregate_MergeMaxNeverGoesBackwards(t *testing.T) {
	t.Parallel()

	prev := domain.Aggregate{Votes: map[uint8]int64{0: 100, 1: 50}, Ballots: 140}
	fresh := domain.Aggregate{Votes: map[uint8]int64{0: 90, 1: 60}, Ballots: 138}

	got := fresh.MergeMax(prev)

	assert.Equal(t, map[uint8]int64{0: 100, 1: 60}, got.Votes)
	assert.Equal(t, int64(140), got.Ballots)
}

func TestAggregate_MergeMaxKeepsOptionsMissingFromFreshRead(t *testing.T) {
	t.Parallel()

	prev := domain.Aggregate{Votes: map[uint8]int64{0: 5, 7: 3}, Ballots: 8}
	fresh := domain.Aggregate{Votes: map[uint8]int64{0: 6}, Ballots: 6}

	got := fresh.MergeMax(prev)

	assert.Equal(t, map[uint8]int64{0: 6, 7: 3}, got.Votes)
	assert.Equal(t, int64(8), got.Ballots)
}

func TestAggregate_MergeMaxIsIdempotent(t *testing.T) {
	t.Parallel()

	a := domain.Aggregate{Votes: map[uint8]int64{0: 4, 1: 9}, Ballots: 12}

	once := a.MergeMax(a)
	twice := once.MergeMax(a)

	assert.Equal(t, a.Votes, once.Votes)
	assert.Equal(t, a.Ballots, once.Ballots)
	assert.Equal(t, once.Votes, twice.Votes)
}

// Проценты считаются от числа бюллетеней, а не от суммы голосов: при
// множественном выборе сумма больше числа проголосовавших, и деление на неё
// даёт цифры, которые нельзя показать в эфире.
func TestAggregate_PercentIsShareOfBallotsNotVotes(t *testing.T) {
	t.Parallel()

	a := domain.Aggregate{Votes: map[uint8]int64{0: 80, 1: 60, 2: 20}, Ballots: 100}

	require.Greater(t, a.TotalVotes(), a.Ballots, "фикстура обязана быть множественным выбором")
	assert.InDelta(t, 80.0, a.Percent(0), 1e-9)
	assert.InDelta(t, 60.0, a.Percent(1), 1e-9)
	assert.InDelta(t, 20.0, a.Percent(2), 1e-9)
}

func TestAggregate_PercentOnEmptyPollIsZero(t *testing.T) {
	t.Parallel()

	assert.InDelta(t, 0.0, domain.Aggregate{}.Percent(0), 1e-9)
	assert.InDelta(t, 0.0, domain.Aggregate{Votes: map[uint8]int64{0: 5}}.Percent(0), 1e-9)
	assert.InDelta(t, 0.0, domain.Aggregate{Ballots: 10}.Percent(3), 1e-9)
}
