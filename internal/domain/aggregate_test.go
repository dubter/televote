package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
)

func TestNewAggregate_IsReadyForWrites(t *testing.T) {
	t.Parallel()

	a := domain.NewAggregate()
	a.Add(2, 1)
	a.Add(2, 3)

	assert.Equal(t, map[uint8]int64{2: 4}, a.Votes)
}

// Копия карты обязательна: иначе вызывающий смог бы изменить агрегат после
// создания, и снимок перестал бы быть снимком.
func TestNewAggregateFrom_CopiesInput(t *testing.T) {
	t.Parallel()

	src := map[uint8]int64{0: 10, 1: 5}
	a := domain.NewAggregateFrom(src, 15)

	src[0] = 999
	delete(src, 1)

	assert.Equal(t, map[uint8]int64{0: 10, 1: 5}, a.Votes)
	assert.EqualValues(t, 15, a.Ballots)
}

func TestAggregate_MergeMaxNeverGoesBackwards(t *testing.T) {
	t.Parallel()

	prev := domain.NewAggregateFrom(map[uint8]int64{0: 100, 1: 50}, 140)
	fresh := domain.NewAggregateFrom(map[uint8]int64{0: 90, 1: 60}, 138)

	got := fresh.MergeMax(prev)

	assert.Equal(t, map[uint8]int64{0: 100, 1: 60}, got.Votes)
	assert.Equal(t, int64(140), got.Ballots)
}

func TestAggregate_MergeMaxKeepsOptionsMissingFromFreshRead(t *testing.T) {
	t.Parallel()

	prev := domain.NewAggregateFrom(map[uint8]int64{0: 5, 7: 3}, 8)
	fresh := domain.NewAggregateFrom(map[uint8]int64{0: 6}, 6)

	got := fresh.MergeMax(prev)

	assert.Equal(t, map[uint8]int64{0: 6, 7: 3}, got.Votes)
	assert.Equal(t, int64(8), got.Ballots)
}

func TestAggregate_MergeMaxIsIdempotent(t *testing.T) {
	t.Parallel()

	a := domain.NewAggregateFrom(map[uint8]int64{0: 4, 1: 9}, 12)

	once := a.MergeMax(a)
	twice := once.MergeMax(a)

	assert.Equal(t, a.Votes, once.Votes)
	assert.Equal(t, a.Ballots, once.Ballots)
	assert.Equal(t, once.Votes, twice.Votes)
}

func TestAggregate_PercentIsShareOfBallotsNotVotes(t *testing.T) {
	t.Parallel()

	a := domain.NewAggregateFrom(map[uint8]int64{0: 80, 1: 60, 2: 20}, 100)

	require.Greater(t, a.Votes[0]+a.Votes[1]+a.Votes[2], a.Ballots, "фикстура обязана быть множественным выбором")
	assert.InDelta(t, 80.0, a.Percent(0), 1e-9)
	assert.InDelta(t, 60.0, a.Percent(1), 1e-9)
	assert.InDelta(t, 20.0, a.Percent(2), 1e-9)
}

func TestAggregate_PercentOnEmptyPollIsZero(t *testing.T) {
	t.Parallel()

	assert.InDelta(t, 0.0, domain.NewAggregate().Percent(0), 1e-9)
	assert.InDelta(t, 0.0, domain.NewAggregateFrom(map[uint8]int64{0: 5}, 0).Percent(0), 1e-9)
	assert.InDelta(t, 0.0, domain.NewAggregateFrom(nil, 10).Percent(3), 1e-9)
}
