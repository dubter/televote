package domain_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
)

func TestShardCountFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		masters int
		want    uint16
	}{
		{name: "один мастер даёт 500 шардов", masters: 1, want: 500},
		{name: "три мастера дают 1500 шардов", masters: 3, want: 1500},
		{name: "шесть мастеров дают 3000 шардов", masters: 6, want: 3000},
		{name: "32 мастера дают 16000 шардов", masters: 32, want: 16000},
		{name: "33 мастера упираются в потолок", masters: 33, want: 16384},
		{name: "40 мастеров упираются в потолок", masters: 40, want: 16384},
		{name: "тысяча мастеров не переполняет uint16", masters: 1000, want: 16384},
		{name: "ноль мастеров даёт минимум, а не ноль", masters: 0, want: 500},
		{name: "отрицательное число мастеров даёт минимум", masters: -3, want: 500},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, domain.ShardCountFor(tc.masters))
		})
	}
}

// shard = hash % shard_count: ноль здесь — паника деления на ноль на горячем
// пути, причём только в проде, где конфиг посчитан из реального числа мастеров.
func TestShardCountFor_NeverReturnsZero(t *testing.T) {
	t.Parallel()

	for masters := -10; masters <= 200; masters++ {
		require.NotZero(t, domain.ShardCountFor(masters), "masters=%d", masters)
	}
}

func TestShardCountFor_IsMonotonicAndCapped(t *testing.T) {
	t.Parallel()

	prev := domain.ShardCountFor(-1)
	for masters := range 200 {
		got := domain.ShardCountFor(masters)
		require.GreaterOrEqual(t, got, prev, "masters=%d: число шардов не должно убывать", masters)
		require.LessOrEqual(t, got, uint16(domain.MaxShardCount), "masters=%d", masters)
		prev = got
	}
}

// Перекос между мастерами равен 1/√(шардов на мастера) (design.md §4).
// 500 шардов на мастера держат его в пределах ±4.5 %.
func TestShardCountFor_KeepsAtLeastFiveHundredShardsPerMaster(t *testing.T) {
	t.Parallel()

	for masters := 1; masters <= 32; masters++ {
		perMaster := int(domain.ShardCountFor(masters)) / masters
		assert.GreaterOrEqual(t, perMaster, 500, "masters=%d", masters)
	}
}

func TestPollType_Valid(t *testing.T) {
	t.Parallel()

	assert.True(t, domain.PollTypeSingle.Valid())
	assert.True(t, domain.PollTypeMultiple.Valid())
	assert.False(t, domain.PollType("ranked").Valid())
	assert.False(t, domain.PollType("").Valid())
}

func TestPoll_OptionCount(t *testing.T) {
	t.Parallel()

	assert.Equal(t, uint8(0), (&domain.Poll{}).OptionCount())
	assert.Equal(t, uint8(3), (&domain.Poll{Options: options(3)}).OptionCount())
	assert.Equal(t, uint8(255), (&domain.Poll{Options: make([]domain.Option, 255)}).OptionCount())
}

// Индекс опции — uint8, значит адресуемых опций не больше 255. Наивное
// uint8(len(options)) при 256 опциях дало бы 0 и тихо отвергло все голоса.
func TestPoll_OptionCountSaturatesAtMaxOptions(t *testing.T) {
	t.Parallel()

	p := &domain.Poll{Options: make([]domain.Option, 300)}
	assert.Equal(t, uint8(domain.MaxOptions), p.OptionCount())
}
