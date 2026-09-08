package vote_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/vote"
)

// testPollID фиксирован, чтобы имена ключей в ассертах не зависели от прогона.
var testPollID = uuid.MustParse("6f1c9f2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b")

// deriveVoters выводит n детерминированных voterID: тесты распределения не
// имеют права быть флаки, поэтому вход не случайный, а воспроизводимый.
func deriveVoters(tb testing.TB, n int) []vote.VoterID {
	tb.Helper()

	salt := []byte("salt-for-distribution-tests-32b!")
	out := make([]vote.VoterID, 0, n)
	for i := 0; i < n; i++ {
		v, err := vote.DeriveVoterID(salt, fmt.Sprintf("voter-%d", i))
		require.NoError(tb, err)
		out = append(out, v)
	}
	return out
}

// Инвариант из CLAUDE.md: дедуп-ключ и счётчик обязаны иметь общий hash tag.
// Без него Redis Cluster отвечает CROSSSLOT, и скрипт не выполняется вовсе —
// то есть голос не будет ни посчитан, ни отвергнут, он просто пропадёт.
func TestDedupAndCounterKeys_ShareHashTag(t *testing.T) {
	t.Parallel()

	voters := deriveVoters(t, 8)

	for _, shard := range []uint16{0, 1, 7, 255, 499, 16383} {
		t.Run(fmt.Sprintf("shard=%d", shard), func(t *testing.T) {
			counter := vote.CounterKey(testPollID, shard)
			tag := vote.HashTag(testPollID, shard)

			counterTag, ok := hashTagOf(counter)
			require.True(t, ok, "у ключа счётчика нет hash tag: %q", counter)
			require.Contains(t, tag, counterTag)

			for _, v := range voters {
				dedup := vote.DedupKey(testPollID, shard, v)

				dedupTag, ok := hashTagOf(dedup)
				require.True(t, ok, "у дедуп-ключа нет hash tag: %q", dedup)
				assert.Equal(t, counterTag, dedupTag,
					"дедуп %q и счётчик %q обязаны делить один тег", dedup, counter)
			}
		})
	}
}

// Тот же инвариант, но выраженный так, как его видит Redis: одинаковый слот.
// Сравнение тегов строкой поймало бы не всё — например тег без закрывающей
// скобки строкой похож, а слот даёт другой.
func TestVote_NoCrossSlotError(t *testing.T) {
	t.Parallel()

	voters := deriveVoters(t, 500)
	polls := []uuid.UUID{testPollID, uuid.MustParse("11111111-2222-3333-4444-555555555555")}

	for _, pollID := range polls {
		for _, shardCount := range []uint16{1, 3, 500, 1024, 16384} {
			counterSlots := make(map[uint16]uint16, shardCount)

			for _, v := range voters {
				shard := vote.ShardFor(v, shardCount)
				dedup := vote.DedupKey(pollID, shard, v)
				counter := vote.CounterKey(pollID, shard)

				require.Equal(t, redisSlot(counter), redisSlot(dedup),
					"CROSSSLOT: %q и %q лежат в разных слотах", dedup, counter)
				counterSlots[shard] = redisSlot(counter)
			}

			if shardCount >= 500 {
				distinct := make(map[uint16]struct{}, len(counterSlots))
				for _, slot := range counterSlots {
					distinct[slot] = struct{}{}
				}
				assert.Greater(t, len(distinct), 1,
					"все шарды попали в один слот при shardCount=%d", shardCount)
			}
		}
	}
}

// Ключ дедупа и ключ счётчика различаются префиксом и не могут наложиться:
// строковый SET по адресу хэша счётчика вернул бы WRONGTYPE и потерял голоса.
func TestDedupAndCounterKeys_HaveDistinctPrefixes(t *testing.T) {
	t.Parallel()

	v := deriveVoters(t, 1)[0]
	dedup := vote.DedupKey(testPollID, 3, v)
	counter := vote.CounterKey(testPollID, 3)

	assert.True(t, strings.HasPrefix(dedup, "v:"), "дедуп-ключ: %q", dedup)
	assert.True(t, strings.HasPrefix(counter, "c:"), "ключ счётчика: %q", counter)
	assert.NotEqual(t, dedup, counter)
	assert.Contains(t, dedup, v.Hex(), "voterID должен адресовать дедуп-ключ")
}

// Соль у опросов разная, но и без соли ключи разных опросов не должны
// пересекаться: pollID входит в тег.
func TestHashTag_DiffersAcrossPollsAndShards(t *testing.T) {
	t.Parallel()

	other := uuid.MustParse("11111111-2222-3333-4444-555555555555")

	assert.NotEqual(t, vote.HashTag(testPollID, 1), vote.HashTag(other, 1))
	assert.NotEqual(t, vote.HashTag(testPollID, 1), vote.HashTag(testPollID, 2))
	assert.Equal(t, vote.HashTag(testPollID, 1), vote.HashTag(testPollID, 1))
}

func TestShardFor_Deterministic(t *testing.T) {
	t.Parallel()

	voters := deriveVoters(t, 100)

	for _, shardCount := range []uint16{1, 7, 500, 1024, 16384} {
		for _, v := range voters {
			want := vote.ShardFor(v, shardCount)
			for i := 0; i < 5; i++ {
				require.Equal(t, want, vote.ShardFor(v, shardCount))
			}
			require.Less(t, want, shardCount, "шард вне диапазона [0, shardCount)")
		}
	}
}

// Перекос между мастерами равен 1/√(шардов на мастера) (design.md §4), и вся
// эта арифметика верна только если сама функция шардирования равномерна.
func TestShardFor_UniformDistribution(t *testing.T) {
	t.Parallel()

	const (
		samples    = 100_000
		shardCount = 256
		tolerance  = 0.20 // ±20 % от ожидаемых 390 попаданий — это ~5σ
	)

	voters := deriveVoters(t, samples)
	hits := make([]int, shardCount)
	for _, v := range voters {
		hits[vote.ShardFor(v, shardCount)]++
	}

	expected := float64(samples) / float64(shardCount)
	lo, hi := expected*(1-tolerance), expected*(1+tolerance)
	for shard, n := range hits {
		require.GreaterOrEqual(t, float64(n), lo, "шард %d недогружен: %d", shard, n)
		require.LessOrEqual(t, float64(n), hi, "шард %d перегружен: %d", shard, n)
	}
}

// shardCount=0 приходит только из битого конфига, но деление на ноль на
// горячем пути консьюмера уронило бы процесс и остановило дренаж.
func TestShardFor_ZeroShardCountIsSafe(t *testing.T) {
	t.Parallel()

	v := deriveVoters(t, 1)[0]
	require.NotPanics(t, func() {
		assert.Equal(t, uint16(0), vote.ShardFor(v, 0))
	})
}
