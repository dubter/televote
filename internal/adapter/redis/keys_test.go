package redis

import (
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var otherPollID = uuid.MustParse("11111111-2222-3333-4444-555555555555")

func redisSlot(key string) uint16 {
	if tag, ok := hashTagOf(key); ok {
		return crc16XModem(tag) & 16383
	}
	return crc16XModem(key) & 16383
}

func hashTagOf(key string) (string, bool) {
	s := strings.IndexByte(key, '{')
	if s < 0 {
		return "", false
	}
	e := strings.IndexByte(key[s+1:], '}')
	if e <= 0 {
		return "", false
	}
	return key[s+1 : s+1+e], true
}

func crc16XModem(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func TestDedupAndCounterKeys_ShareHashTag(t *testing.T) {
	t.Parallel()

	voters := testVoters(t, 8)

	for _, shard := range []uint16{0, 1, 7, 255, 499, 16383} {
		t.Run(fmt.Sprintf("shard=%d", shard), func(t *testing.T) {
			counter := counterKey(testPollID, shard)
			tag := hashTag(testPollID, shard)

			counterTag, ok := hashTagOf(counter)
			require.True(t, ok, "у ключа счётчика нет hash tag: %q", counter)
			require.Contains(t, tag, counterTag)

			for _, v := range voters {
				dedup := dedupKey(testPollID, shard, v)

				dedupTag, ok := hashTagOf(dedup)
				require.True(t, ok, "у дедуп-ключа нет hash tag: %q", dedup)
				assert.Equal(t, counterTag, dedupTag,
					"дедуп %q и счётчик %q обязаны делить один тег", dedup, counter)
			}
		})
	}
}

func TestKeys_NeverCrossSlots(t *testing.T) {
	t.Parallel()

	voters := testVoters(t, 500)

	for _, pollID := range []uuid.UUID{testPollID, otherPollID} {
		for _, shardCount := range []uint16{1, 3, 500, 1024, 16384} {
			counterSlots := make(map[uint16]uint16, shardCount)

			for _, v := range voters {
				shard := shardFor(v, shardCount)
				dedup := dedupKey(pollID, shard, v)
				counter := counterKey(pollID, shard)

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

func TestDedupAndCounterKeys_HaveDistinctPrefixes(t *testing.T) {
	t.Parallel()

	v := testVoters(t, 1)[0]
	dedup := dedupKey(testPollID, 3, v)
	counter := counterKey(testPollID, 3)

	assert.True(t, strings.HasPrefix(dedup, "v:"), "дедуп-ключ: %q", dedup)
	assert.True(t, strings.HasPrefix(counter, "c:"), "ключ счётчика: %q", counter)
	assert.NotEqual(t, dedup, counter)
	assert.Contains(t, dedup, v.Hex(), "voterID должен адресовать дедуп-ключ")
}

func TestDedupKey_StaysShort(t *testing.T) {
	t.Parallel()

	for _, v := range testVoters(t, 20) {
		key := dedupKey(testPollID, shardFor(v, 500), v)
		assert.Less(t, len(key), 96, "дедуп-ключ раздулся: %q", key)
	}
}

func TestShardFor_Deterministic(t *testing.T) {
	t.Parallel()

	voters := testVoters(t, 100)

	for _, shardCount := range []uint16{1, 7, 500, 1024, 16384} {
		for _, v := range voters {
			want := shardFor(v, shardCount)
			for range 5 {
				require.Equal(t, want, shardFor(v, shardCount))
			}
			require.Less(t, want, shardCount, "шард вне диапазона [0, shardCount)")
		}
	}
}

func TestShardFor_UniformDistribution(t *testing.T) {
	t.Parallel()

	const (
		samples    = 100_000
		shardCount = 256
		tolerance  = 0.20
	)

	voters := testVoters(t, samples)
	hits := make([]int, shardCount)
	for _, v := range voters {
		hits[shardFor(v, shardCount)]++
	}

	expected := float64(samples) / float64(shardCount)
	lo, hi := expected*(1-tolerance), expected*(1+tolerance)
	for shard, n := range hits {
		require.GreaterOrEqual(t, float64(n), lo, "шард %d недогружен: %d", shard, n)
		require.LessOrEqual(t, float64(n), hi, "шард %d перегружен: %d", shard, n)
	}
}

func TestShardFor_ZeroShardCountIsSafe(t *testing.T) {
	t.Parallel()

	v := testVoters(t, 1)[0]
	require.NotPanics(t, func() {
		assert.Equal(t, uint16(0), shardFor(v, 0))
	})
}
