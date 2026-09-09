package vote_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/service/vote"
	"github.com/dubter/televote/internal/service/vote/mocks"
)

var (
	testPollID = uuid.MustParse("6f1c9f2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b")
	otherPolls = uuid.MustParse("11111111-2222-3333-4444-555555555555")

	saltA = []byte("poll-a-salt-0123456789abcdef0123")
	saltB = []byte("poll-b-salt-0123456789abcdef0123")
)

const testClientID = "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"

func idleClient(t *testing.T) rueidis.Client {
	t.Helper()
	return mocks.NewMockClient(gomock.NewController(t))
}

func deriveVoters(tb testing.TB, n int) []vote.VoterID {
	tb.Helper()

	salt := []byte("salt-for-distribution-tests-32b!")
	out := make([]vote.VoterID, 0, n)
	for i := range n {
		v, err := vote.DeriveVoterID(salt, fmt.Sprintf("voter-%d", i))
		require.NoError(tb, err)
		out = append(out, v)
	}
	return out
}

func redisSlot(key string) uint16 {
	if tag, ok := hashTagOf(key); ok {
		return crc16XModem(tag) & 16383
	}
	return crc16XModem(key) & 16383
}

func hashTagOf(key string) (string, bool) {
	s := -1
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			s = i
			break
		}
	}
	if s < 0 {
		return "", false
	}
	for e := s + 1; e < len(key); e++ {
		if key[e] == '}' {
			if e == s+1 {
				return "", false
			}
			return key[s+1 : e], true
		}
	}
	return "", false
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

func TestVote_NoCrossSlotError(t *testing.T) {
	t.Parallel()

	voters := deriveVoters(t, 500)

	for _, pollID := range []uuid.UUID{testPollID, otherPolls} {
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

func TestShardFor_Deterministic(t *testing.T) {
	t.Parallel()

	voters := deriveVoters(t, 100)

	for _, shardCount := range []uint16{1, 7, 500, 1024, 16384} {
		for _, v := range voters {
			want := vote.ShardFor(v, shardCount)
			for range 5 {
				require.Equal(t, want, vote.ShardFor(v, shardCount))
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

func TestShardFor_ZeroShardCountIsSafe(t *testing.T) {
	t.Parallel()

	v := deriveVoters(t, 1)[0]
	require.NotPanics(t, func() {
		assert.Equal(t, uint16(0), vote.ShardFor(v, 0))
	})
}

func TestDeriveVoterID_SameInputSameOutput(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"uuid":    testClientID,
		"юникод":  "короткий",
		"длинный": strings.Repeat("x", 64),
	}

	for name, clientID := range cases {
		t.Run(name, func(t *testing.T) {
			first, err := vote.DeriveVoterID(saltA, clientID)
			require.NoError(t, err)

			for range 3 {
				again, err := vote.DeriveVoterID(saltA, clientID)
				require.NoError(t, err)
				assert.Equal(t, first, again)
			}
		})
	}
}

func TestDeriveVoterID_DifferentSaltsUnlinkable(t *testing.T) {
	t.Parallel()

	a, err := vote.DeriveVoterID(saltA, testClientID)
	require.NoError(t, err)
	b, err := vote.DeriveVoterID(saltB, testClientID)
	require.NoError(t, err)

	assert.NotEqual(t, a, b)

	same := 0
	for i := range a {
		if a[i] == b[i] {
			same++
		}
	}
	assert.Less(t, same, len(a)/2, "выходы подозрительно похожи: %x vs %x", a, b)
}

func TestDeriveVoterID_RejectsEmptyAndConstant(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		clientID string
	}{
		{"пустая строка", ""},
		{"пробелы", "   "},
		{"js undefined", "undefined"},
		{"js null", "null"},
		{"js NaN", "NaN"},
		{"строковый объект", "[object Object]"},
		{"ноль", "0"},
		{"прочерк", "-"},
		{"нулевой uuid", "00000000-0000-0000-0000-000000000000"},
		{"регистр не спасает", "UNDEFINED"},
		{"слишком длинный", strings.Repeat("a", 4096)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v, err := vote.DeriveVoterID(saltA, tc.clientID)
			require.ErrorIs(t, err, vote.ErrBadClientID)
			assert.Equal(t, vote.VoterID{}, v, "при ошибке возвращается нулевой voterID")
		})
	}
}

func TestDeriveVoterID_BoundsKeyLength(t *testing.T) {
	t.Parallel()

	cases := []string{
		"a",
		testClientID,
		strings.Repeat("длинный юникод ", 8),
		strings.Repeat("z", 128),
	}

	for _, clientID := range cases {
		v, err := vote.DeriveVoterID(saltA, clientID)
		require.NoError(t, err, "clientID длиной %d байт", len(clientID))

		assert.Len(t, v[:], 16, "voterID обязан быть 16 байт")
		assert.Len(t, v.Hex(), 32)

		key := vote.DedupKey(testPollID, vote.ShardFor(v, 500), v)
		assert.Less(t, len(key), 96, "дедуп-ключ раздулся: %q", key)
	}
}

func TestDeriveVoterID_RejectsEmptySalt(t *testing.T) {
	t.Parallel()

	for _, salt := range [][]byte{nil, {}, []byte("коротко")} {
		_, err := vote.DeriveVoterID(salt, testClientID)
		require.Error(t, err, "соль длиной %d принята", len(salt))
		assert.NotErrorIs(t, err, vote.ErrBadClientID,
			"битая соль — вина сервера, а не клиента: 400 отдавать нельзя")
	}
}

func TestVoterID_HexRoundTrip(t *testing.T) {
	t.Parallel()

	v, err := vote.DeriveVoterID(saltA, testClientID)
	require.NoError(t, err)

	back, err := vote.ParseVoterID(v.Hex())
	require.NoError(t, err)
	assert.Equal(t, v, back)

	_, err = hex.DecodeString(v.Hex())
	require.NoError(t, err)

	for _, bad := range []string{"", "zz", strings.Repeat("ab", 15), strings.Repeat("ab", 17)} {
		_, err := vote.ParseVoterID(bad)
		assert.Error(t, err, "принят битый hex %q", bad)
	}
}

func TestResult_ZeroValueIsNotSuccess(t *testing.T) {
	t.Parallel()

	var zero vote.Result
	assert.False(t, zero.Valid(), "нулевой Result не может быть валидным исходом")
	assert.True(t, vote.ResultCounted.Valid())
	assert.True(t, vote.ResultAlreadyCounted.Valid())

	assert.Equal(t, "counted", vote.ResultCounted.String())
	assert.Equal(t, "already_counted", vote.ResultAlreadyCounted.String())
	assert.Equal(t, "invalid", zero.String())
	assert.Equal(t, "invalid", vote.Result(200).String())
}

func TestNewCaster_RejectsBrokenDependencies(t *testing.T) {
	t.Parallel()

	_, err := vote.NewCaster(nil, 30*time.Minute, 0.1, 250*time.Millisecond)
	require.Error(t, err)

	cases := []struct {
		name   string
		ttl    time.Duration
		jitter float64
	}{
		{"нулевой ttl", 0, 0.1},
		{"отрицательный ttl", -time.Minute, 0.1},
		{"отрицательный джиттер", 30 * time.Minute, -0.1},
		{"джиттер больше половины", 30 * time.Minute, 0.6},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := vote.NewCaster(idleClient(t), tc.ttl, tc.jitter, 250*time.Millisecond)
			assert.Error(t, err)
		})
	}

	c, err := vote.NewCaster(idleClient(t), 30*time.Minute, 0.1, 250*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestCast_RejectsInvalidArgumentsBeforeRedis(t *testing.T) {
	t.Parallel()

	v, err := vote.DeriveVoterID(saltA, testClientID)
	require.NoError(t, err)

	cases := []struct {
		name       string
		shardCount uint16
		choices    []uint8
	}{
		{"нулевой shardCount", 0, []uint8{1}},
		{"пустой выбор", 500, nil},
		{"дубль в выборе", 500, []uint8{2, 2}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c, err := vote.NewCaster(idleClient(t), 30*time.Minute, 0.1, 250*time.Millisecond)
			require.NoError(t, err)

			res, err := c.Cast(context.Background(), testPollID, tc.shardCount, v, tc.choices)
			require.Error(t, err)
			assert.Equal(t, vote.Result(0), res, "при ошибке исход не выдаётся")
			assert.False(t, vote.IsRetryable(err), "битые аргументы ретраем не лечатся")
		})
	}
}

func TestIsRetryable_ClassifiesRedisAndContextErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"отменённый контекст", context.Canceled, false},
		{"истёкший дедлайн", context.DeadlineExceeded, true},
		{"клиент закрывается", rueidis.ErrClosing, false},
		{"обёрнутый сетевой сбой", fmt.Errorf("vote: %w", errors.New("connection reset by peer")), true},
		{"плохой clientID", vote.ErrBadClientID, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, vote.IsRetryable(tc.err))
		})
	}
}
