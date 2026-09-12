package redis

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/adapter/redis/mocks"
	"github.com/dubter/televote/internal/domain"
)

const testTTL = 30 * time.Minute

var (
	testPollID = uuid.MustParse("6f1c9f2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b")
	testTarget = domain.Sharding{PollID: testPollID, ShardCount: 500}
	testSalt   = []byte("salt-for-tally-tests-0123456789!")
)

func testVoters(tb testing.TB, n int) []domain.VoterID {
	tb.Helper()

	out := make([]domain.VoterID, 0, n)
	for i := range n {
		v, err := domain.DeriveVoterID(testSalt, fmt.Sprintf("voter-%d", i))
		require.NoError(tb, err)
		out = append(out, v)
	}
	return out
}

func mockClient(t *testing.T) (*Client, *mocks.MockClient) {
	t.Helper()

	raw := mocks.NewMockClient(gomock.NewController(t))
	return &Client{raw: raw}, raw
}

func testTally(tb testing.TB, ttl time.Duration, jitter float64) *Tally {
	tb.Helper()

	return &Tally{ttl: ttl, jitter: jitter}
}

func TestNewTally_RejectsBrokenDependencies(t *testing.T) {
	t.Parallel()

	_, err := NewTally(nil, testTTL, 0.1, 250*time.Millisecond)
	require.Error(t, err)

	cases := []struct {
		name   string
		ttl    time.Duration
		jitter float64
	}{
		{"нулевой ttl", 0, 0.1},
		{"отрицательный ttl", -time.Minute, 0.1},
		{"отрицательный джиттер", testTTL, -0.1},
		{"джиттер больше половины", testTTL, 0.6},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := mockClient(t)
			_, err := NewTally(client, tc.ttl, tc.jitter, 250*time.Millisecond)
			assert.Error(t, err)
		})
	}

	client, _ := mockClient(t)
	tally, err := NewTally(client, testTTL, 0.1, 250*time.Millisecond)
	require.NoError(t, err)
	require.NotNil(t, tally)
}

func TestApply_RejectsInvalidVoteBeforeRedis(t *testing.T) {
	t.Parallel()

	v := testVoters(t, 1)[0]

	cases := []struct {
		name    string
		target  domain.Sharding
		choices []uint8
	}{
		{"нулевой shardCount", domain.Sharding{PollID: testPollID}, []uint8{1}},
		{"пустой PollID", domain.Sharding{ShardCount: 500}, []uint8{1}},
		{"пустой выбор", testTarget, nil},
		{"дубль в выборе", testTarget, []uint8{2, 2}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := mockClient(t)
			tally, err := NewTally(client, testTTL, 0.1, 250*time.Millisecond)
			require.NoError(t, err)

			res, err := tally.Apply(context.Background(), tc.target, v, tc.choices)
			require.ErrorIs(t, err, domain.ErrInvalidVote)
			assert.NotErrorIs(t, err, domain.ErrStoreUnavailable, "битые аргументы ретраем не лечатся")
			assert.Equal(t, domain.VoteResult(0), res, "при ошибке исход не выдаётся")
		})
	}
}

func TestClassify_WrapsTransientFailuresIntoStoreUnavailable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		err       error
		transient bool
	}{
		{"обрыв сети", errors.New("connection reset by peer"), true},
		{"истёкший дедлайн команды", context.DeadlineExceeded, true},
		{"отменённый контекст", context.Canceled, false},
		{"клиент закрывается", rueidis.ErrClosing, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := classify(tc.err)
			assert.Equal(t, tc.transient, errors.Is(got, domain.ErrStoreUnavailable))
			assert.ErrorIs(t, got, tc.err, "исходная ошибка должна остаться в цепочке")
		})
	}
}

func TestTransientReply_MatchesClusterAndLoadingPrefixes(t *testing.T) {
	t.Parallel()

	for _, msg := range []string{"LOADING Redis is loading", "CLUSTERDOWN The cluster is down", "TRYAGAIN", "OOM command not allowed"} {
		assert.True(t, transientReply(msg), msg)
	}
	for _, msg := range []string{"WRONGTYPE Operation against a key", "CROSSSLOT Keys in request", "ERR unknown command"} {
		assert.False(t, transientReply(msg), msg)
	}
}

func TestKeysFor_FollowShardCountOfTheTarget(t *testing.T) {
	t.Parallel()

	tally := testTally(t, testTTL, 0.1)
	voters := testVoters(t, 200)

	for _, shardCount := range []uint16{1, 500, 1024, 16384} {
		target := domain.Sharding{PollID: testPollID, ShardCount: shardCount}
		for _, v := range voters {
			dedup, counter := tally.keysFor(target, v)

			shard := shardFor(v, shardCount)
			require.Less(t, shard, shardCount)
			assert.Equal(t, dedupKey(testPollID, shard, v), dedup)
			assert.Equal(t, counterKey(testPollID, shard), counter)
		}
	}

	moved := 0
	for _, v := range voters {
		a, _ := tally.keysFor(domain.Sharding{PollID: testPollID, ShardCount: 500}, v)
		b, _ := tally.keysFor(domain.Sharding{PollID: testPollID, ShardCount: 1024}, v)
		if a != b {
			moved++
		}
	}
	assert.Greater(t, moved, len(voters)/2,
		"смена shardCount не сдвинула ключи — значение взято не из аргумента")
}

func TestTTL_HasJitterWithinBounds(t *testing.T) {
	t.Parallel()

	const jitter = 0.1

	tally := testTally(t, testTTL, jitter)
	voters := testVoters(t, 10_000)

	base := testTTL.Seconds()
	lo := int64(base * (1 - jitter))
	hi := int64(base*(1+jitter)) + 1

	distinct := make(map[int64]struct{}, len(voters))
	minSeen, maxSeen := int64(1<<62), int64(0)
	for _, v := range voters {
		got := tally.ttlSecondsFor(v)

		require.GreaterOrEqual(t, got, lo, "TTL вышел за нижнюю границу джиттера")
		require.LessOrEqual(t, got, hi, "TTL вышел за верхнюю границу джиттера")
		distinct[got] = struct{}{}
		minSeen = min(minSeen, got)
		maxSeen = max(maxSeen, got)
	}

	assert.Greater(t, len(distinct), 100, "TTL принимает слишком мало значений")
	assert.Less(t, minSeen, int64(base*(1-jitter/2)), "нижняя половина окна не задействована")
	assert.Greater(t, maxSeen, int64(base*(1+jitter/2)), "верхняя половина окна не задействована")

	fixed := testTally(t, testTTL, 0)
	for _, v := range voters[:100] {
		assert.Equal(t, int64(base), fixed.ttlSecondsFor(v))
	}
}

func TestTTL_NeverRoundsToZero(t *testing.T) {
	t.Parallel()

	tally := testTally(t, 900*time.Millisecond, 0.5)
	for _, v := range testVoters(t, 100) {
		assert.GreaterOrEqual(t, tally.ttlSecondsFor(v), int64(1))
	}
}

func TestTTL_IsStableForSameVoter(t *testing.T) {
	t.Parallel()

	tally := testTally(t, testTTL, 0.1)
	for _, v := range testVoters(t, 50) {
		want := tally.ttlSecondsFor(v)
		for range 5 {
			require.Equal(t, want, tally.ttlSecondsFor(v))
		}
	}
}
