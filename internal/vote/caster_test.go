package vote_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/vote"
)

func TestResult_ZeroValueIsNotSuccess(t *testing.T) {
	t.Parallel()

	assert.NotEqual(t, vote.Result(0), vote.ResultCounted)
	assert.NotEqual(t, vote.Result(0), vote.ResultAlreadyCounted)
	assert.Equal(t, vote.Result(1), vote.ResultCounted)
	assert.Equal(t, vote.Result(2), vote.ResultAlreadyCounted)

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

	_, err := vote.NewCaster(nil, 30*time.Minute, 0.1)
	require.Error(t, err)

	client := struct{ rueidis.Client }{}

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

			_, err := vote.NewCaster(client, tc.ttl, tc.jitter)
			assert.Error(t, err)
		})
	}

	c, err := vote.NewCaster(client, 30*time.Minute, 0.1)
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestCast_RejectsInvalidArgumentsBeforeRedis(t *testing.T) {
	t.Parallel()

	c, err := vote.NewCaster(struct{ rueidis.Client }{}, 30*time.Minute, 0.1)
	require.NoError(t, err)

	v, err := vote.DeriveVoterID(saltA, "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e")
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
