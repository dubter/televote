//go:build integration

package vote_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/dubter/televote/internal/service/vote"
)

const shardCount = 64

func startRedis(ctx context.Context, t *testing.T) rueidis.Client {
	t.Helper()

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:8-alpine",
			ExposedPorts: []string{"6379/tcp"},
			Cmd: []string{
				"redis-server",
				"--cluster-enabled", "yes",
				"--maxmemory-policy", "noeviction",
				"--appendonly", "no",
				"--save", "",
			},
			WaitingFor: wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	testcontainers.CleanupContainer(t, container)

	endpoint, err := container.PortEndpoint(ctx, "6379/tcp", "")
	require.NoError(t, err)

	client, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{endpoint}, DisableCache: true, ForceSingleClient: true,
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	slots := make([]string, 0, 16384)
	for slot := range 16384 {
		slots = append(slots, fmt.Sprint(slot))
	}
	require.NoError(t, client.Do(ctx,
		client.B().Arbitrary(append([]string{"CLUSTER", "ADDSLOTS"}, slots...)...).Build()).Error())

	require.Eventually(t, func() bool {
		info, infoErr := client.Do(ctx, client.B().Arbitrary("CLUSTER", "INFO").Build()).ToString()
		return infoErr == nil && strings.Contains(info, "cluster_state:ok")
	}, time.Minute, 200*time.Millisecond, "cluster mode did not come up")

	return client
}

func newCaster(t *testing.T, client rueidis.Client) *vote.Caster {
	t.Helper()

	c, err := vote.NewCaster(client, time.Hour, 0, 5*time.Second)
	require.NoError(t, err)
	return c
}

func voter(t *testing.T, salt []byte, id string) vote.VoterID {
	t.Helper()

	v, err := vote.DeriveVoterID(salt, id)
	require.NoError(t, err)
	return v
}

func TestCast_SecondVoteOfSameVoterIsNotCounted(t *testing.T) {
	ctx := context.Background()
	caster := newCaster(t, startRedis(ctx, t))

	poll, salt := uuid.New(), []byte(strings.Repeat("s", 32))
	v := voter(t, salt, "viewer-1")

	first, err := caster.Cast(ctx, poll, shardCount, v, []uint8{1})
	require.NoError(t, err)
	assert.Equal(t, vote.ResultCounted, first)

	second, err := caster.Cast(ctx, poll, shardCount, v, []uint8{2})
	require.NoError(t, err)
	assert.Equal(t, vote.ResultAlreadyCounted, second, "дедуп обязан отсечь повтор")

	agg, err := caster.Aggregate(ctx, poll, shardCount)
	require.NoError(t, err)
	assert.EqualValues(t, 1, agg.Ballots)
	assert.EqualValues(t, 1, agg.Votes[1])
	assert.EqualValues(t, 0, agg.Votes[2], "второй выбор не должен попасть в счёт")
}

func TestCast_IsIdempotentUnderConcurrentRedelivery(t *testing.T) {
	ctx := context.Background()
	caster := newCaster(t, startRedis(ctx, t))

	poll, salt := uuid.New(), []byte(strings.Repeat("s", 32))
	v := voter(t, salt, "viewer-1")

	const parallel = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		counted int
	)
	for range parallel {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := caster.Cast(ctx, poll, shardCount, v, []uint8{0})
			if err == nil && res == vote.ResultCounted {
				mu.Lock()
				counted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, counted, "ровно одна из параллельных доставок обязана быть засчитана")

	agg, err := caster.Aggregate(ctx, poll, shardCount)
	require.NoError(t, err)
	assert.EqualValues(t, 1, agg.Ballots)
}

func TestCast_MultipleChoiceCountsEveryOptionAndOneBallot(t *testing.T) {
	ctx := context.Background()
	caster := newCaster(t, startRedis(ctx, t))

	poll, salt := uuid.New(), []byte(strings.Repeat("s", 32))
	v := voter(t, salt, "viewer-1")

	_, err := caster.Cast(ctx, poll, shardCount, v, []uint8{0, 3})
	require.NoError(t, err)

	agg, err := caster.Aggregate(ctx, poll, shardCount)
	require.NoError(t, err)
	assert.EqualValues(t, 1, agg.Ballots, "бюллетень один, даже если выборов несколько")
	assert.EqualValues(t, 1, agg.Votes[0])
	assert.EqualValues(t, 1, agg.Votes[3])
}

func TestCast_KeysOfOneVoterShareSlotAcrossRealCluster(t *testing.T) {
	ctx := context.Background()
	caster := newCaster(t, startRedis(ctx, t))

	poll, salt := uuid.New(), []byte(strings.Repeat("s", 32))

	for i := range 200 {
		v := voter(t, salt, fmt.Sprintf("viewer-%d", i))
		_, err := caster.Cast(ctx, poll, shardCount, v, []uint8{uint8(i % 4)})
		require.NoErrorf(t, err, "голос %d: CROSSSLOT означает, что hash tag разъехался", i)
	}

	agg, err := caster.Aggregate(ctx, poll, shardCount)
	require.NoError(t, err)
	assert.EqualValues(t, 200, agg.Ballots)
}

func TestCast_CounterExpiresSoRedisDoesNotGrowForever(t *testing.T) {
	ctx := context.Background()
	client := startRedis(ctx, t)

	caster, err := vote.NewCaster(client, 5*time.Second, 0, 5*time.Second)
	require.NoError(t, err)

	poll, salt := uuid.New(), []byte(strings.Repeat("s", 32))
	v := voter(t, salt, "viewer-1")

	_, err = caster.Cast(ctx, poll, shardCount, v, []uint8{0})
	require.NoError(t, err)

	shard := vote.ShardFor(v, shardCount)
	ttl := client.Do(ctx, client.B().Ttl().Key(vote.CounterKey(poll, shard)).Build())
	require.NoError(t, ttl.Error())

	seconds, err := ttl.AsInt64()
	require.NoError(t, err)
	assert.Positive(t, seconds, "счётчик без TTL копится до OOM при noeviction")
}

func TestCluster_RejectsCrossSlotScript(t *testing.T) {
	ctx := context.Background()
	client := startRedis(ctx, t)

	err := client.Do(ctx, client.B().Eval().
		Script("return 1").Numkeys(2).Key("{a}:dedup", "{b}:counter").Build()).Error()

	require.Error(t, err, "стенд обязан проверять CROSSSLOT, иначе тесты ключей ничего не доказывают")
	assert.Contains(t, err.Error(), "CROSSSLOT")
}
