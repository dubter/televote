//go:build integration

package redis

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

	"github.com/dubter/televote/internal/domain"
)

const shardCount = 64

func startRedis(ctx context.Context, t *testing.T) *Client {
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

	raw, err := rueidis.NewClient(rueidis.ClientOption{
		InitAddress: []string{endpoint}, DisableCache: true, ForceSingleClient: true,
	})
	require.NoError(t, err)
	t.Cleanup(raw.Close)

	slots := make([]string, 0, 16384)
	for slot := range 16384 {
		slots = append(slots, fmt.Sprint(slot))
	}
	require.NoError(t, raw.Do(ctx,
		raw.B().Arbitrary(append([]string{"CLUSTER", "ADDSLOTS"}, slots...)...).Build()).Error())

	require.Eventually(t, func() bool {
		info, infoErr := raw.Do(ctx, raw.B().Arbitrary("CLUSTER", "INFO").Build()).ToString()
		return infoErr == nil && strings.Contains(info, "cluster_state:ok")
	}, time.Minute, 200*time.Millisecond, "cluster mode did not come up")

	return &Client{raw: raw}
}

func newTally(t *testing.T, client *Client) *Tally {
	t.Helper()

	tally, err := NewTally(client, time.Hour, 0, 5*time.Second)
	require.NoError(t, err)
	return tally
}

func newTarget() domain.Sharding {
	return domain.Sharding{PollID: uuid.New(), ShardCount: shardCount}
}

func voter(t *testing.T, id string) domain.VoterID {
	t.Helper()

	v, err := domain.DeriveVoterID([]byte(strings.Repeat("s", 32)), id)
	require.NoError(t, err)
	return v
}

func TestApply_SecondVoteOfSameVoterIsNotCounted(t *testing.T) {
	ctx := context.Background()
	tally := newTally(t, startRedis(ctx, t))

	target := newTarget()
	v := voter(t, "viewer-1")

	first, err := tally.Apply(ctx, target, v, []uint8{1})
	require.NoError(t, err)
	assert.Equal(t, domain.VoteCounted, first)

	second, err := tally.Apply(ctx, target, v, []uint8{2})
	require.NoError(t, err)
	assert.Equal(t, domain.VoteAlreadyCounted, second, "дедуп обязан отсечь повтор")

	agg, err := tally.Aggregate(ctx, target)
	require.NoError(t, err)
	assert.EqualValues(t, 1, agg.Ballots)
	assert.EqualValues(t, 1, agg.Votes[1])
	assert.EqualValues(t, 0, agg.Votes[2], "второй выбор не должен попасть в счёт")
}

func TestApply_IsIdempotentUnderConcurrentRedelivery(t *testing.T) {
	ctx := context.Background()
	tally := newTally(t, startRedis(ctx, t))

	target := newTarget()
	v := voter(t, "viewer-1")

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
			res, err := tally.Apply(ctx, target, v, []uint8{0})
			if err == nil && res == domain.VoteCounted {
				mu.Lock()
				counted++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, 1, counted, "ровно одна из параллельных доставок обязана быть засчитана")

	agg, err := tally.Aggregate(ctx, target)
	require.NoError(t, err)
	assert.EqualValues(t, 1, agg.Ballots)
}

func TestApply_MultipleChoiceCountsEveryOptionAndOneBallot(t *testing.T) {
	ctx := context.Background()
	tally := newTally(t, startRedis(ctx, t))

	target := newTarget()

	_, err := tally.Apply(ctx, target, voter(t, "viewer-1"), []uint8{0, 3})
	require.NoError(t, err)

	agg, err := tally.Aggregate(ctx, target)
	require.NoError(t, err)
	assert.EqualValues(t, 1, agg.Ballots, "бюллетень один, даже если выборов несколько")
	assert.EqualValues(t, 1, agg.Votes[0])
	assert.EqualValues(t, 1, agg.Votes[3])
}

func TestApply_KeysOfOneVoterShareSlotAcrossRealCluster(t *testing.T) {
	ctx := context.Background()
	tally := newTally(t, startRedis(ctx, t))

	target := newTarget()

	for i := range 200 {
		v := voter(t, fmt.Sprintf("viewer-%d", i))
		_, err := tally.Apply(ctx, target, v, []uint8{uint8(i % 4)})
		require.NoErrorf(t, err, "голос %d: CROSSSLOT означает, что hash tag разъехался", i)
	}

	agg, err := tally.Aggregate(ctx, target)
	require.NoError(t, err)
	assert.EqualValues(t, 200, agg.Ballots)
}

func TestApply_CounterExpiresSoRedisDoesNotGrowForever(t *testing.T) {
	ctx := context.Background()
	client := startRedis(ctx, t)

	tally, err := NewTally(client, 5*time.Second, 0, 5*time.Second)
	require.NoError(t, err)

	target := newTarget()
	v := voter(t, "viewer-1")

	_, err = tally.Apply(ctx, target, v, []uint8{0})
	require.NoError(t, err)

	key := counterKey(target.PollID, shardFor(v, shardCount))
	ttl := client.raw.Do(ctx, client.raw.B().Ttl().Key(key).Build())
	require.NoError(t, ttl.Error())

	seconds, err := ttl.AsInt64()
	require.NoError(t, err)
	assert.Positive(t, seconds, "счётчик без TTL копится до OOM при noeviction")
}

func TestCluster_RejectsCrossSlotScript(t *testing.T) {
	ctx := context.Background()
	client := startRedis(ctx, t)

	err := client.raw.Do(ctx, client.raw.B().Eval().
		Script("return 1").Numkeys(2).Key("{a}:dedup", "{b}:counter").Build()).Error()

	require.Error(t, err, "стенд обязан проверять CROSSSLOT, иначе тесты ключей ничего не доказывают")
	assert.Contains(t, err.Error(), "CROSSSLOT")
}

func TestOpen_PingsTheClusterThroughThePublicClient(t *testing.T) {
	ctx := context.Background()
	client := startRedis(ctx, t)

	require.NoError(t, client.Ping(ctx))
}
