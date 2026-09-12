package redis

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/redis/rueidis"

	"github.com/dubter/televote/internal/domain"
)

const (
	maxJitter          = 0.5
	defaultCmdTimeout  = 250 * time.Millisecond
	aggregateShardCost = time.Millisecond
	maxAggregateWait   = 30 * time.Second
)

type Tally struct {
	client     rueidis.Client
	ttl        time.Duration
	jitter     float64
	cmdTimeout time.Duration
}

func NewTally(client *Client, ttl time.Duration, jitter float64, cmdTimeout time.Duration) (*Tally, error) {
	if client == nil {
		return nil, errors.New("redis: tally without a client")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("redis: ttl must be positive, got %s", ttl)
	}
	if jitter < 0 || jitter > maxJitter {
		return nil, fmt.Errorf("redis: jitter is out of range [0, %v]: %v", maxJitter, jitter)
	}
	if cmdTimeout <= 0 {
		cmdTimeout = defaultCmdTimeout
	}
	return &Tally{client: client.raw, ttl: ttl, jitter: jitter, cmdTimeout: cmdTimeout}, nil
}

func (t *Tally) Apply(
	ctx context.Context,
	target domain.Sharding,
	v domain.VoterID,
	choices []uint8,
) (domain.VoteResult, error) {
	if err := validateVote(target, choices); err != nil {
		return 0, err
	}

	ctx, cancel := context.WithTimeout(ctx, t.cmdTimeout)
	defer cancel()

	dedup, counter := t.keysFor(target, v)

	args := make([]string, 0, len(choices)+1)
	args = append(args, strconv.FormatInt(t.ttlSecondsFor(v), 10))
	for _, idx := range choices {
		args = append(args, strconv.FormatUint(uint64(idx), 10))
	}

	res, err := voteScript.Exec(ctx, t.client, []string{dedup, counter}, args).ToInt64()
	if err != nil {
		return 0, fmt.Errorf("redis: apply vote: %w", classify(err))
	}

	out := domain.VoteResult(res)
	if !out.Valid() {
		return 0, fmt.Errorf("redis: vote script returned an unknown outcome %d", res)
	}
	return out, nil
}

func (t *Tally) Aggregate(ctx context.Context, target domain.Sharding) (domain.Aggregate, error) {
	if !target.Valid() {
		return domain.Aggregate{}, fmt.Errorf("%w: poll %s with %d shards", domain.ErrInvalidVote, target.PollID, target.ShardCount)
	}

	ctx, cancel := context.WithTimeout(ctx, t.aggregateTimeout(target.ShardCount))
	defer cancel()

	cmds := make(rueidis.Commands, 0, target.ShardCount)
	for shard := range target.ShardCount {
		cmds = append(cmds, t.client.B().Hgetall().Key(counterKey(target.PollID, shard)).Build())
	}

	out := domain.NewAggregate()
	for _, resp := range t.client.DoMulti(ctx, cmds...) {
		fields, err := resp.AsStrMap()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				continue
			}
			return domain.Aggregate{}, fmt.Errorf("redis: read counters: %w", classify(err))
		}
		if err := mergeCounters(&out, fields); err != nil {
			return domain.Aggregate{}, err
		}
	}
	return out, nil
}

func mergeCounters(out *domain.Aggregate, fields map[string]string) error {
	for field, raw := range fields {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("redis: counter field %q is not a number: %w", field, err)
		}
		if field == ballotsField {
			out.Ballots += n
			continue
		}
		idx, err := strconv.ParseUint(field, 10, 8)
		if err != nil {
			return fmt.Errorf("redis: counter field %q is not an option index: %w", field, err)
		}
		out.Add(uint8(idx), n)
	}
	return nil
}

func validateVote(target domain.Sharding, choices []uint8) error {
	if !target.Valid() {
		return fmt.Errorf("%w: poll %s with %d shards", domain.ErrInvalidVote, target.PollID, target.ShardCount)
	}
	if len(choices) == 0 {
		return fmt.Errorf("%w: empty choice", domain.ErrInvalidVote)
	}

	var seen [domain.MaxOptions + 1]bool
	for _, idx := range choices {
		if seen[idx] {
			return fmt.Errorf("%w: index %d chosen twice", domain.ErrInvalidVote, idx)
		}
		seen[idx] = true
	}
	return nil
}

func (t *Tally) keysFor(target domain.Sharding, v domain.VoterID) (dedup, counter string) {
	shard := shardFor(v, target.ShardCount)
	return dedupKey(target.PollID, shard, v), counterKey(target.PollID, shard)
}

func (t *Tally) ttlSecondsFor(v domain.VoterID) int64 {
	base := t.ttl.Seconds()

	if t.jitter > 0 {
		const span = 2_000_001
		frac := float64(xxhash.Sum64(v[:])%span)/float64(span/2) - 1
		base *= 1 + t.jitter*frac
	}

	return max(1, int64(base))
}

func (t *Tally) aggregateTimeout(shardCount uint16) time.Duration {
	return min(maxAggregateWait, t.cmdTimeout+time.Duration(shardCount)*aggregateShardCost)
}

func classify(err error) error {
	if transient(err) {
		return fmt.Errorf("%w: %w", domain.ErrStoreUnavailable, err)
	}
	return err
}

func transient(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, rueidis.ErrClosing) {
		return false
	}
	if redisErr, ok := errors.AsType[*rueidis.RedisError](err); ok {
		_, moved := redisErr.IsMoved()
		_, ask := redisErr.IsAsk()
		return moved || ask ||
			redisErr.IsLoading() || redisErr.IsClusterDown() || redisErr.IsTryAgain() ||
			transientReply(redisErr.Error())
	}
	return true
}

var transientReplies = []string{
	"READONLY", "MASTERDOWN", "CLUSTERDOWN", "TRYAGAIN",
	"LOADING", "OOM", "BUSY", "NOREPLICAS",
}

func transientReply(msg string) bool {
	for _, prefix := range transientReplies {
		if strings.HasPrefix(msg, prefix) {
			return true
		}
	}
	return false
}
