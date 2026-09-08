package vote

//go:generate mockgen -destination=mocks/redis.go -package=mocks github.com/redis/rueidis Client

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
	"github.com/redis/rueidis"

	"github.com/dubter/televote/internal/domain"
)

type Result uint8

const (
	ResultCounted        Result = 1
	ResultAlreadyCounted Result = 2
)

func (r Result) Valid() bool { return r == ResultCounted || r == ResultAlreadyCounted }

func (r Result) String() string {
	switch r {
	case ResultCounted:
		return "counted"
	case ResultAlreadyCounted:
		return "already_counted"
	default:
		return "invalid"
	}
}

var ErrInvalidArgs = errors.New("invalid_vote_args")

const (
	maxJitter          = 0.5
	defaultCmdTimeout  = 250 * time.Millisecond
	aggregateShardCost = time.Millisecond
	maxAggregateWait   = 30 * time.Second
)

type Caster struct {
	client     rueidis.Client
	ttl        time.Duration
	jitter     float64
	cmdTimeout time.Duration
}

func NewCaster(client rueidis.Client, ttl time.Duration, jitter float64, cmdTimeout time.Duration) (*Caster, error) {
	if client == nil {
		return nil, errors.New("vote: nil redis client")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("vote: ttl must be positive, got %s", ttl)
	}
	if jitter < 0 || jitter > maxJitter {
		return nil, fmt.Errorf("vote: jitter is out of range [0, %v]: %v", maxJitter, jitter)
	}
	if cmdTimeout <= 0 {
		cmdTimeout = defaultCmdTimeout
	}
	return &Caster{client: client, ttl: ttl, jitter: jitter, cmdTimeout: cmdTimeout}, nil
}

func (c *Caster) aggregateTimeout(shardCount uint16) time.Duration {
	return min(maxAggregateWait, c.cmdTimeout+time.Duration(shardCount)*aggregateShardCost)
}

func (c *Caster) keysFor(pollID uuid.UUID, shardCount uint16, v VoterID) (dedup, counter string) {
	shard := ShardFor(v, shardCount)
	return DedupKey(pollID, shard, v), CounterKey(pollID, shard)
}

func (c *Caster) ttlSecondsFor(v VoterID) int64 {
	base := c.ttl.Seconds()

	if c.jitter > 0 {
		const span = 2_000_001
		frac := float64(xxhash.Sum64(v[:])%span)/float64(span/2) - 1
		base *= 1 + c.jitter*frac
	}

	return max(1, int64(base))
}

func (c *Caster) Cast(
	ctx context.Context,
	pollID uuid.UUID,
	shardCount uint16,
	v VoterID,
	choices []uint8,
) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cmdTimeout)
	defer cancel()

	if err := validateCast(shardCount, choices); err != nil {
		return 0, err
	}

	dedup, counter := c.keysFor(pollID, shardCount, v)

	args := make([]string, 0, len(choices)+1)
	args = append(args, strconv.FormatInt(c.ttlSecondsFor(v), 10))
	for _, idx := range choices {
		args = append(args, strconv.FormatUint(uint64(idx), 10))
	}

	res, err := voteScript.Exec(ctx, c.client, []string{dedup, counter}, args).ToInt64()
	if err != nil {
		return 0, fmt.Errorf("vote: apply vote: %w", err)
	}

	out := Result(res)
	if !out.Valid() {
		return 0, fmt.Errorf("vote: script returned an unknown outcome %d", res)
	}
	return out, nil
}

func validateCast(shardCount uint16, choices []uint8) error {
	if shardCount == 0 {
		return fmt.Errorf("%w: shardCount is zero", ErrInvalidArgs)
	}
	if len(choices) == 0 {
		return fmt.Errorf("%w: empty choice", ErrInvalidArgs)
	}

	var seen [domain.MaxOptions + 1]bool
	for _, idx := range choices {
		if seen[idx] {
			return fmt.Errorf("%w: index %d chosen twice", ErrInvalidArgs, idx)
		}
		seen[idx] = true
	}
	return nil
}

func (c *Caster) Aggregate(ctx context.Context, pollID uuid.UUID, shardCount uint16) (domain.Aggregate, error) {
	if shardCount == 0 {
		return domain.Aggregate{}, fmt.Errorf("%w: shardCount is zero", ErrInvalidArgs)
	}

	ctx, cancel := context.WithTimeout(ctx, c.aggregateTimeout(shardCount))
	defer cancel()

	cmds := make(rueidis.Commands, 0, shardCount)
	for shard := range shardCount {
		cmds = append(cmds, c.client.B().Hgetall().Key(CounterKey(pollID, shard)).Build())
	}

	out := domain.NewAggregate()
	for _, resp := range c.client.DoMulti(ctx, cmds...) {
		fields, err := resp.AsStrMap()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				continue
			}
			return domain.Aggregate{}, fmt.Errorf("vote: read counters: %w", err)
		}
		for field, raw := range fields {
			n, convErr := strconv.ParseInt(raw, 10, 64)
			if convErr != nil {
				return domain.Aggregate{}, fmt.Errorf("vote: field %q is not a number: %w", field, convErr)
			}
			if field == ballotsField {
				out.Ballots += n
				continue
			}
			idx, convErr := strconv.ParseUint(field, 10, 8)
			if convErr != nil {
				return domain.Aggregate{}, fmt.Errorf("vote: field %q is not an option index: %w", field, convErr)
			}
			out.Add(uint8(idx), n)
		}
	}
	return out, nil
}

var permanentErrors = []error{
	context.Canceled,
	rueidis.ErrClosing,
	ErrInvalidArgs,
	ErrBadClientID,
	ErrBadSalt,
	domain.ErrInvalidChoices,
	domain.ErrPollClosed,
}

func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	for _, permanent := range permanentErrors {
		if errors.Is(err, permanent) {
			return false
		}
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
