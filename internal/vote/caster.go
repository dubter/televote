package vote

import (
	"context"
	"errors"
	"fmt"
	"strconv"
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

const maxJitter = 0.5

type Caster struct {
	client rueidis.Client
	ttl    time.Duration
	jitter float64
}

func NewCaster(client rueidis.Client, ttl time.Duration, jitter float64) (*Caster, error) {
	if client == nil {
		return nil, errors.New("vote: nil redis client")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("vote: ttl должен быть положительным, получено %s", ttl)
	}
	if jitter < 0 || jitter > maxJitter {
		return nil, fmt.Errorf("vote: джиттер вне диапазона [0, %v]: %v", maxJitter, jitter)
	}
	return &Caster{client: client, ttl: ttl, jitter: jitter}, nil
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

	secs := int64(base)
	if secs < 1 {
		return 1
	}
	return secs
}

func (c *Caster) Cast(
	ctx context.Context,
	pollID uuid.UUID,
	shardCount uint16,
	v VoterID,
	choices []uint8,
) (Result, error) {
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
		return 0, fmt.Errorf("vote: применение голоса: %w", err)
	}

	out := Result(res)
	if !out.Valid() {
		return 0, fmt.Errorf("vote: скрипт вернул неизвестный исход %d", res)
	}
	return out, nil
}

func validateCast(shardCount uint16, choices []uint8) error {
	if shardCount == 0 {
		return fmt.Errorf("%w: shardCount равен нулю", ErrInvalidArgs)
	}
	if len(choices) == 0 {
		return fmt.Errorf("%w: пустой выбор", ErrInvalidArgs)
	}

	var seen [domain.MaxOptions + 1]bool
	for _, idx := range choices {
		if seen[idx] {
			return fmt.Errorf("%w: индекс %d выбран дважды", ErrInvalidArgs, idx)
		}
		seen[idx] = true
	}
	return nil
}

func (c *Caster) Aggregate(ctx context.Context, pollID uuid.UUID, shardCount uint16) (domain.Aggregate, error) {
	if shardCount == 0 {
		return domain.Aggregate{}, fmt.Errorf("%w: shardCount равен нулю", ErrInvalidArgs)
	}

	cmds := make(rueidis.Commands, 0, shardCount)
	for shard := uint16(0); shard < shardCount; shard++ {
		cmds = append(cmds, c.client.B().Hgetall().Key(CounterKey(pollID, shard)).Build())
	}

	out := domain.Aggregate{Votes: make(map[uint8]int64)}
	for _, resp := range c.client.DoMulti(ctx, cmds...) {
		fields, err := resp.AsStrMap()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				continue
			}
			return domain.Aggregate{}, fmt.Errorf("vote: чтение счётчиков: %w", err)
		}
		for field, raw := range fields {
			n, convErr := strconv.ParseInt(raw, 10, 64)
			if convErr != nil {
				return domain.Aggregate{}, fmt.Errorf("vote: поле %q не число: %w", field, convErr)
			}
			if field == ballotsField {
				out.Ballots += n
				continue
			}
			idx, convErr := strconv.ParseUint(field, 10, 8)
			if convErr != nil {
				return domain.Aggregate{}, fmt.Errorf("vote: поле %q не индекс опции: %w", field, convErr)
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
	var redisErr *rueidis.RedisError
	if errors.As(err, &redisErr) {
		return redisErr.IsLoading() || redisErr.IsClusterDown() || redisErr.IsTryAgain()
	}
	return true
}
