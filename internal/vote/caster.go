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

// Result — исход применения голоса.
//
// Нулевого значения намеренно нет: провалившийся вызов возвращает Result(0), и
// он не должен выглядеть успехом. Будь Counted нулём, `res, _ := Cast(...)`
// с проигнорированной ошибкой читался бы как «голос посчитан».
type Result uint8

const (
	// ResultCounted — голос учтён впервые.
	ResultCounted Result = 1
	// ResultAlreadyCounted — этот голосующий уже учтён в этом опросе.
	ResultAlreadyCounted Result = 2
)

// Valid сообщает, является ли значение настоящим исходом.
func (r Result) Valid() bool { return r == ResultCounted || r == ResultAlreadyCounted }

// String уходит в метку метрики, поэтому набор значений конечен и не содержит
// ничего пользовательского.
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

// ErrInvalidArgs — голос не может быть применён из-за аргументов. Ретраем не
// лечится: повтор с теми же аргументами даст тот же отказ и заклинит партицию.
var ErrInvalidArgs = errors.New("invalid_vote_args")

// maxJitter — джиттер больше половины TTL перестаёт быть размазыванием
// истечения и становится лотереей: нижняя граница уходит вдвое ниже номинала.
const maxJitter = 0.5

// Caster применяет голоса в Redis.
type Caster struct {
	client rueidis.Client
	ttl    time.Duration
	jitter float64
}

// NewCaster собирает Caster. Проверки строгие: каждая из них ловит конфиг,
// который сломал бы дедуп молча, уже в эфире.
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

// keysFor выводит пару ключей шарда. shardCount берётся из аргумента, то есть
// из строки опроса: возьми его Caster из глобального конфига, смена значения в
// эфире сделала бы дедуп-ключи проголосовавших недостижимыми и открыла
// повторное голосование без единой ошибки в логе.
func (c *Caster) keysFor(pollID uuid.UUID, shardCount uint16, v VoterID) (dedup, counter string) {
	shard := ShardFor(v, shardCount)
	return DedupKey(pollID, shard, v), CounterKey(pollID, shard)
}

// ttlSecondsFor считает TTL дедуп-ключа с джиттером.
func (c *Caster) ttlSecondsFor(v VoterID) int64 {
	base := c.ttl.Seconds()

	if c.jitter > 0 {
		// Детерминированная доля в [-1, 1] из хэша идентификатора.
		const span = 2_000_001
		frac := float64(xxhash.Sum64(v[:])%span)/float64(span/2) - 1
		base *= 1 + c.jitter*frac
	}

	secs := int64(base)
	if secs < 1 {
		// EX 0 удалил бы ключ немедленно и открыл повторное голосование.
		return 1
	}
	return secs
}

// Cast применяет голос: дедуп и инкремент одной атомарной операцией.
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

// validateCast проверяет аргументы до обращения к Redis: пустой выбор создал бы
// бюллетень без голосов и завысил знаменатель процентов, а дубль индекса —
// два голоса за один вариант.
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

// Aggregate сворачивает счётчики всех шардов опроса в один агрегат.
//
// Читает с реплик: fan-in по тысячам ключей не должен делить мультиплекс с
// записями консьюмеров и давать head-of-line blocking на дренаже.
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

// permanentErrors — ошибки, которые ретрай не лечит.
var permanentErrors = []error{
	context.Canceled,
	rueidis.ErrClosing,
	ErrInvalidArgs,
	ErrBadClientID,
	ErrBadSalt,
	domain.ErrInvalidChoices,
	domain.ErrPollClosed,
}

// IsRetryable сообщает консьюмеру, имеет ли смысл повторить применение голоса.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}
	for _, permanent := range permanentErrors {
		if errors.Is(err, permanent) {
			return false
		}
	}
	// Ошибка самого Redis (WRONGTYPE, синтаксис скрипта) повторится дословно.
	// Исключение — сигналы «занят, попробуй позже».
	var redisErr *rueidis.RedisError
	if errors.As(err, &redisErr) {
		return redisErr.IsLoading() || redisErr.IsClusterDown() || redisErr.IsTryAgain()
	}
	return true
}
