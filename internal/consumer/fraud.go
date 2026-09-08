package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/google/uuid"
	"github.com/redis/rueidis"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/producer"
)

// Fraud считает агрегаты для поиска накрутки.
//
// Отдельная consumer group — в этом весь смысл: Kafka делит партиции между
// членами одной группы, поэтому общая группа отбирала бы сообщения у подсчёта.
// С разными группами отставание или падение анализа не трогает счёт голосов.
type Fraud struct {
	client *kgo.Client
	redis  rueidis.Client
	log    *slog.Logger
	ttl    int64
}

// NewFraud собирает консьюмер анализа.
func NewFraud(client *kgo.Client, redis rueidis.Client, log *slog.Logger, ttlSeconds int64) (*Fraud, error) {
	switch {
	case client == nil:
		return nil, errors.New("consumer: не задан клиент Kafka")
	case redis == nil:
		return nil, errors.New("consumer: не задан клиент Redis")
	}
	if log == nil {
		log = slog.Default()
	}
	if ttlSeconds <= 0 {
		ttlSeconds = 24 * 3600
	}
	return &Fraud{client: client, redis: redis, log: log, ttl: ttlSeconds}, nil
}

// Ключи агрегатов. Только счётчики по подсети и классу устройства: ни voterID,
// ни полного адреса, ни выбора — иначе анализ стал бы хранилищем персональных
// данных, которого мы не заводим.
func fraudNetKey(pollID uuid.UUID) string { return "fraud:{p:" + pollID.String() + "}:net" }
func fraudUAKey(pollID uuid.UUID) string  { return "fraud:{p:" + pollID.String() + "}:ua" }
func fraudCurveKey(pollID uuid.UUID) string {
	return "fraud:{p:" + pollID.String() + "}:curve"
}

// Signals — то, что видит оператор в админке.
type Signals struct {
	// ByNet — голосов на /16-подсеть. Показывает концентрацию, но не адрес.
	ByNet map[string]int64 `json:"by_net"`
	// ByUAClass — голосов на класс устройства. У подсети реального оператора
	// классов десятки; у скрипта — единицы.
	ByUAClass map[string]int64 `json:"by_ua_class"`
	// Curve — голосов по секунде от начала окна. Человек даёт затухающий
	// поток, скрипт — ровный или залповый.
	Curve map[string]int64 `json:"curve"`
}

// Run читает топик и копит агрегаты до отмены контекста.
func (f *Fraud) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		fetches := f.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil //nolint:nilerr
				}
				f.log.ErrorContext(ctx, "fraud: чтение из Kafka", slog.String("error", e.Err.Error()))
			}
			continue
		}

		batch := make(rueidis.Commands, 0, 64)
		fetches.EachRecord(func(rec *kgo.Record) {
			batch = f.appendCommands(ctx, batch, rec)
		})
		if len(batch) == 0 {
			continue
		}

		// Ошибки записи агрегатов не останавливают чтение: анализ не имеет
		// права влиять на дренаж, а пропуск части сигналов переживаем.
		for _, resp := range f.redis.DoMulti(ctx, batch...) {
			if err := resp.Error(); err != nil {
				f.log.WarnContext(ctx, "fraud: запись агрегата", slog.String("error", err.Error()))
				break
			}
		}
		if err := f.client.CommitUncommittedOffsets(ctx); err != nil {
			f.log.WarnContext(ctx, "fraud: коммит оффсетов", slog.String("error", err.Error()))
		}
	}
	return nil
}

func (f *Fraud) appendCommands(ctx context.Context, batch rueidis.Commands, rec *kgo.Record) rueidis.Commands {
	var msg producer.VoteMessage
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		return batch
	}

	second := "0"
	if !msg.ProducedAt.IsZero() {
		second = strconv.FormatInt(msg.ProducedAt.Unix()%3600, 10)
	}

	b := f.redis.B()
	batch = append(batch,
		b.Hincrby().Key(fraudNetKey(msg.PollID)).Field(msg.Net16).Increment(1).Build(),
		f.redis.B().Hincrby().Key(fraudUAKey(msg.PollID)).Field(msg.UAClass).Increment(1).Build(),
		f.redis.B().Hincrby().Key(fraudCurveKey(msg.PollID)).Field(second).Increment(1).Build(),
	)
	_ = ctx
	return batch
}

// Read отдаёт накопленные сигналы.
func (f *Fraud) Read(ctx context.Context, pollID uuid.UUID) (Signals, error) {
	keys := []string{fraudNetKey(pollID), fraudUAKey(pollID), fraudCurveKey(pollID)}

	cmds := make(rueidis.Commands, 0, len(keys))
	for _, k := range keys {
		cmds = append(cmds, f.redis.B().Hgetall().Key(k).Build())
	}

	out := Signals{
		ByNet:     map[string]int64{},
		ByUAClass: map[string]int64{},
		Curve:     map[string]int64{},
	}
	targets := []map[string]int64{out.ByNet, out.ByUAClass, out.Curve}

	for i, resp := range f.redis.DoMulti(ctx, cmds...) {
		fields, err := resp.AsStrMap()
		if err != nil {
			if rueidis.IsRedisNil(err) {
				continue
			}
			return Signals{}, fmt.Errorf("fraud: чтение сигналов: %w", err)
		}
		for k, raw := range fields {
			n, convErr := strconv.ParseInt(raw, 10, 64)
			if convErr != nil {
				continue
			}
			targets[i][k] = n
		}
	}
	return out, nil
}
