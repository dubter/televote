// Package consumer применяет принятые голоса: подсчёт и анализ накрутки.
//
// Две независимые группы на одном топике. Анализ не имеет права замедлить или
// сломать подсчёт, и разные group.id — именно та гарантия, которая это
// обеспечивает: отставание или падение одной группы не трогает другую.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/OWNER/televote/internal/pollcfg"
	"github.com/OWNER/televote/internal/producer"
	"github.com/OWNER/televote/internal/vote"
)

// Applier применяет голос в хранилище счётчиков.
type Applier interface {
	Cast(ctx context.Context, pollID uuid.UUID, shardCount uint16, v vote.VoterID, choices []uint8) (vote.Result, error)
}

// ConfigLookup отдаёт конфиг опроса по идентификатору: из Kafka приходит
// pollID, а не slug.
type ConfigLookup interface {
	ByID(id uuid.UUID) (*pollcfg.HotConfig, bool)
}

// Observer получает исход каждого применённого сообщения.
type Observer interface {
	VoteCounted(ctx context.Context, result vote.Result)
	VoteRejected(ctx context.Context, reason string)
}

// Причины отказа. Набор конечный и не содержит пользовательских данных:
// значения уходят в метку метрики, и произвольная строка взорвала бы
// кардинальность.
const (
	reasonMalformed   = "malformed"
	reasonUnknownPoll = "unknown_poll"
	reasonOutOfWindow = "out_of_window"
	reasonBadVoterID  = "bad_voter_id"
)

// Counting считает голоса: читает Kafka и применяет их в Redis.
type Counting struct {
	client  *kgo.Client
	applier Applier
	lookup  ConfigLookup
	obs     Observer
	log     *slog.Logger

	// retryBudget ограничивает попытки применить одно сообщение. Ретраить
	// бесконечно нельзя: партиция встанет и дренаж не закончится никогда.
	retryBudget time.Duration
}

// NewCounting собирает консьюмер подсчёта.
func NewCounting(client *kgo.Client, applier Applier, lookup ConfigLookup, obs Observer, log *slog.Logger) (*Counting, error) {
	switch {
	case client == nil:
		return nil, errors.New("consumer: не задан клиент Kafka")
	case applier == nil:
		return nil, errors.New("consumer: не задан applier")
	case lookup == nil:
		return nil, errors.New("consumer: не задан источник конфигов")
	}
	if log == nil {
		log = slog.Default()
	}
	return &Counting{
		client:      client,
		applier:     applier,
		lookup:      lookup,
		obs:         obs,
		log:         log,
		retryBudget: 30 * time.Second,
	}, nil
}

// Run читает и применяет голоса до отмены контекста.
//
// Оффсет коммитится ПОСЛЕ применения: коммит вперёд потерял бы голоса при
// падении между коммитом и записью. Обратный порядок даёт дубли доставки, но
// они безвредны — применение идемпотентно по voterID.
func (c *Counting) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if errors.Is(e.Err, context.Canceled) {
					return nil
				}
				c.log.ErrorContext(ctx, "consumer: чтение из Kafka",
					slog.String("topic", e.Topic), slog.String("error", e.Err.Error()))
			}
			continue
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			c.applyRecord(ctx, rec)
		})

		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			// Незакоммиченные оффсеты означают повторную доставку, а не потерю.
			c.log.WarnContext(ctx, "consumer: коммит оффсетов не удался",
				slog.String("error", err.Error()))
		}
	}
}

func (c *Counting) applyRecord(ctx context.Context, rec *kgo.Record) {
	var msg producer.VoteMessage
	if err := json.Unmarshal(rec.Value, &msg); err != nil {
		c.reject(ctx, reasonMalformed, err)
		return
	}

	cfg, ok := c.lookup.ByID(msg.PollID)
	if !ok {
		c.reject(ctx, reasonUnknownPoll, fmt.Errorf("опрос %s не найден", msg.PollID))
		return
	}

	// Окно проверяется по метке приёма, а не по времени обработки: голос с
	// 59-й секунды эфира консьюмится через минуты после закрытия и обязан
	// быть засчитан.
	if !cfg.Window.IsOpenAt(msg.ProducedAt) {
		c.reject(ctx, reasonOutOfWindow, fmt.Errorf("голос вне окна: %s", msg.ProducedAt))
		return
	}

	voterID, err := vote.ParseVoterID(msg.VoterID)
	if err != nil {
		c.reject(ctx, reasonBadVoterID, err)
		return
	}

	res, err := c.applyWithRetry(ctx, cfg, voterID, msg.Choices)
	if err != nil {
		c.log.ErrorContext(ctx, "consumer: голос не применён",
			slog.String("poll", msg.PollID.String()), slog.String("error", err.Error()))
		return
	}
	if c.obs != nil {
		c.obs.VoteCounted(ctx, res)
	}
}

// applyWithRetry повторяет применение, пока ошибка транзиентна и есть бюджет.
//
// Классификация здесь стоит дорого в обе стороны: ретрай постоянной ошибки
// заклинит партицию навсегда, а отказ от ретрая транзиентной потеряет голос.
func (c *Counting) applyWithRetry(
	ctx context.Context,
	cfg *pollcfg.HotConfig,
	voterID vote.VoterID,
	choices []uint8,
) (vote.Result, error) {
	deadline := time.Now().Add(c.retryBudget)
	backoff := 20 * time.Millisecond

	for {
		res, err := c.applier.Cast(ctx, cfg.ID, cfg.ShardCount, voterID, choices)
		if err == nil {
			return res, nil
		}
		if !vote.IsRetryable(err) || time.Now().After(deadline) {
			return 0, err
		}

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

func (c *Counting) reject(ctx context.Context, reason string, err error) {
	if c.obs != nil {
		c.obs.VoteRejected(ctx, reason)
	}
	c.log.WarnContext(ctx, "consumer: сообщение отвергнуто",
		slog.String("reason", reason), slog.String("error", err.Error()))
}
