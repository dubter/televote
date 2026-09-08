// Package snapshot переносит агрегат из Redis в Postgres и ведёт жизненный
// цикл опроса по расписанию.
package snapshot

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/dubter/televote/internal/domain"
)

// Aggregator читает счётчики опроса из Redis.
type Aggregator interface {
	Aggregate(ctx context.Context, pollID uuid.UUID, shardCount uint16) (domain.Aggregate, error)
}

// Results хранит агрегат.
type Results interface {
	Upsert(ctx context.Context, pollID uuid.UUID, a domain.Aggregate) error
	Get(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, error)
	SaveAdjusted(ctx context.Context, pollID uuid.UUID, a domain.Aggregate, excludedNets []string) error
}

// Polls даёт доступ к жизненному циклу опросов.
type Polls interface {
	ListActive(ctx context.Context) ([]*domain.Poll, error)
	Transition(ctx context.Context, id uuid.UUID, to domain.Status, version uint32) error
}

// LagReader сообщает, сколько сообщений опроса ещё не обработано.
type LagReader interface {
	Lag(ctx context.Context) (int64, error)
}

// Snapshotter ведёт агрегат и расписание опросов.
type Snapshotter struct {
	agg      Aggregator
	results  Results
	polls    Polls
	lag      LagReader
	interval time.Duration
	grace    time.Duration
	now      func() time.Time
	log      *slog.Logger
	obs      Observer
}

// Observer публикует наблюдаемое состояние дренажа.
type Observer interface {
	SetConsumerLag(n int64)
	SetBallots(pollSlug string, n int64)
}

// Config — параметры снапшотера.
type Config struct {
	Interval time.Duration
	Grace    time.Duration
	Now      func() time.Time
	Log      *slog.Logger
	Observer Observer
}

// New собирает снапшотер.
func New(agg Aggregator, results Results, polls Polls, lag LagReader, cfg Config) (*Snapshotter, error) {
	switch {
	case agg == nil:
		return nil, errors.New("snapshot: не задан агрегатор")
	case results == nil:
		return nil, errors.New("snapshot: не задано хранилище результатов")
	case polls == nil:
		return nil, errors.New("snapshot: не задан репозиторий опросов")
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.Grace <= 0 {
		cfg.Grace = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	return &Snapshotter{
		agg: agg, results: results, polls: polls, lag: lag,
		interval: cfg.Interval, grace: cfg.Grace, now: cfg.Now, log: cfg.Log,
		obs: cfg.Observer,
	}, nil
}

// Run ведёт цикл до отмены контекста.
func (s *Snapshotter) Run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Tick(ctx); err != nil {
				s.log.WarnContext(ctx, "snapshot: цикл завершился с ошибкой",
					slog.String("error", err.Error()))
			}
		}
	}
}

// Tick обрабатывает все активные опросы: открывает по расписанию, снимает
// агрегат, финализирует закончившиеся.
func (s *Snapshotter) Tick(ctx context.Context) error {
	polls, err := s.polls.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("snapshot: список активных опросов: %w", err)
	}

	var errs []error
	for _, p := range polls {
		if err := s.handle(ctx, p); err != nil {
			errs = append(errs, fmt.Errorf("опрос %s: %w", p.Slug, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Snapshotter) handle(ctx context.Context, p *domain.Poll) error {
	now := s.now()

	if p.ShouldOpenAt(now) {
		if err := s.polls.Transition(ctx, p.ID, domain.StatusOpen, p.Version); err != nil {
			return fmt.Errorf("открытие по расписанию: %w", err)
		}
		s.log.InfoContext(ctx, "snapshot: опрос открыт по расписанию", slog.String("slug", p.Slug))
		return nil
	}

	if p.Status != domain.StatusOpen {
		return nil
	}

	if _, err := s.TickOnce(ctx, p); err != nil {
		return err
	}

	// Финализация только после grace и только при нулевом лаге.
	if now.Before(p.ClosesAt.Add(s.grace)) {
		return nil
	}
	drained, err := s.drained(ctx)
	if err != nil {
		return err
	}
	if !drained {
		s.log.InfoContext(ctx, "snapshot: дренаж ещё идёт, финализация отложена",
			slog.String("slug", p.Slug))
		return nil
	}
	return s.Finalize(ctx, p, nil)
}

// TickOnce снимает агрегат опроса и кладёт его в Postgres.
func (s *Snapshotter) TickOnce(ctx context.Context, p *domain.Poll) (domain.Aggregate, error) {
	fresh, err := s.agg.Aggregate(ctx, p.ID, p.ShardCount)
	if err != nil {
		return domain.Aggregate{}, fmt.Errorf("чтение счётчиков: %w", err)
	}

	prev, err := s.results.Get(ctx, p.ID)
	if err != nil {
		return domain.Aggregate{}, fmt.Errorf("чтение предыдущего снимка: %w", err)
	}

	merged := fresh.MergeMax(prev)
	if err := s.results.Upsert(ctx, p.ID, merged); err != nil {
		return domain.Aggregate{}, fmt.Errorf("запись снимка: %w", err)
	}
	if s.obs != nil {
		s.obs.SetBallots(p.Slug, merged.Ballots)
	}
	return merged, nil
}

// Finalize фиксирует результат и закрывает опрос.
func (s *Snapshotter) Finalize(ctx context.Context, p *domain.Poll, excludedNets []string) error {
	final, err := s.TickOnce(ctx, p)
	if err != nil {
		return fmt.Errorf("финальный снимок: %w", err)
	}
	if err := s.results.SaveAdjusted(ctx, p.ID, final, excludedNets); err != nil {
		return fmt.Errorf("сохранение итогового результата: %w", err)
	}
	if err := s.polls.Transition(ctx, p.ID, domain.StatusClosed, p.Version); err != nil {
		return fmt.Errorf("закрытие опроса: %w", err)
	}

	s.log.InfoContext(ctx, "snapshot: опрос финализирован",
		slog.String("slug", p.Slug), slog.Int64("ballots", final.Ballots))
	return nil
}

// drained сообщает, доехали ли все принятые голоса до Redis.
func (s *Snapshotter) drained(ctx context.Context) (bool, error) {
	if s.lag == nil {
		s.log.WarnContext(ctx, "snapshot: источник consumer lag не задан, финализация по времени")
		return true, nil
	}
	lag, err := s.lag.Lag(ctx)
	if err != nil {
		return false, fmt.Errorf("чтение consumer lag: %w", err)
	}
	if s.obs != nil {
		s.obs.SetConsumerLag(lag)
	}
	return lag == 0, nil
}
