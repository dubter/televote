// Package pollcfg держит конфиги активных опросов в памяти процесса.
package pollcfg

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/dubter/televote/internal/domain"
)

// Ошибки сборки кэша. Полуготовый кэш хуже отказа: он тихо отдаёт пустой
// конфиг, и опрос выглядит несуществующим.
var (
	// ErrNoRepo — источник конфига не задан.
	ErrNoRepo = errors.New("pollcfg: источник конфига не задан")
	// ErrBadInterval — интервал обновления неположителен.
	ErrBadInterval = errors.New("pollcfg: интервал обновления должен быть положительным")
)

// defaultRefreshTimeout ограничивает один поход к источнику. Зависший запрос
// иначе держал бы старт инстанса вечно, а рефрешер — навсегда на прежнем снимке.
const defaultRefreshTimeout = 5 * time.Second

// Repo — источник конфига. Интерфейс объявлен на стороне потребителя и нарочно
// узкий: кэшу не нужен весь репозиторий опросов.
type Repo interface {
	ListActive(ctx context.Context) ([]*domain.Poll, error)
}

// HotConfig — всё, что нужно и приёму голоса, и публичной выдаче конфига.
// Значение неизменяемо после публикации: читатели видят его без блокировок.
type HotConfig struct {
	ID         uuid.UUID
	Slug       string
	Question   string
	Options    []domain.Option
	Rules      domain.ChoiceRules
	Window     domain.Window
	ShardCount uint16
	Salt       []byte
}

// snapshot — неизменяемый срез состояния. Рефрешер подменяет его целиком, а не
// правит на месте, поэтому читателю не нужен ни мьютекс, ни копия.
type snapshot struct {
	bySlug map[string]*HotConfig
	byID   map[uuid.UUID]*HotConfig
}

// Option настраивает кэш.
type Option func(*Cache)

// WithLogger задаёт логгер. Молчаливый отказ рефрешера — это ровно тот тихий
// отказ, ради которого ведётся таблица инвариантов.
func WithLogger(l *slog.Logger) Option {
	return func(c *Cache) {
		if l != nil {
			c.log = l
		}
	}
}

// WithRefreshTimeout ограничивает время одного обращения к источнику.
func WithRefreshTimeout(d time.Duration) Option {
	return func(c *Cache) {
		if d > 0 {
			c.refreshTimeout = d
		}
	}
}

// Cache — конфиги активных опросов в памяти.
type Cache struct {
	repo           Repo
	interval       time.Duration
	refreshTimeout time.Duration
	log            *slog.Logger

	// current и lastRefresh читаются с горячего пути, поэтому оба атомарные:
	current     atomic.Pointer[snapshot]
	lastRefresh atomic.Int64 // UnixNano последнего УДАЧНОГО обновления
}

// NewCache собирает кэш.
func NewCache(repo Repo, interval time.Duration, opts ...Option) (*Cache, error) {
	if repo == nil {
		return nil, ErrNoRepo
	}
	if interval <= 0 {
		return nil, fmt.Errorf("%w: %s", ErrBadInterval, interval)
	}

	c := &Cache{
		repo:           repo,
		interval:       interval,
		refreshTimeout: defaultRefreshTimeout,
		log:            slog.Default(),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Warm загружает конфиги до прохождения readiness.
func (c *Cache) Warm(ctx context.Context) error {
	return c.refresh(ctx)
}

// Run обновляет снимок в фоне до отмены контекста.
func (c *Cache) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.refresh(ctx); err != nil {
				c.log.WarnContext(ctx, "pollcfg: обновление конфига не удалось, работаем на прежнем снимке",
					slog.String("error", err.Error()))
			}
		}
	}
}

// BySlug отдаёт конфиг по короткой ссылке. Только память, никакой сети.
func (c *Cache) BySlug(slug string) (*HotConfig, bool) {
	snap := c.current.Load()
	if snap == nil {
		return nil, false
	}
	cfg, ok := snap.bySlug[slug]
	return cfg, ok
}

// ByID отдаёт конфиг по идентификатору: консьюмер подсчёта получает из Kafka
// pollID, а не slug, и без этого поиска обходил бы всю карту на каждое сообщение.
func (c *Cache) ByID(id uuid.UUID) (*HotConfig, bool) {
	snap := c.current.Load()
	if snap == nil {
		return nil, false
	}
	cfg, ok := snap.byID[id]
	return cfg, ok
}

// LastRefresh — время последнего УДАЧНОГО обновления. Именно удачного: иначе
// по этой метке нельзя было бы увидеть застой на устаревшем снимке.
func (c *Cache) LastRefresh() time.Time {
	ns := c.lastRefresh.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}

func (c *Cache) refresh(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.refreshTimeout)
	defer cancel()

	polls, err := c.repo.ListActive(ctx)
	if err != nil {
		return fmt.Errorf("pollcfg: чтение активных опросов: %w", err)
	}

	snap := &snapshot{
		bySlug: make(map[string]*HotConfig, len(polls)),
		byID:   make(map[uuid.UUID]*HotConfig, len(polls)),
	}
	for _, p := range polls {
		cfg, ok := c.hotConfig(ctx, p)
		if !ok {
			continue
		}
		snap.bySlug[cfg.Slug] = cfg
		snap.byID[cfg.ID] = cfg
	}

	c.current.Store(snap)
	c.lastRefresh.Store(time.Now().UnixNano())
	return nil
}

// hotConfig превращает строку опроса в неизменяемый конфиг.
func (c *Cache) hotConfig(ctx context.Context, p *domain.Poll) (*HotConfig, bool) {
	if p == nil {
		return nil, false
	}
	if p.Slug == "" {
		// Конфиг под пустым ключом отвечал бы на запрос без slug.
		c.log.WarnContext(ctx, "pollcfg: опрос без slug пропущен", slog.String("id", p.ID.String()))
		return nil, false
	}
	if p.ID == uuid.Nil {
		// Нулевой ID склеил бы разные опросы в одну запись.
		c.log.WarnContext(ctx, "pollcfg: опрос без ID пропущен", slog.String("slug", p.Slug))
		return nil, false
	}
	if len(p.Salt) == 0 {
		c.log.WarnContext(ctx, "pollcfg: у опроса нет соли вывода voterID",
			slog.String("slug", p.Slug))
	}

	options := make([]domain.Option, len(p.Options))
	copy(options, p.Options)

	salt := make([]byte, len(p.Salt))
	copy(salt, p.Salt)

	return &HotConfig{
		ID:         p.ID,
		Slug:       p.Slug,
		Question:   p.Question,
		Options:    options,
		Rules:      p.ChoiceRules(),
		Window:     p.Window(),
		ShardCount: p.ShardCount,
		Salt:       salt,
	}, true
}
