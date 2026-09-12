package pollcfg

//go:generate mockgen -source=cache.go -destination=mocks/cache.go -package=mocks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/dubter/televote/internal/domain"
)

var (
	ErrNoRepo      = errors.New("pollcfg: config source is not set")
	ErrBadInterval = errors.New("pollcfg: refresh interval must be positive")
)

const defaultRefreshTimeout = 5 * time.Second

type Repo interface {
	ListActive(ctx context.Context) ([]*domain.Poll, error)
}

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

func (c *HotConfig) Sharding() domain.Sharding {
	return domain.Sharding{PollID: c.ID, ShardCount: c.ShardCount}
}

type snapshot struct {
	bySlug map[string]*HotConfig
	byID   map[uuid.UUID]*HotConfig
}

type Option func(*Cache)

func WithLogger(l *slog.Logger) Option {
	return func(c *Cache) {
		if l != nil {
			c.log = l
		}
	}
}

type Cache struct {
	repo           Repo
	interval       time.Duration
	refreshTimeout time.Duration
	log            *slog.Logger

	current     atomic.Pointer[snapshot]
	lastRefresh atomic.Int64
	obs         AgeObserver
}

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

func (c *Cache) Warm(ctx context.Context) error {
	return c.refresh(ctx)
}

type AgeObserver interface {
	SetConfigAge(seconds float64)
}

func (c *Cache) WithObserver(obs AgeObserver) *Cache {
	c.obs = obs
	return c
}

func (c *Cache) Run(ctx context.Context) {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.refresh(ctx); err != nil {
				c.log.WarnContext(ctx, "pollcfg: config refresh failed, serving the previous snapshot",
					slog.String("error", err.Error()))
			}
			c.reportAge()
		}
	}
}

func (c *Cache) BySlug(slug string) (*HotConfig, bool) {
	snap := c.current.Load()
	if snap == nil {
		return nil, false
	}
	cfg, ok := snap.bySlug[slug]
	return cfg, ok
}

func (c *Cache) ByID(id uuid.UUID) (*HotConfig, bool) {
	snap := c.current.Load()
	if snap == nil {
		return nil, false
	}
	cfg, ok := snap.byID[id]
	return cfg, ok
}

func (c *Cache) reportAge() {
	if c.obs == nil {
		return
	}
	last := c.LastRefresh()
	if last.IsZero() {
		return
	}
	c.obs.SetConfigAge(time.Since(last).Seconds())
}

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
		return fmt.Errorf("pollcfg: read active polls: %w", err)
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

func (c *Cache) hotConfig(ctx context.Context, p *domain.Poll) (*HotConfig, bool) {
	if p == nil {
		return nil, false
	}
	if p.Slug == "" {
		c.log.WarnContext(ctx, "pollcfg: poll without slug skipped", slog.String("id", p.ID.String()))
		return nil, false
	}
	if p.ID == uuid.Nil {
		c.log.WarnContext(ctx, "pollcfg: poll without ID skipped", slog.String("slug", p.Slug))
		return nil, false
	}
	if len(p.Salt) == 0 {
		c.log.WarnContext(ctx, "pollcfg: poll has no voterID derivation salt",
			slog.String("slug", p.Slug))
	}

	return &HotConfig{
		ID:         p.ID,
		Slug:       p.Slug,
		Question:   p.Question,
		Options:    slices.Clone(p.Options),
		Rules:      p.ChoiceRules(),
		Window:     p.Window(),
		ShardCount: p.ShardCount,
		Salt:       bytes.Clone(p.Salt),
	}, true
}
