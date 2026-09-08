package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/dubter/televote/internal/domain"
)

var (
	ErrSlugTaken = errors.New("slug_taken")

	ErrVersionConflict = errors.New("version_conflict")

	ErrNotFound = domain.ErrNotFound
)

const (
	defaultMaxConns = int32(10)
	pingTimeout     = 5 * time.Second
)

type Option func(*pgxpool.Config)

func WithMaxConns(n int32) Option {
	return func(c *pgxpool.Config) {
		if n > 0 {
			c.MaxConns = n
		}
	}
}

func NewPool(ctx context.Context, dsn string, opts ...Option) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, errors.New("postgres: empty DSN")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}

	if cfg.MaxConns == 0 {
		cfg.MaxConns = defaultMaxConns
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.MinConns == 0 && cfg.MaxConns >= 2 {
		cfg.MinConns = 2
	}

	cfg.ConnConfig.Tracer = otelpgx.NewTracer()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}

	return pool, nil
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}
