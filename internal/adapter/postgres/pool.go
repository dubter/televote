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
	ErrSlugTaken = domain.ErrSlugTaken

	ErrVersionConflict = domain.ErrVersionConflict

	ErrNotFound = domain.ErrNotFound

	ErrEmptyDSN = errors.New("postgres: empty DSN")
)

const (
	defaultMaxConns = int32(10)
	minConns        = int32(2)
	pingTimeout     = 5 * time.Second
)

type Config struct {
	DSN      string
	MaxConns int32
}

type DB struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, cfg Config) (*DB, error) {
	if cfg.DSN == "" {
		return nil, ErrEmptyDSN
	}

	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("postgres: parse DSN: %w", err)
	}

	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if pc.MaxConns <= 0 {
		pc.MaxConns = defaultMaxConns
	}
	if pc.MinConns == 0 && pc.MaxConns >= minConns {
		pc.MinConns = minConns
	}
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement
	pc.ConnConfig.Tracer = otelpgx.NewTracer()

	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("postgres: create pool: %w", err)
	}

	db := &DB{pool: pool}
	if err := db.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return db, nil
}

func (d *DB) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	if err := d.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: ping: %w", err)
	}
	return nil
}

func (d *DB) Close() {
	d.pool.Close()
}

func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}
