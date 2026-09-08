// Package postgres — адаптер control plane: конфигурация опросов, агрегат
// результатов, администраторы и аудит.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Ошибки адаптера. Сравнивать только через errors.Is: репозитории оставляют за
// собой право обернуть сентинел деталями запроса.
var (
	// ErrSlugTaken — слаг занят. Уникальность обеспечивает БД, а не пара
	// SELECT+INSERT: между ними успевает вклиниться второй админ.
	ErrSlugTaken = errors.New("slug_taken")

	// ErrVersionConflict — строку изменили между чтением и записью.
	ErrVersionConflict = errors.New("version_conflict")

	// ErrNotFound — запрошенной строки нет.
	ErrNotFound = errors.New("not_found")
)

// Пул маленький: Postgres вне горячего пути.
const (
	defaultMaxConns = int32(10)
	// Без дедлайна под с битым DSN «стартовал» бы минутами.
	pingTimeout = 5 * time.Second
)

// Option настраивает пул поверх параметров DSN.
//
// Вариативный параметр, а не отдельный конструктор: NewPool(ctx, dsn) остаётся
// корректным вызовом, а размер пула приезжает из конфига сервиса (POSTGRES_MAX_CONNS),
// который знает про роль процесса — у приёма и у консьюмера она разная.
type Option func(*pgxpool.Config)

// WithMaxConns задаёт размер пула. Значение ≤ 0 игнорируется: ноль соединений
// означал бы пул, который никогда не отдаёт соединение, то есть вечное
// ожидание вместо честной ошибки конфигурации.
func WithMaxConns(n int32) Option {
	return func(c *pgxpool.Config) {
		if n > 0 {
			c.MaxConns = n
		}
	}
}

// NewPool поднимает пул соединений и проверяет его живость.
func NewPool(ctx context.Context, dsn string, opts ...Option) (*pgxpool.Pool, error) {
	if dsn == "" {
		return nil, errors.New("postgres: пустой DSN")
	}

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres: разбор DSN: %w", err)
	}

	// Параметры из DSN имеют приоритет над дефолтом, а Option — над DSN:
	// DSN описывает, куда подключаться, конфиг сервиса — сколько соединений
	// нужно этой роли процесса.
	if cfg.MaxConns == 0 {
		cfg.MaxConns = defaultMaxConns
	}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.MinConns == 0 && cfg.MaxConns >= 2 {
		// Пара тёплых соединений: рефрешер конфига ходит раз в 2 с, и открывать
		// соединение заново на каждый тик — лишняя задержка на пустом месте.
		cfg.MinConns = 2
	}

	cfg.ConnConfig.Tracer = otelpgx.NewTracer()

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("postgres: создание пула: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: проверка соединения: %w", err)
	}

	return pool, nil
}

// isUniqueViolation сообщает, что ошибка — нарушение уникального индекса с
// именем constraint.
func isUniqueViolation(err error, constraint string) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "23505" && pgErr.ConstraintName == constraint
}
