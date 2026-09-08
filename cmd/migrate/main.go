// Команда migrate применяет схему Postgres.
//
// Отдельный бинарь, а не флаг сервиса: миграции обязаны выполниться один раз
// до старта инстансов, а два инстанса, стартующих одновременно, гонялись бы
// за одну и ту же блокировку.
package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // драйвер database/sql для pgx
	"github.com/pressly/goose/v3"

	"github.com/OWNER/televote/migrations"
)

func main() {
	command := flag.String("command", "up", "команда goose: up | down | status | version")
	timeout := flag.Duration("timeout", time.Minute, "предел ожидания Postgres")
	flag.Parse()

	if err := run(*command, *timeout); err != nil {
		slog.Error("миграции не применены", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(command string, timeout time.Duration) error {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN не задан")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("подключение: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Postgres в compose может быть ещё не готов принимать соединения даже
	// после healthcheck: ждём с коротким шагом, а не падаем на первой попытке.
	if err := waitReady(ctx, db); err != nil {
		return err
	}

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("диалект: %w", err)
	}

	if err := goose.RunContext(ctx, command, db, "."); err != nil {
		return fmt.Errorf("goose %s: %w", command, err)
	}
	slog.Info("миграции применены", slog.String("command", command))
	return nil
}

func waitReady(ctx context.Context, db *sql.DB) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		if err := db.PingContext(ctx); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres не ответил за отведённое время: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
