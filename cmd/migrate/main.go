package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/dubter/televote/migrations"
)

func main() {
	command := flag.String("command", "up", "goose command: up | down | status | version")
	timeout := flag.Duration("timeout", time.Minute, "postgres wait timeout")
	flag.Parse()

	if err := run(*command, *timeout); err != nil {
		slog.Error("migrations not applied", slog.Any("error", err))
		os.Exit(1)
	}
}

func run(command string, timeout time.Duration) error {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		return fmt.Errorf("POSTGRES_DSN is not set")
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = db.Close() }() //nolint:errcheck // a pool close error changes nothing

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := waitReady(ctx, db); err != nil {
		return err
	}

	goose.SetBaseFS(migrations.FS)
	if err := goose.SetDialect("postgres"); err != nil {
		return fmt.Errorf("dialect: %w", err)
	}

	if err := goose.RunContext(ctx, command, db, "."); err != nil {
		return fmt.Errorf("goose %s: %w", command, err)
	}
	slog.Info("migrations applied", slog.String("command", command))
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
			return fmt.Errorf("postgres did not respond in time: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}
