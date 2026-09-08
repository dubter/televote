package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/dubter/televote/internal/app"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "проверить /readyz локального процесса и выйти")
	flag.Parse()

	if *healthcheck {
		os.Exit(app.SelfHealthcheck())
	}

	if err := app.Run(context.Background(), app.RoleAPI); err != nil {
		slog.Error("сервис остановлен с ошибкой", slog.Any("error", err))
		os.Exit(1)
	}
}
