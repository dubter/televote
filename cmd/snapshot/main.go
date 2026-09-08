package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/dubter/televote/internal/platform/app"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "check /readyz of the local process and exit")
	flag.Parse()

	if *healthcheck {
		os.Exit(app.SelfHealthcheck())
	}

	if err := app.Run(context.Background(), app.RoleSnapshot); err != nil {
		slog.Error("service stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}
