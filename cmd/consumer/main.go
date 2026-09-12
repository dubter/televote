package main

import (
	"context"
	"flag"
	"log/slog"
	"os"

	"github.com/dubter/televote/internal/platform/app"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false,
		"probe /readyz of the running process and exit: docker healthcheck can only exec, and the image has no shell or curl")
	flag.Parse()

	if *healthcheck {
		os.Exit(app.SelfHealthcheck())
	}

	if err := app.RunConsumer(context.Background()); err != nil {
		slog.Error("service stopped with an error", slog.Any("error", err))
		os.Exit(1)
	}
}
