package app

import (
	"context"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

func SelfHealthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	probe := url.URL{Scheme: "http", Host: addr, Path: "/readyz"}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probe.String(), http.NoBody)
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return 1
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("healthcheck: closing response body", slog.Any("error", err))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
