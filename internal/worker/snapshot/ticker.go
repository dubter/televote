package snapshot

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type Cycle interface {
	Tick(ctx context.Context) error
}

type Ticker struct {
	cycle    Cycle
	interval time.Duration
	log      *slog.Logger
}

func NewTicker(cycle Cycle, interval time.Duration, log *slog.Logger) (*Ticker, error) {
	switch {
	case cycle == nil:
		return nil, errors.New("snapshot worker: cycle is required")
	case interval <= 0:
		return nil, errors.New("snapshot worker: interval must be positive")
	case log == nil:
		return nil, errors.New("snapshot worker: logger is required")
	}
	return &Ticker{cycle: cycle, interval: interval, log: log}, nil
}

func (t *Ticker) Run(ctx context.Context) {
	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := t.cycle.Tick(ctx); err != nil {
				t.log.WarnContext(ctx, "snapshot: cycle finished with an error", slog.String("error", err.Error()))
			}
		}
	}
}
