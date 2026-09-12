package snapshot

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type countingTick struct {
	calls atomic.Int32
	err   error
}

func (c *countingTick) Tick(context.Context) error {
	c.calls.Add(1)
	return c.err
}

func TestTicker_RunsTheUseCaseOnEveryIntervalUntilCancelled(t *testing.T) {
	t.Parallel()

	tick := &countingTick{}
	w, err := NewTicker(tick, 5*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	require.GreaterOrEqual(t, tick.calls.Load(), int32(3), "тикер обязан вызывать use case регулярно")
}

func TestTicker_KeepsGoingWhenACycleFails(t *testing.T) {
	t.Parallel()

	tick := &countingTick{err: errors.New("postgres down")}
	w, err := NewTicker(tick, 5*time.Millisecond, slog.New(slog.NewTextHandler(io.Discard, nil)))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	require.GreaterOrEqual(t, tick.calls.Load(), int32(2), "одна ошибка цикла не должна останавливать снапшоты")
}

func TestNewTicker_RejectsBrokenDependencies(t *testing.T) {
	t.Parallel()

	_, err := NewTicker(nil, time.Second, slog.Default())
	require.Error(t, err)
	_, err = NewTicker(&countingTick{}, 0, slog.Default())
	require.Error(t, err)
	_, err = NewTicker(&countingTick{}, time.Second, nil)
	require.Error(t, err)
}
