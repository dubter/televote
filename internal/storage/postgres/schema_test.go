//go:build integration

package postgres_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/migrations"
)

// applySchema накатывает те же миграции, что и продовый мигратор.
//
// Не «CREATE TABLE в тесте»: тогда тест проверял бы схему, которой нет в
// проде, и расхождение всплыло бы только на стенде.
func applySchema(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	entries, err := migrations.FS.ReadDir(".")
	require.NoError(t, err)

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		body, readErr := migrations.FS.ReadFile(e.Name())
		require.NoError(t, readErr)

		sql := upSection(string(body))
		require.NotEmpty(t, sql, "в миграции %s нет секции Up", e.Name())

		_, execErr := pool.Exec(ctx, sql)
		require.NoError(t, execErr, "миграция %s", e.Name())
	}
}

// upSection вырезает часть между «+goose Up» и «+goose Down».
func upSection(body string) string {
	const (
		up   = "-- +goose Up"
		down = "-- +goose Down"
	)

	i := indexAfter(body, up)
	if i < 0 {
		return ""
	}
	rest := body[i:]

	if j := indexOf(rest, down); j >= 0 {
		rest = rest[:j]
	}
	return stripStatementMarkers(rest)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}

func indexAfter(s, sub string) int {
	if i := indexOf(s, sub); i >= 0 {
		return i + len(sub)
	}
	return -1
}

// stripStatementMarkers убирает служебные строки goose: в одном Exec они не нужны.
func stripStatementMarkers(s string) string {
	var out []byte
	for line := range splitLines(s) {
		if len(line) >= 2 && line[0] == '-' && line[1] == '-' &&
			(indexOf(line, "+goose") >= 0) {
			continue
		}
		out = append(out, line...)
		out = append(out, '\n')
	}
	return string(out)
}

func splitLines(s string) func(func(string) bool) {
	return func(yield func(string) bool) {
		start := 0
		for i := range len(s) {
			if s[i] == '\n' {
				if !yield(s[start:i]) {
					return
				}
				start = i + 1
			}
		}
		if start < len(s) {
			yield(s[start:])
		}
	}
}
