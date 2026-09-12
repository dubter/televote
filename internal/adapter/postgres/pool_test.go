package postgres_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/adapter/postgres"
)

func TestOpen_RejectsEmptyDSN(t *testing.T) {
	t.Parallel()

	_, err := postgres.Open(context.Background(), postgres.Config{})
	require.ErrorIs(t, err, postgres.ErrEmptyDSN)
}

func TestNewRepos_RejectNilDB(t *testing.T) {
	t.Parallel()

	_, err := postgres.NewPollRepo(nil)
	require.Error(t, err)
	_, err = postgres.NewResultRepo(nil)
	require.Error(t, err)
	_, err = postgres.NewAdminRepo(nil)
	require.Error(t, err)
}
