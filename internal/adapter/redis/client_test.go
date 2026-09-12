package redis

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOpen_RequiresAddresses(t *testing.T) {
	t.Parallel()

	_, err := Open(context.Background(), Config{})
	require.ErrorIs(t, err, ErrNoAddresses)

	_, err = Open(context.Background(), Config{Addrs: []string{" "}})
	require.ErrorIs(t, err, ErrNoAddresses)
}
