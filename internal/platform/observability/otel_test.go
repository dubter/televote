package observability_test

import (
	"context"
	"log/slog"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/platform/observability"
)

func TestSetup_WithEndpointWiresExportersAndShutsDownCleanly(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	tel, err := observability.Setup(context.Background(), observability.Config{
		Endpoint:    "http://127.0.0.1:1",
		ServiceName: "televote",
	}, slog.NewJSONHandler(os.Stdout, nil))
	require.NoError(t, err)
	require.NotNil(t, tel.Logs)
	require.NoError(t, tel.Shutdown(context.Background()))
}

func TestSetup_WithoutEndpointStaysLocal(t *testing.T) {
	t.Parallel()

	tel, err := observability.Setup(context.Background(), observability.Config{ServiceName: "televote"},
		slog.NewJSONHandler(os.Stdout, nil))
	require.NoError(t, err)
	require.NotNil(t, tel.Logs)
	require.NoError(t, tel.Shutdown(context.Background()))
}
