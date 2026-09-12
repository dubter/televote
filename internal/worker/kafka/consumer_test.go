package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/worker/kafka/mocks"
)

func newTestConsumer(t *testing.T) (*Consumer, *mocks.MockCounting, *mocks.MockObserver) {
	t.Helper()

	ctrl := gomock.NewController(t)
	counting := mocks.NewMockCounting(ctrl)
	obs := mocks.NewMockObserver(ctrl)

	c := &Consumer{counting: counting, obs: obs, log: slog.New(slog.NewTextHandler(io.Discard, nil)), workers: 4}
	return c, counting, obs
}

func encoded(t *testing.T, msg domain.VoteMessage) []byte {
	t.Helper()

	payload, err := json.Marshal(msg)
	require.NoError(t, err)
	return payload
}

func TestHandle_DecodesTheMessageAndCountsIt(t *testing.T) {
	t.Parallel()

	c, counting, _ := newTestConsumer(t)
	msg := domain.VoteMessage{
		PollID: uuid.New(), VoterID: "0123456789abcdef0123456789abcdef",
		Choices: []uint8{1}, ProducedAt: time.Date(2026, 9, 8, 20, 47, 45, 0, time.UTC),
	}
	counting.EXPECT().Count(gomock.Any(), msg).Return(domain.VoteCounted, nil)

	c.handle(context.Background(), &kgo.Record{Value: encoded(t, msg)})
}

func TestHandle_MalformedPayloadNeverReachesTheUseCase(t *testing.T) {
	t.Parallel()

	c, counting, obs := newTestConsumer(t)
	counting.EXPECT().Count(gomock.Any(), gomock.Any()).Times(0)
	obs.EXPECT().VoteRejected("malformed").Times(1)

	c.handle(context.Background(), &kgo.Record{Value: []byte("не json")})
}

func TestHandle_RejectionIsLoggedNotPanicked(t *testing.T) {
	t.Parallel()

	c, counting, _ := newTestConsumer(t)
	msg := domain.VoteMessage{PollID: uuid.New(), VoterID: "x", Choices: []uint8{0}}
	counting.EXPECT().Count(gomock.Any(), msg).Return(domain.VoteResult(0), errors.New("out of window"))

	c.handle(context.Background(), &kgo.Record{Value: encoded(t, msg), Partition: 3, Offset: 42})
}

func TestHandle_PropagatesTraceContextFromTheRecord(t *testing.T) {
	t.Parallel()

	c, counting, _ := newTestConsumer(t)
	msg := domain.VoteMessage{PollID: uuid.New(), VoterID: "x", Choices: []uint8{0}}

	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:  trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8},
		Remote:  true,
	})
	counting.EXPECT().Count(gomock.Any(), msg).DoAndReturn(
		func(ctx context.Context, _ domain.VoteMessage) (domain.VoteResult, error) {
			require.Equal(t, parent.TraceID(), trace.SpanContextFromContext(ctx).TraceID(),
				"трейс из заголовков Kafka обязан продолжиться в use case")
			return domain.VoteCounted, nil
		})

	rec := &kgo.Record{Value: encoded(t, msg), Context: trace.ContextWithRemoteSpanContext(context.Background(), parent)}
	c.handle(context.Background(), rec)
}
