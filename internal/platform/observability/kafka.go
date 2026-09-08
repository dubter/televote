package observability

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

func KafkaHooks(group string) []kgo.Hook {
	tracer := kotel.NewTracer(
		kotel.TracerProvider(otel.GetTracerProvider()),
		kotel.TracerPropagator(otel.GetTextMapPropagator()),
		kotel.ConsumerGroup(group),
	)
	return kotel.NewKotel(kotel.WithTracer(tracer)).Hooks()
}
