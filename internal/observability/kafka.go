package observability

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel"
)

// Хуки переносят trace-контекст в заголовках записи: приём в API и подсчёт в
// консьюмере разнесены на минуты, и без переноса это два несвязанных трейса.
func KafkaHooks(group string) []kgo.Hook {
	tracer := kotel.NewTracer(
		kotel.TracerProvider(otel.GetTracerProvider()),
		kotel.TracerPropagator(otel.GetTextMapPropagator()),
		kotel.ConsumerGroup(group),
	)
	return kotel.NewKotel(kotel.WithTracer(tracer)).Hooks()
}
