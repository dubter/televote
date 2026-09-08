package domain_test

import (
	"testing"
	"time"

	"github.com/dubter/televote/internal/domain"
)

// Наглядная таблица «дренаж против ёмкости»: главный рычаг «стоимость против
// задержки результата». Запускать: go test -run Table -v ./internal/domain/
func TestTableOfCapacity(t *testing.T) {
	t.Parallel()

	for _, d := range []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute} {
		c := domain.CapacityFor(30_000_000, d)
		t.Logf("дренаж %-6s → приём %2d · консьюмеров %2d · мастеров Redis %2d · партиций %2d",
			d, c.VoteAPI, c.Consumers, c.RedisMasters, c.KafkaPartitions)
	}
}
