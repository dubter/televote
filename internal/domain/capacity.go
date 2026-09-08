package domain

import "time"

// Capacity — сколько ресурсов нужно под эфир.
type Capacity struct {
	VoteAPI         int `json:"vote_api"`
	Consumers       int `json:"consumers"`
	RedisMasters    int `json:"redis_masters"`
	KafkaPartitions int `json:"kafka_partitions"`
}

// Эмпирические потолки одного экземпляра. Взяты из расчёта в docs/design.md §3
// и проверены нагрузочным тестом на стенде.
const (
	// votesPerAPIPod — приём не ходит в хранилища: узкое место в syscalls.
	votesPerAPIPod = 70_000
	// votesPerSecPerMaster — Lua исполняется однопоточно, поэтому вертикальное
	// масштабирование Redis не работает вовсе, только добавление мастеров.
	votesPerSecPerMaster = 80_000
	// votesPerSecPerConsumer — консьюмер упирается в тот же Redis.
	votesPerSecPerConsumer = 30_000
	// peakShare — доля голосов, приходящая в пиковые 15 секунд окна.
	peakShare       = 0.5
	peakWindowSecs  = 15
	minCapacityUnit = 1
	// safetyMargin — запас поверх расчёта. Нужен из-за перекоса нагрузки между
	// мастерами (±5 % при 500 шардах на мастера) и из-за того, что во время
	// failover оставшиеся ноды тянут долю упавшей. Ёмкость арендуется на
	// двадцать минут вокруг эфира, поэтому запас почти ничего не стоит.
	safetyMargin = 2.0
)

// Базовая линия между эфирами: админка должна отвечать, даже когда голосования
// нет, а Redis и консьюмеры в это время не нужны вовсе.
const (
	baselineAPI       = 2
	baselineConsumers = 0
	baselineMasters   = 0
)

// CapacityFor выводит ёмкость из ожидаемого числа голосов и окна дренажа.
//
// Приём считается по пику: он обязан принять всплеск в реальном времени.
// Подсчёт — по дренажу: у него есть окно целиком, и чем оно длиннее, тем
// меньше нужно мастеров Redis. Это главный рычаг «стоимость против задержки
// результата».
func CapacityFor(expectedVotes int64, drainWindow time.Duration) Capacity {
	if expectedVotes <= 0 {
		return Capacity{
			VoteAPI: baselineAPI, Consumers: baselineConsumers,
			RedisMasters: baselineMasters, KafkaPartitions: minCapacityUnit,
		}
	}
	if drainWindow <= 0 {
		drainWindow = 5 * time.Minute
	}

	peakRPS := float64(expectedVotes) * peakShare / peakWindowSecs
	drainRPS := float64(expectedVotes) / drainWindow.Seconds()

	// Ёмкость никогда не опускается ниже базовой линии: даже крошечный опрос
	// не должен оставлять приём в одном экземпляре — выкатка или падение пода
	// сделали бы его недоступным целиком.
	return Capacity{
		VoteAPI:      max(baselineAPI, ceilUnits(peakRPS*safetyMargin, votesPerAPIPod)),
		Consumers:    ceilUnits(drainRPS*safetyMargin, votesPerSecPerConsumer),
		RedisMasters: ceilUnits(drainRPS*safetyMargin, votesPerSecPerMaster),
		// Партиций столько же, сколько консьюмеров: меньше — часть консьюмеров
		// простаивает, потому что партиция обрабатывается одним членом группы.
		KafkaPartitions: ceilUnits(drainRPS*safetyMargin, votesPerSecPerConsumer),
	}
}

func ceilUnits(load float64, perUnit int) int {
	n := int(load/float64(perUnit)) + 1
	if n < minCapacityUnit {
		return minCapacityUnit
	}
	return n
}
