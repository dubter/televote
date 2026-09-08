package domain

import "time"

type Capacity struct {
	VoteAPI         int `json:"vote_api"`
	Consumers       int `json:"consumers"`
	RedisMasters    int `json:"redis_masters"`
	KafkaPartitions int `json:"kafka_partitions"`
}

const (
	votesPerAPIPod         = 70_000
	votesPerSecPerMaster   = 80_000
	votesPerSecPerConsumer = 30_000
	peakShare              = 0.5
	peakWindowSecs         = 15
	minCapacityUnit        = 1
	safetyMargin           = 2.0
)

const (
	baselineAPI       = 2
	baselineConsumers = 0
	baselineMasters   = 0
)

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

	return Capacity{
		VoteAPI:         max(baselineAPI, ceilUnits(peakRPS*safetyMargin, votesPerAPIPod)),
		Consumers:       ceilUnits(drainRPS*safetyMargin, votesPerSecPerConsumer),
		RedisMasters:    ceilUnits(drainRPS*safetyMargin, votesPerSecPerMaster),
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
