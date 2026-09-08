package vote

import (
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

const (
	dedupPrefix   = "v:"
	counterPrefix = "c:"
)

func HashTag(pollID uuid.UUID, shard uint16) string {
	return "p:" + pollID.String() + ":s" + strconv.FormatUint(uint64(shard), 10)
}

func DedupKey(pollID uuid.UUID, shard uint16, v VoterID) string {
	return dedupPrefix + "{" + HashTag(pollID, shard) + "}:" + v.Hex()
}

func CounterKey(pollID uuid.UUID, shard uint16) string {
	return counterPrefix + "{" + HashTag(pollID, shard) + "}"
}

func ShardFor(v VoterID, shardCount uint16) uint16 {
	if shardCount == 0 {
		return 0
	}
	return uint16(xxhash.Sum64(v[:]) % uint64(shardCount))
}
