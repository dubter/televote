package redis

import (
	"strconv"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"

	"github.com/dubter/televote/internal/domain"
)

const (
	dedupPrefix   = "v:"
	counterPrefix = "c:"
	ballotsField  = "b"
)

func hashTag(pollID uuid.UUID, shard uint16) string {
	return "p:" + pollID.String() + ":s" + strconv.FormatUint(uint64(shard), 10)
}

func dedupKey(pollID uuid.UUID, shard uint16, v domain.VoterID) string {
	return dedupPrefix + "{" + hashTag(pollID, shard) + "}:" + v.Hex()
}

func counterKey(pollID uuid.UUID, shard uint16) string {
	return counterPrefix + "{" + hashTag(pollID, shard) + "}"
}

func shardFor(v domain.VoterID, shardCount uint16) uint16 {
	if shardCount == 0 {
		return 0
	}
	return uint16(xxhash.Sum64(v[:]) % uint64(shardCount))
}
