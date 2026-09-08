package vote

import (
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

// Префиксы разные: по одному адресу лежат строка-маркер и хэш счётчиков.
// Наложение дало бы WRONGTYPE.
const (
	dedupPrefix   = "v:"
	counterPrefix = "c:"
)

// HashTag — общая часть ключей одного шарда, без фигурных скобок.
func HashTag(pollID uuid.UUID, shard uint16) string {
	var b strings.Builder
	b.Grow(2 + 36 + 2 + 5)
	b.WriteString("p:")
	b.WriteString(pollID.String())
	b.WriteString(":s")
	b.WriteString(strconv.FormatUint(uint64(shard), 10))
	return b.String()
}

// DedupKey — маркер «этот голосующий уже учтён». Значение не содержит выбор:
// ключ доказывает факт голосования, но не его содержание.
func DedupKey(pollID uuid.UUID, shard uint16, v VoterID) string {
	var b strings.Builder
	b.Grow(len(dedupPrefix) + 2 + 45 + 1 + 32)
	b.WriteString(dedupPrefix)
	b.WriteByte('{')
	b.WriteString(HashTag(pollID, shard))
	b.WriteString("}:")
	b.WriteString(v.Hex())
	return b.String()
}

// CounterKey — хэш счётчиков шарда: поле на опцию плюс "b" с числом бюллетеней.
func CounterKey(pollID uuid.UUID, shard uint16) string {
	var b strings.Builder
	b.Grow(len(counterPrefix) + 2 + 45)
	b.WriteString(counterPrefix)
	b.WriteByte('{')
	b.WriteString(HashTag(pollID, shard))
	b.WriteByte('}')
	return b.String()
}

// ShardFor выбирает шард по идентификатору голосующего.
func ShardFor(v VoterID, shardCount uint16) uint16 {
	if shardCount == 0 {
		return 0
	}
	return uint16(xxhash.Sum64(v[:]) % uint64(shardCount))
}
