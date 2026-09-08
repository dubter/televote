package vote

import (
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/google/uuid"
)

// Префиксы ключей. Разные, потому что по одному адресу лежат значения разных
// типов: строка-маркер и хэш счётчиков. Наложение дало бы WRONGTYPE и потерю
// голосов вместо внятной ошибки.
const (
	dedupPrefix   = "v:"
	counterPrefix = "c:"
)

// HashTag — общая часть ключей одного шарда, без фигурных скобок.
//
// Redis Cluster считает слот по содержимому первого непустого {...}, поэтому
// общий тег гарантирует, что дедуп-маркер и счётчик окажутся в одном слоте.
// Без этого EVALSHA возвращает CROSSSLOT и голос не будет ни посчитан, ни
// отвергнут — он просто пропадёт.
func HashTag(pollID uuid.UUID, shard uint16) string {
	var b strings.Builder
	b.Grow(2 + 36 + 2 + 5)
	b.WriteString("p:")
	b.WriteString(pollID.String())
	b.WriteString(":s")
	b.WriteString(strconv.FormatUint(uint64(shard), 10))
	return b.String()
}

// DedupKey — маркер «этот голосующий уже учтён в этом опросе».
//
// Значение маркера не содержит выбор: ключ доказывает факт голосования, но не
// его содержание. На этом держится утверждение, что связь «человек → выбор»
// не выводима из дедупа.
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

// CounterKey — хэш со счётчиками одного шарда: поле на опцию плюс поле "b"
// с числом бюллетеней.
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
//
// Ключ шардирования не выбирается свободно: дедуп-маркер по природе привязан к
// voterID, а атомарность требует, чтобы счётчик лежал с ним в одном слоте.
// Равномерность обеспечена тем, что voterID — выход HMAC.
//
// shardCount == 0 приходит только из битого конфига, но деление на ноль на
// горячем пути консьюмера уронило бы процесс и остановило дренаж.
func ShardFor(v VoterID, shardCount uint16) uint16 {
	if shardCount == 0 {
		return 0
	}
	return uint16(xxhash.Sum64(v[:]) % uint64(shardCount))
}
