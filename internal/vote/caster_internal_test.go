package vote

// Тесты внутренностей Caster: вывод ключей и TTL проверяются без Redis.
//
// Это сознательно юниты, а не integration. scripts/invariants.tsv ломает
// caster.go и ждёт красного от `go test` БЕЗ тега integration: тест инварианта,
// доступный только с Docker, инвариант не защищает.

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testTTL = 30 * time.Minute

var internalPollID = uuid.MustParse("6f1c9f2a-3b4d-4e5f-8a9b-0c1d2e3f4a5b")

func testVoters(tb testing.TB, n int) []VoterID {
	tb.Helper()

	salt := []byte("salt-for-caster-internal-tests!!")
	out := make([]VoterID, 0, n)
	for i := 0; i < n; i++ {
		v, err := DeriveVoterID(salt, fmt.Sprintf("voter-%d", i))
		require.NoError(tb, err)
		out = append(out, v)
	}
	return out
}

func testCaster(tb testing.TB, ttl time.Duration, jitter float64) *Caster {
	tb.Helper()

	// Клиент не нужен: keysFor и ttlSecondsFor до сети не доходят, а NewCaster
	// справедливо отвергает nil-клиент.
	return &Caster{ttl: ttl, jitter: jitter}
}

// Инвариант из CLAUDE.md: shard_count берётся из строки опроса. Если бы Caster
// брал его из глобального конфига, смена значения в эфире перевела бы уже
// проголосовавших на другие шарды — их дедуп-ключи стали бы недостижимы, и
// повторное голосование открылось бы молча, без единой ошибки в логе.
func TestVote_ShardCountFromPoll(t *testing.T) {
	t.Parallel()

	c := testCaster(t, testTTL, 0.1)
	voters := testVoters(t, 200)

	cases := []struct {
		name       string
		shardCount uint16
	}{
		{"один шард", 1},
		{"минимум из ShardCountFor", 500},
		{"кластер из двух мастеров", 1024},
		{"потолок", 16384},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, v := range voters {
				dedup, counter := c.keysFor(internalPollID, tc.shardCount, v)

				shard := ShardFor(v, tc.shardCount)
				require.Less(t, shard, tc.shardCount)
				assert.Equal(t, DedupKey(internalPollID, shard, v), dedup)
				assert.Equal(t, CounterKey(internalPollID, shard), counter)
			}
		})
	}

	// Разное число шардов обязано давать разные ключи хотя бы части
	// голосующих: одинаковый результат означал бы, что аргумент проигнорирован.
	moved := 0
	for _, v := range voters {
		a, _ := c.keysFor(internalPollID, 500, v)
		b, _ := c.keysFor(internalPollID, 1024, v)
		if a != b {
			moved++
		}
	}
	assert.Greater(t, moved, len(voters)/2,
		"смена shardCount не сдвинула ключи — значение взято не из аргумента")
}

// Инвариант из CLAUDE.md: TTL дедупа с джиттером ±10 %. Без него 30 млн ключей
// с одинаковым сроком истекут одновременно, и active expiry Redis добьёт
// кластер ровно на хвосте дренажа — там, где ещё считаются последние голоса.
func TestVote_TTLHasJitter(t *testing.T) {
	t.Parallel()

	const jitter = 0.1

	c := testCaster(t, testTTL, jitter)
	voters := testVoters(t, 10_000)

	base := testTTL.Seconds()
	lo := int64(base * (1 - jitter))
	hi := int64(base*(1+jitter)) + 1

	distinct := make(map[int64]struct{}, len(voters))
	minSeen, maxSeen := int64(1<<62), int64(0)
	for _, v := range voters {
		got := c.ttlSecondsFor(v)

		require.GreaterOrEqual(t, got, lo, "TTL вышел за нижнюю границу джиттера")
		require.LessOrEqual(t, got, hi, "TTL вышел за верхнюю границу джиттера")
		distinct[got] = struct{}{}
		minSeen = min(minSeen, got)
		maxSeen = max(maxSeen, got)
	}

	// Разброс, а не пара значений вокруг номинала: истечение обязано
	// размазаться по всему окну ±10 %.
	assert.Greater(t, len(distinct), 100, "TTL принимает слишком мало значений")
	assert.Less(t, minSeen, int64(base*(1-jitter/2)), "нижняя половина окна не задействована")
	assert.Greater(t, maxSeen, int64(base*(1+jitter/2)), "верхняя половина окна не задействована")

	// Нулевой джиттер отключает разброс — тогда конфиг честно говорит, что
	// лавина истечения разрешена, и это видно в тесте, а не только в проде.
	fixed := testCaster(t, testTTL, 0)
	for _, v := range voters[:100] {
		assert.Equal(t, int64(base), fixed.ttlSecondsFor(v))
	}
}

// TTL меньше секунды Redis принимает только как EX 0, что немедленно удалило
// бы ключ и открыло повторное голосование. Насыщаем до одной секунды.
func TestCaster_TTLNeverRoundsToZero(t *testing.T) {
	t.Parallel()

	c := testCaster(t, 900*time.Millisecond, 0.5)
	for _, v := range testVoters(t, 100) {
		assert.GreaterOrEqual(t, c.ttlSecondsFor(v), int64(1))
	}
}

// Джиттер выводится из voterID, поэтому повторная доставка того же сообщения
// считает тот же TTL. Это не косметика: SET NX не продлевает ключ, и
// расхождение TTL между попытками означало бы разное окно дедупа.
func TestCaster_TTLIsStableForSameVoter(t *testing.T) {
	t.Parallel()

	c := testCaster(t, testTTL, 0.1)
	for _, v := range testVoters(t, 50) {
		want := c.ttlSecondsFor(v)
		for i := 0; i < 5; i++ {
			require.Equal(t, want, c.ttlSecondsFor(v))
		}
	}
}
