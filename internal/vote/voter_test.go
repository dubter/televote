package vote_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OWNER/televote/internal/vote"
)

var (
	saltA = []byte("poll-a-salt-0123456789abcdef0123")
	saltB = []byte("poll-b-salt-0123456789abcdef0123")
)

// Идемпотентность дедупа держится на этом: тот же вход даёт тот же ключ, иначе
// повторная доставка одного и того же сообщения из Kafka завысила бы результат.
func TestDeriveVoterID_SameInputSameOutput(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"uuid":    "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e",
		"юникод":  "короткий",
		"длинный": strings.Repeat("x", 64),
	}

	for name, clientID := range cases {
		t.Run(name, func(t *testing.T) {
			first, err := vote.DeriveVoterID(saltA, clientID)
			require.NoError(t, err)

			for i := 0; i < 3; i++ {
				again, err := vote.DeriveVoterID(saltA, clientID)
				require.NoError(t, err)
				assert.Equal(t, first, again)
			}
		})
	}
}

// Приватность: тот же браузер в двух опросах даёт несвязанные voterID, иначе
// по совпадению ключей можно было бы склеить участие человека в разных опросах.
func TestDeriveVoterID_DifferentSaltsUnlinkable(t *testing.T) {
	t.Parallel()

	const clientID = "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"

	a, err := vote.DeriveVoterID(saltA, clientID)
	require.NoError(t, err)
	b, err := vote.DeriveVoterID(saltB, clientID)
	require.NoError(t, err)

	assert.NotEqual(t, a, b)

	// Ни один байт не должен совпадать «структурно»: HMAC разными ключами даёт
	// независимые выходы, и совпадение хотя бы половины означало бы ошибку в
	// использовании соли (например соль не попала в ключ HMAC).
	same := 0
	for i := range a {
		if a[i] == b[i] {
			same++
		}
	}
	assert.Less(t, same, len(a)/2, "выходы подозрительно похожи: %x vs %x", a, b)
}

// clientID генерит клиент, поэтому вход враждебен по определению. Пустая
// строка и «константы» сломанных клиентов (undefined, null, нулевой UUID)
// склеили бы миллионы зрителей в один дедуп-ключ: первый голос прошёл бы,
// остальные вернули already_counted — тихая потеря целой когорты.
func TestDeriveVoterID_RejectsEmptyAndConstant(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		clientID string
	}{
		{"пустая строка", ""},
		{"пробелы", "   "},
		{"js undefined", "undefined"},
		{"js null", "null"},
		{"js NaN", "NaN"},
		{"строковый объект", "[object Object]"},
		{"ноль", "0"},
		{"прочерк", "-"},
		{"нулевой uuid", "00000000-0000-0000-0000-000000000000"},
		{"регистр не спасает", "UNDEFINED"},
		{"слишком длинный", strings.Repeat("a", 4096)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			v, err := vote.DeriveVoterID(saltA, tc.clientID)
			require.ErrorIs(t, err, vote.ErrBadClientID)
			assert.Equal(t, vote.VoterID{}, v, "при ошибке возвращается нулевой voterID")
		})
	}
}

// Длина ключа фиксирована независимо от присланного: 4 КБ мусора в поле voter
// не должны превращаться в 4 КБ ключа в Redis. 30 млн таких ключей — это RAM.
func TestDeriveVoterID_BoundsKeyLength(t *testing.T) {
	t.Parallel()

	cases := []string{
		"a",
		"9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e",
		strings.Repeat("длинный юникод ", 8),
		strings.Repeat("z", 128),
	}

	for _, clientID := range cases {
		v, err := vote.DeriveVoterID(saltA, clientID)
		require.NoError(t, err, "clientID длиной %d байт", len(clientID))

		assert.Len(t, v[:], 16, "voterID обязан быть 16 байт")
		assert.Len(t, v.Hex(), 32)

		key := vote.DedupKey(testPollID, vote.ShardFor(v, 500), v)
		assert.Less(t, len(key), 96, "дедуп-ключ раздулся: %q", key)
	}
}

// Соль берётся из строки опроса. Пустая соль означает битую запись в БД, а не
// «хэшируем без соли»: без соли чужой voterID подбирается по известному uuid.
func TestDeriveVoterID_RejectsEmptySalt(t *testing.T) {
	t.Parallel()

	for _, salt := range [][]byte{nil, {}, []byte("коротко")} {
		_, err := vote.DeriveVoterID(salt, "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e")
		require.Error(t, err, "соль длиной %d принята", len(salt))
		assert.NotErrorIs(t, err, vote.ErrBadClientID,
			"битая соль — вина сервера, а не клиента: 400 отдавать нельзя")
	}
}

// Hex нужен продюсеру (VoteMessage.VoterID) и консьюмеру, который читает его
// обратно. Пара обязана быть обратимой, иначе голос применится не к тому ключу.
func TestVoterID_HexRoundTrip(t *testing.T) {
	t.Parallel()

	v, err := vote.DeriveVoterID(saltA, "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e")
	require.NoError(t, err)

	back, err := vote.ParseVoterID(v.Hex())
	require.NoError(t, err)
	assert.Equal(t, v, back)

	_, err = hex.DecodeString(v.Hex())
	require.NoError(t, err)

	for _, bad := range []string{"", "zz", strings.Repeat("ab", 15), strings.Repeat("ab", 17)} {
		_, err := vote.ParseVoterID(bad)
		assert.Error(t, err, "принят битый hex %q", bad)
	}
}
