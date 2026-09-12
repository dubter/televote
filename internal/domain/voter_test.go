package domain_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/domain"
)

var (
	saltA = []byte("poll-a-salt-0123456789abcdef0123")
	saltB = []byte("poll-b-salt-0123456789abcdef0123")
)

const testClientID = "9b2f4c6e-1a3d-4b5c-8d7e-0f1a2b3c4d5e"

func TestDeriveVoterID_SameInputSameOutput(t *testing.T) {
	t.Parallel()

	cases := map[string]string{
		"uuid":    testClientID,
		"юникод":  "короткий",
		"длинный": strings.Repeat("x", 64),
	}

	for name, clientID := range cases {
		t.Run(name, func(t *testing.T) {
			first, err := domain.DeriveVoterID(saltA, clientID)
			require.NoError(t, err)

			for range 3 {
				again, err := domain.DeriveVoterID(saltA, clientID)
				require.NoError(t, err)
				assert.Equal(t, first, again)
			}
		})
	}
}

func TestDeriveVoterID_DifferentSaltsUnlinkable(t *testing.T) {
	t.Parallel()

	a, err := domain.DeriveVoterID(saltA, testClientID)
	require.NoError(t, err)
	b, err := domain.DeriveVoterID(saltB, testClientID)
	require.NoError(t, err)

	assert.NotEqual(t, a, b)

	same := 0
	for i := range a {
		if a[i] == b[i] {
			same++
		}
	}
	assert.Less(t, same, len(a)/2, "выходы подозрительно похожи: %x vs %x", a, b)
}

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

			v, err := domain.DeriveVoterID(saltA, tc.clientID)
			require.ErrorIs(t, err, domain.ErrBadClientID)
			assert.Equal(t, domain.VoterID{}, v, "при ошибке возвращается нулевой voterID")
		})
	}
}

func TestDeriveVoterID_IsAlwaysSixteenBytes(t *testing.T) {
	t.Parallel()

	cases := []string{
		"a",
		testClientID,
		strings.Repeat("длинный юникод ", 8),
		strings.Repeat("z", 128),
	}

	for _, clientID := range cases {
		v, err := domain.DeriveVoterID(saltA, clientID)
		require.NoError(t, err, "clientID длиной %d байт", len(clientID))

		assert.Len(t, v[:], 16, "voterID обязан быть 16 байт")
		assert.Len(t, v.Hex(), 32)
	}
}

func TestDeriveVoterID_RejectsEmptySalt(t *testing.T) {
	t.Parallel()

	for _, salt := range [][]byte{nil, {}, []byte("коротко")} {
		_, err := domain.DeriveVoterID(salt, testClientID)
		require.ErrorIs(t, err, domain.ErrBadSalt, "соль длиной %d принята", len(salt))
		assert.NotErrorIs(t, err, domain.ErrBadClientID,
			"битая соль — вина сервера, а не клиента: 400 отдавать нельзя")
	}
}

func TestVoterID_HexRoundTrip(t *testing.T) {
	t.Parallel()

	v, err := domain.DeriveVoterID(saltA, testClientID)
	require.NoError(t, err)

	back, err := domain.ParseVoterID(v.Hex())
	require.NoError(t, err)
	assert.Equal(t, v, back)

	_, err = hex.DecodeString(v.Hex())
	require.NoError(t, err)

	for _, bad := range []string{"", "zz", strings.Repeat("ab", 15), strings.Repeat("ab", 17)} {
		_, err := domain.ParseVoterID(bad)
		assert.Error(t, err, "принят битый hex %q", bad)
	}
}

func TestVoteResult_ZeroValueIsNotSuccess(t *testing.T) {
	t.Parallel()

	var zero domain.VoteResult
	assert.False(t, zero.Valid(), "нулевой VoteResult не может быть валидным исходом")
	assert.True(t, domain.VoteCounted.Valid())
	assert.True(t, domain.VoteAlreadyCounted.Valid())

	assert.Equal(t, "counted", domain.VoteCounted.String())
	assert.Equal(t, "already_counted", domain.VoteAlreadyCounted.String())
	assert.Equal(t, "invalid", zero.String())
	assert.Equal(t, "invalid", domain.VoteResult(200).String())
}
