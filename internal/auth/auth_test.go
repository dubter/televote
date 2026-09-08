package auth_test

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/auth"
)

var testKey = []byte("0123456789abcdef0123456789abcdef")

func TestHashVerify_RoundTrip(t *testing.T) {
	t.Parallel()

	hash, err := auth.HashPassword("правильный-пароль")
	require.NoError(t, err)
	assert.NotContains(t, hash, "правильный-пароль", "хэш не имеет права содержать пароль")

	ok, err := auth.VerifyPassword(hash, "правильный-пароль")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestVerify_WrongPassword(t *testing.T) {
	t.Parallel()

	hash, err := auth.HashPassword("правильный-пароль")
	require.NoError(t, err)

	ok, err := auth.VerifyPassword(hash, "неправильный")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestHashPassword_IsSalted(t *testing.T) {
	t.Parallel()

	first, err := auth.HashPassword("одинаковый")
	require.NoError(t, err)
	second, err := auth.HashPassword("одинаковый")
	require.NoError(t, err)

	assert.NotEqual(t, first, second)
}

func TestNewTokenService_RejectsWeakKey(t *testing.T) {
	t.Parallel()

	_, err := auth.NewTokenService([]byte("короткий"), time.Hour)
	require.ErrorIs(t, err, auth.ErrWeakKey)

	_, err = auth.NewTokenService(testKey, 0)
	require.Error(t, err)
}

func TestFR7_AdminTokenRoundTrip(t *testing.T) {
	t.Parallel()

	svc, err := auth.NewTokenService(testKey, time.Hour)
	require.NoError(t, err)

	id := uuid.New()
	raw, err := svc.Issue(id, auth.RoleEditor)
	require.NoError(t, err)

	claims, err := svc.Parse(raw)
	require.NoError(t, err)
	assert.Equal(t, auth.RoleEditor, claims.Role)

	got, err := claims.UserID()
	require.NoError(t, err)
	assert.Equal(t, id, got)
}

func TestFR7_ExpiredJWTRejected(t *testing.T) {
	t.Parallel()

	short, err := auth.NewTokenService(testKey, time.Nanosecond)
	require.NoError(t, err)

	raw, err := short.Issue(uuid.New(), auth.RoleAdmin)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	_, err = short.Parse(raw)
	require.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestParse_RejectsTamperedAndForeignTokens(t *testing.T) {
	t.Parallel()

	svc, err := auth.NewTokenService(testKey, time.Hour)
	require.NoError(t, err)

	raw, err := svc.Issue(uuid.New(), auth.RoleAdmin)
	require.NoError(t, err)

	other, err := auth.NewTokenService([]byte("ffffffffffffffffffffffffffffffff"), time.Hour)
	require.NoError(t, err)
	_, err = other.Parse(raw)
	assert.ErrorIs(t, err, auth.ErrInvalidToken, "токен чужого ключа принят")

	tampered := raw[:len(raw)-3] + "aaa"
	_, err = svc.Parse(tampered)
	assert.ErrorIs(t, err, auth.ErrInvalidToken)

	for _, bad := range []string{"", "не.токен", strings.Repeat("a", 64)} {
		_, err := svc.Parse(bad)
		assert.ErrorIs(t, err, auth.ErrInvalidToken, "принят мусор %q", bad)
	}
}

func TestParse_RejectsAlgNone(t *testing.T) {
	t.Parallel()

	svc, err := auth.NewTokenService(testKey, time.Hour)
	require.NoError(t, err)

	claims := auth.Claims{
		Role: auth.RoleAdmin,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   uuid.New().String(),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	unsigned, err := jwt.NewWithClaims(jwt.SigningMethodNone, claims).
		SignedString(jwt.UnsafeAllowNoneSignatureType)
	require.NoError(t, err)

	_, err = svc.Parse(unsigned)
	assert.ErrorIs(t, err, auth.ErrInvalidToken)
}

func TestRole_AtLeastOrdering(t *testing.T) {
	t.Parallel()

	assert.True(t, auth.RoleAdmin.AtLeast(auth.RoleViewer))
	assert.True(t, auth.RoleAdmin.AtLeast(auth.RoleAdmin))
	assert.True(t, auth.RoleEditor.AtLeast(auth.RoleViewer))

	assert.False(t, auth.RoleViewer.AtLeast(auth.RoleEditor))
	assert.False(t, auth.RoleEditor.AtLeast(auth.RoleAdmin))
	assert.False(t, auth.Role("root").AtLeast(auth.RoleViewer), "неизвестная роль не даёт прав")
	assert.False(t, auth.Role("").Valid())
}

func TestLoginLimiter_BlocksAfterN(t *testing.T) {
	t.Parallel()

	l := auth.NewLoginLimiter(3, time.Minute, 100)

	for i := range 3 {
		assert.True(t, l.Allow("admin"), "попытка %d отвергнута преждевременно", i+1)
	}
	assert.False(t, l.Allow("admin"), "четвёртая попытка обязана быть отвергнута")

	assert.True(t, l.Allow("другой-логин"), "лимит считается по логину, а не глобально")

	l.Reset("admin")
	assert.True(t, l.Allow("admin"), "после удачного входа счётчик обязан сбрасываться")
}

func TestLoginLimiter_TableIsBounded(t *testing.T) {
	t.Parallel()

	const maxKeys = 64
	l := auth.NewLoginLimiter(3, time.Minute, maxKeys)

	for i := range maxKeys * 20 {
		l.Allow(strings.Repeat("x", i%7) + string(rune('a'+i%26)) + string(rune(i)))
	}

	for range 3 {
		assert.True(t, l.Allow("после-переполнения"))
	}
	assert.False(t, l.Allow("после-переполнения"))
}
