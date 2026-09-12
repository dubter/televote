package auth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/service/auth"
	"github.com/dubter/televote/internal/service/auth/mocks"
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

func newAuthService(t *testing.T) (*auth.Service, *mocks.MockAdmins, *auth.TokenService) {
	t.Helper()

	tokens, err := auth.NewTokenService(testKey, time.Hour)
	require.NoError(t, err)

	admins := mocks.NewMockAdmins(gomock.NewController(t))
	svc, err := auth.NewService(admins, tokens)
	require.NoError(t, err)
	return svc, admins, tokens
}

func TestLogin_IssuesATokenForValidCredentials(t *testing.T) {
	t.Parallel()

	svc, admins, tokens := newAuthService(t)

	hash, err := auth.HashPassword("secret")
	require.NoError(t, err)
	id := uuid.New()
	admins.EXPECT().ByLogin(gomock.Any(), "admin").
		Return(&domain.Admin{ID: id, Login: "admin", PasswordHash: hash, Role: string(auth.RoleAdmin)}, nil)

	session, err := svc.Login(context.Background(), "admin", "secret")
	require.NoError(t, err)
	assert.Equal(t, auth.RoleAdmin, session.Role)

	claims, err := tokens.Parse(session.Token)
	require.NoError(t, err)
	assert.Equal(t, auth.RoleAdmin, claims.Role)
	assert.Equal(t, id.String(), claims.Subject)
}

func TestLogin_WrongPasswordAndUnknownLoginAreIndistinguishable(t *testing.T) {
	t.Parallel()

	svc, admins, _ := newAuthService(t)

	hash, err := auth.HashPassword("secret")
	require.NoError(t, err)
	admins.EXPECT().ByLogin(gomock.Any(), "admin").
		Return(&domain.Admin{ID: uuid.New(), Login: "admin", PasswordHash: hash, Role: string(auth.RoleAdmin)}, nil)
	admins.EXPECT().ByLogin(gomock.Any(), "нет").Return(nil, domain.ErrNotFound)

	_, wrongPass := svc.Login(context.Background(), "admin", "нет")
	_, wrongUser := svc.Login(context.Background(), "нет", "secret")

	assert.ErrorIs(t, wrongPass, auth.ErrInvalidCredentials)
	assert.ErrorIs(t, wrongUser, auth.ErrInvalidCredentials, "ответ не имеет права выдавать, существует ли логин")
}
