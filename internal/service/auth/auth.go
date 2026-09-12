package auth

//go:generate mockgen -source=auth.go -destination=mocks/auth.go -package=mocks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/dubter/televote/internal/domain"
)

var (
	ErrInvalidToken       = errors.New("invalid_token")
	ErrWeakKey            = errors.New("weak_signing_key")
	ErrInvalidCredentials = errors.New("invalid_credentials")
)

const minKeyLen = 32

func HashPassword(plain string) (string, error) {
	hash, err := argon2id.CreateHash(plain, argon2id.DefaultParams)
	if err != nil {
		return "", fmt.Errorf("auth: hash password: %w", err)
	}
	return hash, nil
}

func VerifyPassword(hash, plain string) (bool, error) {
	ok, err := argon2id.ComparePasswordAndHash(plain, hash)
	if err != nil {
		return false, fmt.Errorf("auth: verify password: %w", err)
	}
	return ok, nil
}

type Role string

const (
	RoleViewer Role = "viewer"
	RoleEditor Role = "editor"
	RoleAdmin  Role = "admin"
)

var roleRank = map[Role]int{RoleViewer: 1, RoleEditor: 2, RoleAdmin: 3}

func (r Role) Valid() bool { return roleRank[r] > 0 }

func (r Role) AtLeast(required Role) bool {
	have, ok := roleRank[r]
	if !ok {
		return false
	}
	need, ok := roleRank[required]
	if !ok {
		return false
	}
	return have >= need
}

type Claims struct {
	Role Role `json:"role"`
	jwt.RegisteredClaims
}

func (c *Claims) UserID() (uuid.UUID, error) {
	id, err := uuid.Parse(c.Subject)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: subject is not a UUID", ErrInvalidToken)
	}
	return id, nil
}

type TokenService struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

func NewTokenService(key []byte, ttl time.Duration) (*TokenService, error) {
	if len(key) < minKeyLen {
		return nil, fmt.Errorf("%w: length %d, minimum %d", ErrWeakKey, len(key), minKeyLen)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("auth: ttl must be positive, got %s", ttl)
	}
	return &TokenService{key: key, ttl: ttl, now: time.Now}, nil
}

func (t *TokenService) Issue(userID uuid.UUID, role Role) (string, error) {
	if !role.Valid() {
		return "", fmt.Errorf("auth: unknown role %q", role)
	}

	now := t.now()
	claims := Claims{
		Role: role,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(t.ttl)),
		},
	}

	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(t.key)
	if err != nil {
		return "", fmt.Errorf("auth: sign token: %w", err)
	}
	return signed, nil
}

func (t *TokenService) Parse(raw string) (*Claims, error) {
	var claims Claims

	_, err := jwt.ParseWithClaims(raw, &claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: signing method %q", ErrInvalidToken, token.Method.Alg())
		}
		return t.key, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	if !claims.Role.Valid() {
		return nil, fmt.Errorf("%w: unknown role %q", ErrInvalidToken, claims.Role)
	}
	if _, err := claims.UserID(); err != nil {
		return nil, fmt.Errorf("auth: parse subject: %w", err)
	}
	return &claims, nil
}

type Admins interface {
	ByLogin(ctx context.Context, login string) (*domain.Admin, error)
}

type Session struct {
	Token string
	Role  Role
}

type Service struct {
	admins Admins
	tokens *TokenService
}

func NewService(admins Admins, tokens *TokenService) (*Service, error) {
	switch {
	case admins == nil:
		return nil, errors.New("auth: admin store is required")
	case tokens == nil:
		return nil, errors.New("auth: token service is required")
	}
	return &Service{admins: admins, tokens: tokens}, nil
}

func (s *Service) Login(ctx context.Context, login, password string) (Session, error) {
	admin, err := s.admins.ByLogin(ctx, login)
	if err != nil {
		return Session{}, fmt.Errorf("%w: %w", ErrInvalidCredentials, err)
	}
	ok, err := VerifyPassword(admin.PasswordHash, password)
	if err != nil || !ok {
		return Session{}, ErrInvalidCredentials
	}

	role := Role(admin.Role)
	token, err := s.tokens.Issue(admin.ID, role)
	if err != nil {
		return Session{}, err
	}
	return Session{Token: token, Role: role}, nil
}

func (s *Service) Parse(raw string) (*Claims, error) {
	return s.tokens.Parse(raw)
}
