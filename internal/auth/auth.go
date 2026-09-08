package auth

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/alexedwards/argon2id"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

var (
	ErrInvalidToken    = errors.New("invalid_token")
	ErrWeakKey         = errors.New("weak_signing_key")
	ErrTooManyAttempts = errors.New("too_many_attempts")
)

const minKeyLen = 32

func HashPassword(plain string) (string, error) {
	hash, err := argon2id.CreateHash(plain, argon2id.DefaultParams)
	if err != nil {
		return "", fmt.Errorf("auth: хэширование пароля: %w", err)
	}
	return hash, nil
}

func VerifyPassword(hash, plain string) (bool, error) {
	ok, err := argon2id.ComparePasswordAndHash(plain, hash)
	if err != nil {
		return false, fmt.Errorf("auth: проверка пароля: %w", err)
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
		return uuid.Nil, fmt.Errorf("%w: subject не UUID", ErrInvalidToken)
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
		return nil, fmt.Errorf("%w: длина %d, минимум %d", ErrWeakKey, len(key), minKeyLen)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("auth: ttl должен быть положительным, получено %s", ttl)
	}
	return &TokenService{key: key, ttl: ttl, now: time.Now}, nil
}

func (t *TokenService) Issue(userID uuid.UUID, role Role) (string, error) {
	if !role.Valid() {
		return "", fmt.Errorf("auth: неизвестная роль %q", role)
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
		return "", fmt.Errorf("auth: подпись токена: %w", err)
	}
	return signed, nil
}

func (t *TokenService) Parse(raw string) (*Claims, error) {
	var claims Claims

	_, err := jwt.ParseWithClaims(raw, &claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: метод подписи %q", ErrInvalidToken, token.Method.Alg())
		}
		return t.key, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidToken, err)
	}
	if !claims.Role.Valid() {
		return nil, fmt.Errorf("%w: неизвестная роль %q", ErrInvalidToken, claims.Role)
	}
	if _, err := claims.UserID(); err != nil {
		return nil, err
	}
	return &claims, nil
}

type LoginLimiter struct {
	mu       sync.Mutex
	attempts map[string]*attempt
	limit    int
	window   time.Duration
	maxKeys  int
	now      func() time.Time
}

type attempt struct {
	count int
	until time.Time
}

func NewLoginLimiter(limit int, window time.Duration, maxKeys int) *LoginLimiter {
	if limit <= 0 {
		limit = 5
	}
	if window <= 0 {
		window = time.Minute
	}
	if maxKeys <= 0 {
		maxKeys = 10_000
	}
	return &LoginLimiter{
		attempts: make(map[string]*attempt),
		limit:    limit,
		window:   window,
		maxKeys:  maxKeys,
		now:      time.Now,
	}
}

func (l *LoginLimiter) Allow(login string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	a, ok := l.attempts[login]
	if !ok || now.After(a.until) {
		if len(l.attempts) >= l.maxKeys {
			l.evictExpiredLocked(now)
		}
		l.attempts[login] = &attempt{count: 1, until: now.Add(l.window)}
		return true
	}

	a.count++
	return a.count <= l.limit
}

func (l *LoginLimiter) Reset(login string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.attempts, login)
}

func (l *LoginLimiter) evictExpiredLocked(now time.Time) {
	for login, a := range l.attempts {
		if now.After(a.until) {
			delete(l.attempts, login)
		}
	}
	if len(l.attempts) >= l.maxKeys {
		clear(l.attempts)
	}
}
