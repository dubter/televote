// Package auth отвечает за доступ к админке: пароли, токены и роли.
//
// Голосующий здесь не участвует: голосование анонимно и аутентификации не
// требует. Всё, что в этом пакете, относится к control plane.
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

// Ошибки аутентификации. Наружу все они превращаются в 401 без подробностей:
// «нет такого логина» и «неверный пароль» — это подсказка перебору.
var (
	// ErrInvalidToken — подпись, срок или состав токена не годятся.
	ErrInvalidToken = errors.New("invalid_token")
	// ErrWeakKey — ключ подписи слишком короткий.
	ErrWeakKey = errors.New("weak_signing_key")
	// ErrTooManyAttempts — превышен лимит попыток входа.
	ErrTooManyAttempts = errors.New("too_many_attempts")
)

// minKeyLen — ключ короче этого не даёт HMAC осмысленной стойкости.
const minKeyLen = 32

// HashPassword считает argon2id-хэш.
//
// argon2id намеренно дорог по CPU и памяти — именно это делает перебор
// нерентабельным. Обратная сторона: без лимита попыток эндпоинт логина
// становится усилителем DoS, поэтому LoginLimiter здесь не опция.
func HashPassword(plain string) (string, error) {
	hash, err := argon2id.CreateHash(plain, argon2id.DefaultParams)
	if err != nil {
		return "", fmt.Errorf("auth: хэширование пароля: %w", err)
	}
	return hash, nil
}

// VerifyPassword сверяет пароль с хэшем.
func VerifyPassword(hash, plain string) (bool, error) {
	ok, err := argon2id.ComparePasswordAndHash(plain, hash)
	if err != nil {
		return false, fmt.Errorf("auth: проверка пароля: %w", err)
	}
	return ok, nil
}

// Role — роль администратора.
type Role string

// Роли по возрастанию прав.
const (
	// RoleViewer — только чтение результатов.
	RoleViewer Role = "viewer"
	// RoleEditor — создание и ведение опросов.
	RoleEditor Role = "editor"
	// RoleAdmin — всё, включая управление пользователями и исключение голосов.
	RoleAdmin Role = "admin"
)

// roleRank задаёт порядок явно. Сравнение строк дало бы «admin < editor <
// viewer» по алфавиту, то есть ровно обратный порядок прав.
var roleRank = map[Role]int{RoleViewer: 1, RoleEditor: 2, RoleAdmin: 3}

// Valid сообщает, известна ли роль.
func (r Role) Valid() bool { return roleRank[r] > 0 }

// AtLeast сообщает, покрывает ли роль требуемый уровень.
func (r Role) AtLeast(min Role) bool {
	have, ok := roleRank[r]
	if !ok {
		return false
	}
	need, ok := roleRank[min]
	if !ok {
		return false
	}
	return have >= need
}

// Claims — полезная нагрузка админского токена.
type Claims struct {
	Role Role `json:"role"`
	jwt.RegisteredClaims
}

// UserID возвращает идентификатор администратора.
func (c *Claims) UserID() (uuid.UUID, error) {
	id, err := uuid.Parse(c.Subject)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: subject не UUID", ErrInvalidToken)
	}
	return id, nil
}

// TokenService выдаёт и проверяет админские токены.
//
// Токен живёт в заголовке Authorization, а не в cookie: браузер не отправляет
// его автоматически, и вопрос CSRF в админке не возникает вовсе.
type TokenService struct {
	key []byte
	ttl time.Duration
	now func() time.Time
}

// NewTokenService собирает сервис токенов.
func NewTokenService(key []byte, ttl time.Duration) (*TokenService, error) {
	if len(key) < minKeyLen {
		return nil, fmt.Errorf("%w: длина %d, минимум %d", ErrWeakKey, len(key), minKeyLen)
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("auth: ttl должен быть положительным, получено %s", ttl)
	}
	return &TokenService{key: key, ttl: ttl, now: time.Now}, nil
}

// Issue выдаёт токен администратору.
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

// Parse проверяет токен.
//
// Метод подписи проверяется явно: без этого токен с alg=none принимается как
// валидный, и админом становится кто угодно.
func (t *TokenService) Parse(raw string) (*Claims, error) {
	var claims Claims

	_, err := jwt.ParseWithClaims(raw, &claims, func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("%w: метод подписи %q", ErrInvalidToken, token.Method.Alg())
		}
		return t.key, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidToken, err)
	}
	if !claims.Role.Valid() {
		return nil, fmt.Errorf("%w: неизвестная роль %q", ErrInvalidToken, claims.Role)
	}
	if _, err := claims.UserID(); err != nil {
		return nil, err
	}
	return &claims, nil
}

// LoginLimiter ограничивает попытки входа по логину.
//
// Нужен именно из-за argon2id: проверка пароля стоит десятки миллисекунд CPU и
// десятки мегабайт памяти, поэтому незащищённый логин — это готовый усилитель
// отказа в обслуживании, даже без единого угаданного пароля.
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

// NewLoginLimiter собирает лимитер попыток входа.
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

// Allow сообщает, можно ли ещё пробовать войти под этим логином.
func (l *LoginLimiter) Allow(login string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	a, ok := l.attempts[login]
	if !ok || now.After(a.until) {
		// Таблица ограничена сверху: логин выбирает клиент, и без потолка
		// перебор несуществующих логинов съел бы память процесса.
		if len(l.attempts) >= l.maxKeys {
			l.evictExpiredLocked(now)
		}
		l.attempts[login] = &attempt{count: 1, until: now.Add(l.window)}
		return true
	}

	a.count++
	return a.count <= l.limit
}

// Reset снимает счётчик после удачного входа.
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
	// Если протухших не нашлось, таблица всё равно не растёт: чистим целиком.
	// Потеря счётчиков хуже перебора, но лучше исчерпания памяти.
	if len(l.attempts) >= l.maxKeys {
		clear(l.attempts)
	}
}
