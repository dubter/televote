// Package vote содержит горячий путь голоса: вывод voterID, шардирование
// ключей и атомарное применение голоса в Redis.
package vote

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// VoterID выводится сервером из присланного клиентом значения и соли опроса:
// клиент не может ни занять чужой ключ, ни связать участие в разных опросах.
type VoterID [16]byte

// Hex — представление для сообщения Kafka.
func (v VoterID) Hex() string { return hex.EncodeToString(v[:]) }

// ParseVoterID читает VoterID из hex. Пара с Hex обязана быть обратимой,
// иначе голос применится не к тому ключу.
func ParseVoterID(s string) (VoterID, error) {
	var v VoterID
	if len(s) != hex.EncodedLen(len(v)) {
		return VoterID{}, fmt.Errorf("voterID: ожидалось %d hex-символов, получено %d",
			hex.EncodedLen(len(v)), len(s))
	}
	if _, err := hex.Decode(v[:], []byte(s)); err != nil {
		return VoterID{}, fmt.Errorf("voterID: %w", err)
	}
	return v, nil
}

// Ошибки разделены намеренно: битый clientID — вина клиента (400), битая
// соль — испорченная строка опроса, то есть вина сервера.
var (
	// ErrBadClientID — клиент прислал пустое, константное или слишком длинное значение.
	ErrBadClientID = errors.New("bad_client_id")
	// ErrBadSalt — у опроса отсутствует или слишком короткая соль.
	ErrBadSalt = errors.New("bad_poll_salt")
)

const (
	minSaltLen = 16
	// При 30 млн ключей длина входа превращается в память кластера.
	maxClientIDLen = 256
)

// constantClientIDs — что присылают сломанные клиенты вместо идентификатора.
// Опасны тем, что склеивают миллионы зрителей в один дедуп-ключ: первый голос
// проходит, остальные теряются молча.
var constantClientIDs = map[string]struct{}{
	"":                                     {},
	"undefined":                            {},
	"null":                                 {},
	"nan":                                  {},
	"nil":                                  {},
	"none":                                 {},
	"false":                                {},
	"true":                                 {},
	"0":                                    {},
	"1":                                    {},
	"-":                                    {},
	"[object object]":                      {},
	"00000000-0000-0000-0000-000000000000": {},
}

// DeriveVoterID выводит идентификатор голосующего из соли опроса и значения,
// присланного клиентом.
func DeriveVoterID(salt []byte, clientID string) (VoterID, error) {
	if len(salt) < minSaltLen {
		return VoterID{}, fmt.Errorf("%w: длина %d, минимум %d", ErrBadSalt, len(salt), minSaltLen)
	}
	if len(clientID) > maxClientIDLen {
		return VoterID{}, fmt.Errorf("%w: длина %d, максимум %d", ErrBadClientID, len(clientID), maxClientIDLen)
	}

	normalized := strings.ToLower(strings.TrimSpace(clientID))
	if _, bad := constantClientIDs[normalized]; bad {
		return VoterID{}, fmt.Errorf("%w: %q", ErrBadClientID, clientID)
	}

	mac := hmac.New(sha256.New, salt)
	mac.Write([]byte(clientID))
	sum := mac.Sum(nil)

	var v VoterID
	copy(v[:], sum[:len(v)])
	return v, nil
}
