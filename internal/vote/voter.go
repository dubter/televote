// Package vote содержит горячий путь голоса: вывод идентификатора голосующего,
// шардирование ключей и атомарное применение голоса в Redis.
package vote

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// VoterID — идентификатор голосующего внутри одного опроса. Выводится сервером
// из присланного клиентом значения и соли опроса, поэтому клиент не может ни
// занять чужой ключ, ни связать своё участие в разных опросах.
type VoterID [16]byte

// Hex — представление для передачи в сообщении Kafka и обратно.
func (v VoterID) Hex() string { return hex.EncodeToString(v[:]) }

// ParseVoterID читает VoterID из hex — так консьюмер восстанавливает его из
// сообщения. Пара Hex/ParseVoterID обязана быть обратимой, иначе голос
// применится не к тому ключу.
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

// Ошибки вывода идентификатора. Разделены намеренно: битый clientID — вина
// клиента и повод для 400, а битая соль — испорченная строка опроса, то есть
// вина сервера, и отдавать за неё 400 нельзя.
var (
	// ErrBadClientID — клиент прислал пустое, константное или слишком длинное значение.
	ErrBadClientID = errors.New("bad_client_id")
	// ErrBadSalt — у опроса отсутствует или слишком короткая соль.
	ErrBadSalt = errors.New("bad_poll_salt")
)

const (
	// minSaltLen — соль короче этого не даёт HMAC осмысленной стойкости.
	minSaltLen = 16
	// maxClientIDLen — клиент присылает 128-битный UUID; всё, что заметно
	// длиннее, это мусор или попытка раздуть ключ в Redis. При 30 млн ключей
	// длина входа превращается в память кластера.
	maxClientIDLen = 256
)

// constantClientIDs — значения, которые присылают сломанные клиенты вместо
// идентификатора. Опасны не сами по себе: они склеивают миллионы зрителей в
// один дедуп-ключ, первый голос проходит, остальные получают already_counted,
// и целая когорта теряется молча. Отказ на входе делает поломку видимой.
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

// DeriveVoterID выводит идентификатор голосующего из соли опроса и присланного
// клиентом значения.
//
// Клиент присылает вход, ключ выводит сервер — это даёт три свойства сразу:
// длина ключа фиксирована независимо от присланного; чужой дедуп-ключ нельзя
// занять, потому что соль неизвестна; между опросами один и тот же браузер
// даёт несвязанные идентификаторы.
//
// Функция детерминирована: тот же вход даёт тот же ключ. На этом держится
// идемпотентность применения голоса, без которой повторная доставка
// сообщения из Kafka завышала бы результат.
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
