package vote

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

type VoterID [16]byte

func (v VoterID) Hex() string { return hex.EncodeToString(v[:]) }

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

var (
	ErrBadClientID = errors.New("bad_client_id")
	ErrBadSalt     = errors.New("bad_poll_salt")
)

const (
	minSaltLen     = 16
	maxClientIDLen = 256
)

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
