package vote_test

// Слот Redis Cluster, посчитанный по спецификации, а не по реализации пакета.

// redisSlot повторяет keyHashSlot из cluster-spec: если в ключе есть непустой
// {...}, хэшируется только содержимое фигурных скобок, иначе весь ключ.
func redisSlot(key string) uint16 {
	if tag, ok := hashTagOf(key); ok {
		return crc16XModem(tag) & 16383
	}
	return crc16XModem(key) & 16383
}

// hashTagOf возвращает содержимое первого непустого {...} и признак того, что
// хэш-тег вообще есть. Именно отсутствие тега превращает пару ключей в
// CROSSSLOT, поэтому ok здесь важнее самой строки.
func hashTagOf(key string) (string, bool) {
	s := -1
	for i := 0; i < len(key); i++ {
		if key[i] == '{' {
			s = i
			break
		}
	}
	if s < 0 {
		return "", false
	}
	for e := s + 1; e < len(key); e++ {
		if key[e] == '}' {
			if e == s+1 {
				return "", false // пустой {} тегом не считается
			}
			return key[s+1 : e], true
		}
	}
	return "", false
}

// crc16XModem — CRC-16/XMODEM (poly 0x1021, init 0), тот же, что в Redis.
func crc16XModem(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc ^= uint16(s[i]) << 8
		for j := 0; j < 8; j++ {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}
