package vote_test

func redisSlot(key string) uint16 {
	if tag, ok := hashTagOf(key); ok {
		return crc16XModem(tag) & 16383
	}
	return crc16XModem(key) & 16383
}

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
