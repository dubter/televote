package httpx

import "strings"

// UAClass сводит User-Agent к грубому классу устройства.
//
// В сообщение Kafka уезжает именно класс, а не заголовок целиком: полный
// User-Agent — это отпечаток, а «iOS 18» им не является. Класса хватает,
// чтобы увидеть подсеть, где всё разнообразие устройств схлопнулось в одно
// значение, и не хватает, чтобы кого-то опознать.
//
// Как ключ дедупа не годится в принципе: у сотен тысяч зрителей с одинаковыми
// телефонами он совпадает, и правило «один голос на класс» отвергло бы их всех.
func UAClass(userAgent string) string {
	ua := strings.ToLower(userAgent)

	switch {
	case ua == "":
		return "unknown"
	case strings.Contains(ua, "iphone"), strings.Contains(ua, "ipad"):
		return "iOS " + majorVersionAfter(ua, "os ")
	case strings.Contains(ua, "android"):
		return "Android " + majorVersionAfter(ua, "android ")
	case strings.Contains(ua, "windows"):
		return "Windows"
	case strings.Contains(ua, "mac os"):
		return "macOS"
	case strings.Contains(ua, "linux"):
		return "Linux"
	default:
		return "other"
	}
}

// majorVersionAfter достаёт мажорную версию: минорные и патч-версии дробили бы
// классы на сотни значений и вернули бы отпечаток через заднюю дверь.
func majorVersionAfter(ua, marker string) string {
	i := strings.Index(ua, marker)
	if i < 0 {
		return "?"
	}

	var digits strings.Builder
	for _, r := range ua[i+len(marker):] {
		if r < '0' || r > '9' {
			break
		}
		digits.WriteRune(r)
	}
	if digits.Len() == 0 {
		return "?"
	}
	return digits.String()
}
