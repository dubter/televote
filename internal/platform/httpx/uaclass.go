package httpx

import "strings"

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

func majorVersionAfter(ua, marker string) string {
	_, after, ok := strings.Cut(ua, marker)
	if !ok {
		return "?"
	}

	var digits strings.Builder
	for _, r := range after {
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
