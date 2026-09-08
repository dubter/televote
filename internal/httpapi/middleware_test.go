package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OWNER/televote/internal/httpapi"
)

func mustPrefixes(tb testing.TB, cidrs ...string) []netip.Prefix {
	tb.Helper()

	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		p, err := netip.ParsePrefix(c)
		require.NoError(tb, err)
		out = append(out, p)
	}
	return out
}

// probe отдаёт адрес, который middleware положил в контекст: именно он
// становится ключом лимита, поэтому проверять надо его, а не заголовки.
func probe() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(httpapi.IPFromContext(r.Context()).String()))
	})
}

func TestClientIP_TrustsProxyHop(t *testing.T) {
	t.Parallel()

	trusted := mustPrefixes(t, "10.0.0.0/8")
	h := httpapi.ClientIP(trusted)(probe())

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.RemoteAddr = "10.0.0.7:34567"
	r.Header.Set("X-Forwarded-For", "203.0.113.42")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	assert.Equal(t, "203.0.113.42", w.Body.String())
}

// Если брать X-Forwarded-For как есть, обход лимита — одна строка в curl,
// причём метрика лимитера остаётся зелёной: запросы считаются по подставным
// ключам, и с дашборда это выглядит нормальной работой.
func TestNFR9_ForgedXFFIgnored(t *testing.T) {
	t.Parallel()

	h := httpapi.ClientIP(mustPrefixes(t, "10.0.0.0/8"))(probe())

	cases := []struct {
		name string
		xff  string
	}{
		{"подделка адреса", "1.2.3.4"},
		{"цепочка подделок", "1.2.3.4, 5.6.7.8"},
		{"мусор", "не адрес"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodPost, "/", nil)
			r.RemoteAddr = "198.51.100.9:12345" // не доверенный источник
			r.Header.Set("X-Forwarded-For", tc.xff)

			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			assert.Equal(t, "198.51.100.9", w.Body.String(),
				"заголовок от недоверенного источника не имеет права влиять на ключ лимита")
		})
	}
}

// Клиенту выдают целую /64, поэтому лимит по полному адресу не защищает ни от
// чего: каждый запрос приходит с нового адреса той же подсети.
func TestNFR9_IPv6LimitedByPrefix(t *testing.T) {
	t.Parallel()

	first := netip.MustParseAddr("2001:db8:abcd:1234::1")
	second := netip.MustParseAddr("2001:db8:abcd:1234:ffff:ffff:ffff:ffff")
	other := netip.MustParseAddr("2001:db8:abcd:9999::1")

	assert.Equal(t, httpapi.LimitKey(first), httpapi.LimitKey(second),
		"адреса одной /64 обязаны делить ключ лимита")
	assert.NotEqual(t, httpapi.LimitKey(first), httpapi.LimitKey(other))

	v4 := netip.MustParseAddr("203.0.113.42")
	assert.Equal(t, "203.0.113.42", httpapi.LimitKey(v4), "для IPv4 ключ — полный адрес")
	assert.Equal(t, "unknown", httpapi.LimitKey(netip.Addr{}))
}

// Агрегат накрутки считается по подсети: она показывает аномалию и не
// идентифицирует человека.
func TestNet16_AggregatesBySubnet(t *testing.T) {
	t.Parallel()

	a := netip.MustParseAddr("203.0.113.42")
	b := netip.MustParseAddr("203.0.99.1")
	c := netip.MustParseAddr("198.51.100.9")

	assert.Equal(t, httpapi.Net16(a), httpapi.Net16(b))
	assert.NotEqual(t, httpapi.Net16(a), httpapi.Net16(c))
	assert.NotContains(t, httpapi.Net16(a), "113.42", "полный адрес не имеет права попасть в агрегат")
}

func TestRateLimit_ReturnsTooManyRequestsWithRetryAfter(t *testing.T) {
	t.Parallel()

	h := httpapi.ClientIP(nil)(httpapi.RateLimit(2, time.Minute)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }),
	))

	call := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = "203.0.113.42:1111"
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}

	require.Equal(t, http.StatusAccepted, call().Code)
	require.Equal(t, http.StatusAccepted, call().Code)

	limited := call()
	assert.Equal(t, http.StatusTooManyRequests, limited.Code)
	assert.NotEmpty(t, limited.Header().Get("Retry-After"), "клиенту нужно знать, когда повторить")
}

func TestBlockDatacenterASN_Returns403(t *testing.T) {
	t.Parallel()

	ranges := mustPrefixes(t, "198.51.100.0/24")
	h := httpapi.ClientIP(nil)(httpapi.BlockDatacenterASN(ranges)(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) }),
	))

	cases := map[string]int{
		"198.51.100.9:1111": http.StatusForbidden, // датацентр
		"203.0.113.42:1111": http.StatusAccepted,  // живой зритель
	}

	for remote, want := range cases {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = remote
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		assert.Equal(t, want, w.Code, "адрес %s", remote)
	}
}

// Класс устройства уезжает в Kafka вместо User-Agent. Полный заголовок — это
// отпечаток, а класс им не является: у сотен тысяч зрителей он совпадает.
func TestUAClass_CollapsesMinorVersions(t *testing.T) {
	t.Parallel()

	iphone18a := "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1_1 like Mac OS X) AppleWebKit/605.1.15"
	iphone18b := "Mozilla/5.0 (iPhone; CPU iPhone OS 18_4 like Mac OS X) AppleWebKit/605.1.15"

	assert.Equal(t, httpapi.UAClass(iphone18a), httpapi.UAClass(iphone18b),
		"минорные версии обязаны схлопываться, иначе класс становится отпечатком")
	assert.Equal(t, "iOS 18", httpapi.UAClass(iphone18a))
	assert.Equal(t, "Android 14", httpapi.UAClass("Mozilla/5.0 (Linux; Android 14; Pixel 8)"))
	assert.Equal(t, "unknown", httpapi.UAClass(""))
	assert.NotContains(t, httpapi.UAClass(iphone18a), "AppleWebKit")
}

func TestNFR9_SecurityHeadersPresent(t *testing.T) {
	t.Parallel()

	h := httpapi.SecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))

	for header, want := range map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "no-referrer",
	} {
		assert.Equal(t, want, w.Header().Get(header), "заголовок %s", header)
	}
	assert.NotEmpty(t, w.Header().Get("Strict-Transport-Security"))
}
