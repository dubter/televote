package httpx_test

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/platform/httpx"
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

func probe() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(httpx.IPFromContext(r.Context()).String()))
	})
}

func TestClientIP_TrustsProxyHop(t *testing.T) {
	t.Parallel()

	trusted := mustPrefixes(t, "10.0.0.0/8")
	h := httpx.ClientIP(trusted)(probe())

	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.RemoteAddr = "10.0.0.7:34567"
	r.Header.Set("X-Forwarded-For", "203.0.113.42")

	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	assert.Equal(t, "203.0.113.42", w.Body.String())
}

func TestNFR9_ForgedXFFIgnored(t *testing.T) {
	t.Parallel()

	h := httpx.ClientIP(mustPrefixes(t, "10.0.0.0/8"))(probe())

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

func TestNFR9_IPv6LimitedByPrefix(t *testing.T) {
	t.Parallel()

	first := netip.MustParseAddr("2001:db8:abcd:1234::1")
	second := netip.MustParseAddr("2001:db8:abcd:1234:ffff:ffff:ffff:ffff")
	other := netip.MustParseAddr("2001:db8:abcd:9999::1")

	assert.Equal(t, httpx.LimitKey(first), httpx.LimitKey(second),
		"адреса одной /64 обязаны делить ключ лимита")
	assert.NotEqual(t, httpx.LimitKey(first), httpx.LimitKey(other))

	v4 := netip.MustParseAddr("203.0.113.42")
	assert.Equal(t, "203.0.113.42", httpx.LimitKey(v4), "для IPv4 ключ — полный адрес")
	assert.Equal(t, "unknown", httpx.LimitKey(netip.Addr{}))
}

func TestRateLimit_ReturnsTooManyRequestsWithRetryAfter(t *testing.T) {
	t.Parallel()

	h := httpx.ClientIP(nil)(httpx.RateLimit(2, time.Minute)(
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

func TestNFR9_SecurityHeadersPresent(t *testing.T) {
	t.Parallel()

	h := httpx.SecurityHeaders(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
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
