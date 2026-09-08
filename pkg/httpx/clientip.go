package httpx

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"strings"
)

type ctxKey int

const clientIPKey ctxKey = iota

func ClientIP(trusted []netip.Prefix) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			addr := resolveClientIP(r, trusted)
			ctx := context.WithValue(r.Context(), clientIPKey, addr)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func IPFromContext(ctx context.Context) netip.Addr {
	addr, ok := ctx.Value(clientIPKey).(netip.Addr)
	if !ok {
		return netip.Addr{}
	}
	return addr
}

func resolveClientIP(r *http.Request, trusted []netip.Prefix) netip.Addr {
	peer := peerAddr(r.RemoteAddr)
	if !peer.IsValid() || !isTrusted(peer, trusted) {
		return peer
	}

	parts := strings.Split(r.Header.Get("X-Forwarded-For"), ",")
	for i := len(parts) - 1; i >= 0; i-- {
		candidate, err := netip.ParseAddr(strings.TrimSpace(parts[i]))
		if err != nil {
			continue
		}
		if !isTrusted(candidate, trusted) {
			return candidate.Unmap()
		}
	}

	if fromHeader, err := netip.ParseAddr(strings.TrimSpace(r.Header.Get("X-Real-IP"))); err == nil {
		return fromHeader.Unmap()
	}
	return peer
}

func peerAddr(remoteAddr string) netip.Addr {
	host, _, splitErr := net.SplitHostPort(remoteAddr)
	if splitErr != nil {
		host = remoteAddr
	}
	addr, err := netip.ParseAddr(strings.TrimSpace(host))
	if err != nil {
		return netip.Addr{}
	}
	return addr.Unmap()
}

func isTrusted(addr netip.Addr, trusted []netip.Prefix) bool {
	for _, prefix := range trusted {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

const (
	ipv6LimitBits = 64
	net16Bits     = 16
	ipv6AggBits   = 32
)

func LimitKey(a netip.Addr) string {
	if !a.IsValid() {
		return "unknown"
	}
	if a.Is4() {
		return a.String()
	}
	prefix, err := a.Prefix(ipv6LimitBits)
	if err != nil {
		return a.String()
	}
	return prefix.String()
}

func Net16(a netip.Addr) string {
	if !a.IsValid() {
		return "unknown"
	}
	bits := net16Bits
	if a.Is6() && !a.Is4In6() {
		bits = ipv6AggBits
	}
	prefix, err := a.Prefix(bits)
	if err != nil {
		return "unknown"
	}
	return prefix.String()
}
