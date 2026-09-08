package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func SelfHealthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	probe := url.URL{Scheme: "http", Host: addr, Path: "/readyz"}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, probe.String(), http.NoBody)
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return 1
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("healthcheck: closing response body", slog.Any("error", err))
		}
	}()

	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func netDialer(timeout time.Duration) net.Dialer {
	return net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
}

type kafkaLag struct {
	client *kgo.Client
	topic  string
}

func (k kafkaLag) Lag(ctx context.Context) (int64, error) {
	admin := kadm.NewClient(k.client)

	name, ok := k.client.OptValue(kgo.ConsumerGroup).(string)
	if !ok || name == "" {
		return 0, fmt.Errorf("kafka lag: consumer group is not set")
	}

	lags, err := admin.Lag(ctx, name)
	if err != nil {
		return 0, fmt.Errorf("kafka lag: %w", err)
	}

	var total int64
	for name := range lags {
		for topic, partitions := range lags[name].Lag {
			if topic != k.topic {
				continue
			}
			for id := range partitions {
				if p := partitions[id]; p.Lag > 0 {
					total += p.Lag
				}
			}
		}
	}
	return total, nil
}

func (a *app) datacenterRanges() []netip.Prefix {
	if !a.cfg.ASNBlockEnabled {
		return nil
	}

	data, err := os.ReadFile(a.cfg.ASNBlocklistPath)
	if err != nil {
		a.log.Warn("datacenter network list not read, filter disabled",
			"path", a.cfg.ASNBlocklistPath, "error", err.Error())
		return nil
	}

	var out []netip.Prefix
	for line := range strings.SplitSeq(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		prefix, parseErr := netip.ParsePrefix(line)
		if parseErr != nil {
			a.log.Warn("datacenter list line skipped", "line", line)
			continue
		}
		out = append(out, prefix)
	}

	a.log.Info("datacenter network filter loaded", "prefixes", len(out))
	return out
}

func adminJWTBytes(raw string) []byte {
	if decoded, err := hex.DecodeString(raw); err == nil && len(decoded) >= 32 {
		return decoded
	}
	sum := sha256.Sum256([]byte("televote-dev-admin-jwt:" + raw))
	return sum[:]
}
