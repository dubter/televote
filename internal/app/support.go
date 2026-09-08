package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/metrics"
	"github.com/dubter/televote/internal/storage/postgres"
	"github.com/dubter/televote/internal/vote"
	"github.com/dubter/televote/pkg/health"
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

	//nolint:gosec // G704: адрес берётся из собственного HTTP_ADDR, не из запроса
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", http.NoBody)
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: адрес формируется из собственного HTTP_ADDR

	if err != nil {
		return 1
	}
	defer func() { _ = resp.Body.Close() }() //nolint:errcheck // healthcheck читает только код ответа

	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

type pgxpoolWrapper struct{ pool *pgxpool.Pool }

func openPool(ctx context.Context, dsn string, maxConns int32) (*pgxpoolWrapper, error) {
	pool, err := postgres.NewPool(ctx, dsn, postgres.WithMaxConns(maxConns))
	if err != nil {
		return nil, err
	}
	return &pgxpoolWrapper{pool: pool}, nil
}

func (p *pgxpoolWrapper) checker() health.Checker {
	return func(ctx context.Context) error { return p.pool.Ping(ctx) }
}

func (p *pgxpoolWrapper) close() { p.pool.Close() }

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
		return 0, fmt.Errorf("kafka lag: не задана группа консьюмеров")
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
		a.log.Warn("список датацентровых сетей не прочитан, фильтр выключен",
			"path", a.cfg.ASNBlocklistPath, "error", err.Error())
		return nil
	}

	var out []netip.Prefix
	for line := range strings.SplitSeq(string(data), "\n") { //nolint:gocritic // строки короткие
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		prefix, parseErr := netip.ParsePrefix(line)
		if parseErr != nil {
			a.log.Warn("строка списка датацентров пропущена", "line", line)
			continue
		}
		out = append(out, prefix)
	}

	a.log.Info("фильтр датацентровых сетей загружен", "prefixes", len(out))
	return out
}

func adminJWTBytes(raw string) []byte {
	if decoded, err := hex.DecodeString(raw); err == nil && len(decoded) >= 32 {
		return decoded
	}
	sum := sha256.Sum256([]byte("televote-dev-admin-jwt:" + raw))
	return sum[:]
}

type countingObserver struct{ m *metrics.Metrics }

func (o countingObserver) VoteCounted(ctx context.Context, r vote.Result) { o.m.VoteCounted(ctx, r) }
func (o countingObserver) VoteRejected(ctx context.Context, reason string) {
	o.m.VoteRejectedCtx(ctx, reason)
}
func (o countingObserver) ApplySeconds(d float64) { o.m.ApplySeconds(d) }
