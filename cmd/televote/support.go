package main

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

	"github.com/OWNER/televote/internal/storage/postgres"
	"github.com/OWNER/televote/pkg/health"
)

// selfHealthcheck дёргает /readyz собственного процесса.
//
// Нужен, потому что образ distroless: ни curl, ни wget, ни shell в нём нет,
// а healthcheck контейнера должен чем-то проверять готовность.
func selfHealthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}

	// Адрес собственного слушателя, а не пользовательский ввод: G704 здесь
	// ложное срабатывание, но запрос всё равно строится через NewRequest
	// с контекстом, чтобы не висеть дольше таймаута healthcheck контейнера.
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

// pgxpoolWrapper прячет пул за узким интерфейсом: наружу нужны только
// проверка готовности и закрытие.
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

// kafkaLag сообщает снапшотеру, сколько сообщений ещё не обработано.
//
// Ноль — это критерий финализации, а не таймаут: он означает, что все
// принятые голоса доехали до Redis и результат можно фиксировать.
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

// datacenterRanges читает список датацентровых сетей.
//
// Отсутствие файла — не ошибка старта: фильтр это дополнительный слой, и
// падать из-за него в момент эфира было бы хуже, чем работать без него.
// Но молчать нельзя, поэтому отсутствие попадает в лог.
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

// adminJWTBytes превращает ключ из конфига в байты подписи.
//
// В production конфиг уже потребовал полноценный секрет. В dev значение из
// .env.example — плейсхолдер, и вместо отказа стартовать оно сворачивается в
// детерминированный ключ: `make demo` обязан работать без ручной генерации,
// а два инстанса на одном .env — принимать токены друг друга.
func adminJWTBytes(raw string) []byte {
	if decoded, err := hex.DecodeString(raw); err == nil && len(decoded) >= 32 {
		return decoded
	}
	sum := sha256.Sum256([]byte("televote-dev-admin-jwt:" + raw))
	return sum[:]
}
