package config_test

import (
	"bufio"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dubter/televote/internal/platform/config"
)

func minimalEnv() map[string]string {
	return map[string]string{
		"REDIS_ADDRS":   "redis-1:6379,redis-2:6379",
		"KAFKA_BROKERS": "kafka:9092",
		"POSTGRES_DSN":  "postgres://u:p@localhost:5432/televote?sslmode=disable",
	}
}

func envWith(overrides map[string]string) map[string]string {
	e := minimalEnv()
	for k, v := range overrides {
		if v == "" {
			delete(e, k)
			continue
		}
		e[k] = v
	}
	return e
}

func productionEnv() map[string]string {
	return envWith(map[string]string{
		"ENV":                      "production",
		"POSTGRES_DSN":             "postgres://app:s3cret@pg-primary:5432/televote?sslmode=verify-full",
		"POSTGRES_READ_DSN":        "postgres://app:s3cret@pg-replica:5432/televote?sslmode=verify-full",
		"ADMIN_JWT_KEY":            strings.Repeat("c3", 32),
		"ADMIN_BOOTSTRAP_PASSWORD": "not-a-default-password",
	})
}

func TestLoad_MissingRequiredValuesFail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		unset   string
		wantErr string
	}{
		{name: "без POSTGRES_DSN", unset: "POSTGRES_DSN", wantErr: "POSTGRES_DSN"},
		{name: "без REDIS_ADDRS", unset: "REDIS_ADDRS", wantErr: "REDIS_ADDRS"},
		{name: "без KAFKA_BROKERS", unset: "KAFKA_BROKERS", wantErr: "KAFKA_BROKERS"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := minimalEnv()
			delete(e, tt.unset)

			cfg, err := config.LoadFrom(e)

			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.ErrorIs(t, err, config.ErrInvalidConfig)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoad_ProductionRejectsDefaultCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string]string
		wantErr   string
	}{
		{
			name:      "дефолтный ключ админского JWT",
			overrides: map[string]string{"ADMIN_JWT_KEY": "CHANGE_ME_2222222222222222222222222222222222222222222222222222222222"},
			wantErr:   "ADMIN_JWT_KEY",
		},
		{
			name:      "dev-пароль бутстрап-админа",
			overrides: map[string]string{"ADMIN_BOOTSTRAP_PASSWORD": "dev-only-change-me"},
			wantErr:   "ADMIN_BOOTSTRAP_PASSWORD",
		},
		{
			name:      "пустой пароль бутстрап-админа",
			overrides: map[string]string{"ADMIN_BOOTSTRAP_PASSWORD": ""},
			wantErr:   "ADMIN_BOOTSTRAP_PASSWORD",
		},
		{
			name:      "дефолтные креды Postgres televote:televote",
			overrides: map[string]string{"POSTGRES_DSN": "postgres://televote:televote@postgres:5432/televote?sslmode=disable"},
			wantErr:   "POSTGRES_DSN",
		},
		{
			name:      "дефолтные креды у реплики",
			overrides: map[string]string{"POSTGRES_READ_DSN": "postgres://televote:televote@postgres:5432/televote?sslmode=disable"},
			wantErr:   "POSTGRES_READ_DSN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := productionEnv()
			for k, v := range tt.overrides {
				if v == "" {
					delete(e, k)
					continue
				}
				e[k] = v
			}

			cfg, err := config.LoadFrom(e)

			require.Error(t, err, "production обязан отказаться стартовать")
			assert.Nil(t, cfg)
			assert.ErrorIs(t, err, config.ErrInsecureDefault)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoad_ProductionAcceptsRealSecrets(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(productionEnv())

	require.NoError(t, err)
	assert.True(t, cfg.IsProduction())
}

func TestLoad_DevAcceptsPlaceholderSecrets(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(envWith(map[string]string{
		"ADMIN_JWT_KEY":            "CHANGE_ME_2222222222222222222222222222222222222222222222222222222222",
		"ADMIN_BOOTSTRAP_PASSWORD": "dev-only-change-me",
		"POSTGRES_DSN":             "postgres://televote:televote@postgres:5432/televote?sslmode=disable",
	}))

	require.NoError(t, err)
	assert.False(t, cfg.IsProduction())
	assert.True(t, cfg.UsesInsecureDefaults(), "main.go обязан предупредить об этом в логе")
}

func TestLoad_DedupTTLMustOutliveDrainWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		dedupTTL string
		grace    string
		jitter   string
		wantErr  bool
	}{
		{name: "дефолты держат инвариант", dedupTTL: "30m", grace: "30s", jitter: "0.1"},
		{name: "ровно на границе без джиттера", dedupTTL: "15m30s", grace: "30s", jitter: "0"},
		{name: "короче окна дренажа", dedupTTL: "5m", grace: "30s", jitter: "0", wantErr: true},
		{name: "джиттер уводит нижнюю границу под дренаж", dedupTTL: "16m", grace: "30s", jitter: "0.1", wantErr: true},
		{name: "длинный grace требует длинного TTL", dedupTTL: "12m", grace: "5m", jitter: "0", wantErr: true},
		{name: "джиттер учтён запасом", dedupTTL: "30m", grace: "5m", jitter: "0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.LoadFrom(envWith(map[string]string{
				"DEDUP_TTL":            tt.dedupTTL,
				"SNAPSHOT_FINAL_GRACE": tt.grace,
				"DEDUP_TTL_JITTER":     tt.jitter,
			}))

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, cfg)
				assert.ErrorIs(t, err, config.ErrDedupTTLTooShort)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestLoad_RejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string]string
		wantErr   string
	}{
		{name: "неизвестное окружение", overrides: map[string]string{"ENV": "prod"}, wantErr: "ENV"},
		{name: "неизвестный уровень логов", overrides: map[string]string{"LOG_LEVEL": "trace"}, wantErr: "LOG_LEVEL"},
		{name: "нулевой ReadHeaderTimeout открывает Slowloris", overrides: map[string]string{"READ_HEADER_TIMEOUT": "0s"}, wantErr: "READ_HEADER_TIMEOUT"},
		{name: "ReadHeaderTimeout больше ReadTimeout", overrides: map[string]string{"READ_HEADER_TIMEOUT": "9s"}, wantErr: "READ_HEADER_TIMEOUT"},
		{name: "нулевой ShutdownGrace", overrides: map[string]string{"SHUTDOWN_GRACE": "0s"}, wantErr: "SHUTDOWN_GRACE"},
		{name: "бюджет ретрая длиннее окна дренажа", overrides: map[string]string{"VOTE_RETRY_BUDGET": "10m"}, wantErr: "VOTE_RETRY_BUDGET"},
		{name: "нулевой лимит тела запроса", overrides: map[string]string{"MAX_BODY_BYTES": "0"}, wantErr: "MAX_BODY_BYTES"},
		{name: "отрицательный джиттер дедупа", overrides: map[string]string{"DEDUP_TTL_JITTER": "-0.1"}, wantErr: "DEDUP_TTL_JITTER"},
		{name: "джиттер дедупа больше половины", overrides: map[string]string{"DEDUP_TTL_JITTER": "0.6"}, wantErr: "DEDUP_TTL_JITTER"},
		{name: "доля сэмплирования трейсов больше единицы", overrides: map[string]string{"OTEL_TRACE_SAMPLE_RATIO": "1.5"}, wantErr: "OTEL_TRACE_SAMPLE_RATIO"},
		{name: "доля сэмплирования аномалий больше единицы", overrides: map[string]string{"ANOMALY_SAMPLE_RATE": "2"}, wantErr: "ANOMALY_SAMPLE_RATE"},
		{name: "порог брейкера вне (0,1]", overrides: map[string]string{"BREAKER_ERROR_RATIO": "0"}, wantErr: "BREAKER_ERROR_RATIO"},
		{name: "нулевой пул Postgres", overrides: map[string]string{"POSTGRES_MAX_CONNS": "0"}, wantErr: "POSTGRES_MAX_CONNS"},
		{name: "ноль воркеров консьюмера", overrides: map[string]string{"CONSUMER_WORKERS": "0"}, wantErr: "CONSUMER_WORKERS"},
		{name: "нулевой интервал рефрешера конфига", overrides: map[string]string{"POLL_CONFIG_REFRESH": "0s"}, wantErr: "POLL_CONFIG_REFRESH"},
		{name: "нулевой интервал снапшотов", overrides: map[string]string{"SNAPSHOT_INTERVAL": "0s"}, wantErr: "SNAPSHOT_INTERVAL"},
		{name: "нулевой rate limit", overrides: map[string]string{"RATE_LIMIT_PER_MIN": "0"}, wantErr: "RATE_LIMIT_PER_MIN"},
		{name: "публичный адрес без схемы", overrides: map[string]string{"PUBLIC_BASE_URL": "localhost:8080"}, wantErr: "PUBLIC_BASE_URL"},
		{name: "адрес коллектора без схемы", overrides: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "otel-lgtm:4317"}, wantErr: "OTEL_EXPORTER_OTLP_ENDPOINT"},
		{name: "нечитаемый адрес коллектора", overrides: map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "://bad"}, wantErr: "OTEL_EXPORTER_OTLP_ENDPOINT"},
		{name: "мусор вместо доверенного прокси", overrides: map[string]string{"TRUSTED_PROXIES": "10.0.0.0/8,not-a-cidr"}, wantErr: "TRUSTED_PROXIES"},
		{name: "адрес вместо префикса в доверенных прокси", overrides: map[string]string{"TRUSTED_PROXIES": "10.0.0.1"}, wantErr: "TRUSTED_PROXIES"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg, err := config.LoadFrom(envWith(tt.overrides))

			require.Error(t, err)
			assert.Nil(t, cfg)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestLoad_ProductionRejectsPubliclyBoundDebugAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		addr    string
		wantErr bool
	}{
		{name: "loopback разрешён", addr: "127.0.0.1:6060"},
		{name: "loopback IPv6 разрешён", addr: "[::1]:6060"},
		{name: "выключенный pprof разрешён", addr: "off"},
		{name: "все интерфейсы запрещены", addr: ":6060", wantErr: true},
		{name: "0.0.0.0 запрещён", addr: "0.0.0.0:6060", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := productionEnv()
			e["DEBUG_ADDR"] = tt.addr

			cfg, err := config.LoadFrom(e)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "DEBUG_ADDR")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.addr != "off", cfg.DebugEnabled())
		})
	}
}

func TestLoad_ConsumerWorkersDefaultsAndOverrides(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(minimalEnv())
	require.NoError(t, err)
	assert.Equal(t, 64, cfg.ConsumerWorkers, "64 команд в полёте — дефолт, с которым мерили стоимость голоса")

	cfg, err = config.LoadFrom(envWith(map[string]string{"CONSUMER_WORKERS": "256"}))
	require.NoError(t, err)
	assert.Equal(t, 256, cfg.ConsumerWorkers)
}

func TestLoad_ReadDSNFallsBackToPrimary(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(minimalEnv())

	require.NoError(t, err)
	assert.Equal(t, cfg.PostgresDSN, cfg.PostgresReadDSN,
		"без реплики конфиг опросов обязан читаться с primary, а не из пустой строки")
}

func TestLoad_ParsesTrustedProxiesAsPrefixes(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(envWith(map[string]string{
		"TRUSTED_PROXIES": "10.0.0.0/8,172.16.0.0/12,2001:db8::/32",
	}))

	require.NoError(t, err)
	require.Len(t, cfg.TrustedProxies, 3)
	assert.True(t, cfg.TrustedProxies[0].Contains(mustAddr(t, "10.1.2.3")))
	assert.False(t, cfg.TrustedProxies[0].Contains(mustAddr(t, "11.1.2.3")))
	assert.True(t, cfg.TrustedProxies[2].Contains(mustAddr(t, "2001:db8::1")))
}

func TestConfig_CoversEveryVariableInEnvExample(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", ".env.example")
	f, err := os.Open(path) //nolint:gosec // fixed path inside the repository
	if err != nil {
		t.Fatalf("%s is unreadable: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })

	composeOnly := map[string]struct{}{
		"APP_PORT": {}, "APP1_PORT": {}, "APP2_PORT": {},
		"GRAFANA_PORT": {}, "POSTGRES_PORT": {},
	}

	known := config.EnvKeys()
	knownSet := make(map[string]struct{}, len(known))
	for _, k := range known {
		knownSet[k] = struct{}{}
	}

	var missing []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, _, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		name = strings.TrimSpace(name)
		if _, skip := composeOnly[name]; skip {
			continue
		}
		if _, found := knownSet[name]; !found {
			missing = append(missing, name)
		}
	}
	require.NoError(t, sc.Err())

	assert.Empty(t, missing, "переменные из .env.example не покрыты config.Config: %v", missing)
}

func TestConfig_EnvExampleLoadsAsIs(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", "..", ".env.example")
	body, err := os.ReadFile(path) //nolint:gosec // fixed path inside the repository
	require.NoError(t, err)

	environ := map[string]string{}
	for line := range strings.SplitSeq(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if name, value, ok := strings.Cut(line, "="); ok {
			environ[strings.TrimSpace(name)] = strings.Trim(strings.TrimSpace(value), `"`)
		}
	}

	_, err = config.LoadFrom(environ)
	assert.NoError(t, err, "пример конфигурации обязан запускаться как есть")
}

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()

	a, err := netip.ParseAddr(s)
	require.NoError(t, err)
	return a
}
