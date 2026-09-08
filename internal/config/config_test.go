package config_test

import (
	"bufio"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/OWNER/televote/internal/config"
)

// minimalEnv — тот минимум, без которого сервис не имеет права стартовать.
// Всё остальное обязано подставиться дефолтами, совпадающими с .env.example.
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

// productionEnv — валидная production-конфигурация: ни одного дефолтного секрета.
func productionEnv() map[string]string {
	return envWith(map[string]string{
		"ENV":                      "production",
		"POSTGRES_DSN":             "postgres://app:s3cret@pg-primary:5432/televote?sslmode=verify-full",
		"POSTGRES_READ_DSN":        "postgres://app:s3cret@pg-replica:5432/televote?sslmode=verify-full",
		"ADMIN_JWT_KEY":            strings.Repeat("c3", 32),
		"ADMIN_BOOTSTRAP_PASSWORD": "not-a-default-password",
	})
}

func TestLoad_DefaultsMatchEnvExample(t *testing.T) {
	t.Parallel()

	cfg, err := config.LoadFrom(minimalEnv())
	require.NoError(t, err)

	assert.Equal(t, "dev", cfg.Env)
	assert.Equal(t, "info", cfg.LogLevel)

	assert.Equal(t, ":8080", cfg.HTTPAddr)
	assert.Equal(t, "127.0.0.1:6060", cfg.DebugAddr)
	assert.Equal(t, 3*time.Second, cfg.ReadHeaderTimeout)
	assert.Equal(t, 5*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 10*time.Second, cfg.WriteTimeout)
	assert.Equal(t, 60*time.Second, cfg.IdleTimeout)
	assert.Equal(t, 25*time.Second, cfg.ShutdownGrace)
	assert.Equal(t, int64(1024), cfg.MaxBodyBytes)

	assert.Equal(t, []string{"redis-1:6379", "redis-2:6379"}, cfg.RedisAddrs)
	assert.Equal(t, 2*time.Second, cfg.RedisDialTimeout)
	assert.Equal(t, 250*time.Millisecond, cfg.RedisCmdTimeout)
	assert.Equal(t, 250*time.Millisecond, cfg.VoteRetryBudget)
	assert.InDelta(t, 0.5, cfg.BreakerErrorRatio, 1e-9)
	assert.Equal(t, 5*time.Second, cfg.BreakerWindow)

	assert.Equal(t, int32(20), cfg.PostgresMaxConns)
	assert.Equal(t, 2*time.Second, cfg.PollConfigRefresh)

	assert.Equal(t, []string{"kafka:9092"}, cfg.KafkaBrokers)
	assert.Equal(t, "votes", cfg.KafkaTopic)
	assert.NotEqual(t, cfg.KafkaConsumerGroup, cfg.KafkaFraudGroup,
		"анализ обязан читать топик независимо от подсчёта")
	assert.Equal(t, 5*time.Millisecond, cfg.KafkaLinger)

	assert.Equal(t, 30*time.Minute, cfg.DedupTTL)
	assert.InDelta(t, 0.1, cfg.DedupTTLJitter, 1e-9)

	assert.Equal(t, 6000, cfg.RateLimitPerMin)
	assert.Equal(t, 200, cfg.RateLimitBurst)
	assert.Equal(t, 200000, cfg.RateLimitMaxKeys)

	assert.True(t, cfg.ASNBlockEnabled)
	assert.InDelta(t, 0.01, cfg.AnomalySampleRate, 1e-9)

	assert.Equal(t, 30*time.Minute, cfg.AdminJWTTTL)
	assert.Equal(t, "admin", cfg.AdminBootstrapLogin)

	assert.Equal(t, 5*time.Second, cfg.SnapshotInterval)
	assert.Equal(t, 30*time.Second, cfg.SnapshotFinalGrace)

	assert.Equal(t, "televote", cfg.OTelServiceName)
	assert.InDelta(t, 0.0001, cfg.TraceSampleRatio, 1e-12)
	assert.Equal(t, "http://localhost:8080", cfg.PublicBaseURL)
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

// В dev дефолты из .env.example обязаны заводиться как есть: иначе `make demo`
// требует ручных шагов, а он не должен.
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

// Первый пункт таблицы тихих отказов в CLAUDE.md: ключ дедупа, истекающий
// раньше токена, открывает окно для replay. Проверяем с учётом джиттера —
// эффективный TTL уходит вниз на DEDUP_TTL_JITTER.
// Дедуп-ключ создаёт консьюмер, а не приём. Два сообщения одного голосующего
// могут быть обработаны в начале и в конце дренажа, и ключ обязан пережить
// этот разрыв: иначе второй голос будет засчитан как первый, тихо и без ошибок.
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
		{name: "ровно на границе без джиттера", dedupTTL: "10m30s", grace: "30s", jitter: "0"},
		{name: "короче окна дренажа", dedupTTL: "5m", grace: "30s", jitter: "0", wantErr: true},
		{name: "джиттер уводит нижнюю границу под дренаж", dedupTTL: "11m", grace: "30s", jitter: "0.1", wantErr: true},
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
		{name: "бюджет ретрая длиннее WriteTimeout", overrides: map[string]string{"VOTE_RETRY_BUDGET": "30s"}, wantErr: "VOTE_RETRY_BUDGET"},
		{name: "нулевой лимит тела запроса", overrides: map[string]string{"MAX_BODY_BYTES": "0"}, wantErr: "MAX_BODY_BYTES"},
		{name: "отрицательный джиттер дедупа", overrides: map[string]string{"DEDUP_TTL_JITTER": "-0.1"}, wantErr: "DEDUP_TTL_JITTER"},
		{name: "джиттер дедупа больше половины", overrides: map[string]string{"DEDUP_TTL_JITTER": "0.6"}, wantErr: "DEDUP_TTL_JITTER"},
		{name: "доля сэмплирования трейсов больше единицы", overrides: map[string]string{"OTEL_TRACE_SAMPLE_RATIO": "1.5"}, wantErr: "OTEL_TRACE_SAMPLE_RATIO"},
		{name: "доля сэмплирования аномалий больше единицы", overrides: map[string]string{"ANOMALY_SAMPLE_RATE": "2"}, wantErr: "ANOMALY_SAMPLE_RATE"},
		{name: "порог брейкера вне (0,1]", overrides: map[string]string{"BREAKER_ERROR_RATIO": "0"}, wantErr: "BREAKER_ERROR_RATIO"},
		{name: "группа анализа совпадает с группой подсчёта", overrides: map[string]string{"KAFKA_FRAUD_GROUP": "televote-counting"}, wantErr: "KAFKA_FRAUD_GROUP"},
		{name: "нулевой пул Postgres", overrides: map[string]string{"POSTGRES_MAX_CONNS": "0"}, wantErr: "POSTGRES_MAX_CONNS"},
		{name: "нулевой интервал рефрешера конфига", overrides: map[string]string{"POLL_CONFIG_REFRESH": "0s"}, wantErr: "POLL_CONFIG_REFRESH"},
		{name: "нулевой интервал снапшотов", overrides: map[string]string{"SNAPSHOT_INTERVAL": "0s"}, wantErr: "SNAPSHOT_INTERVAL"},
		{name: "нулевой rate limit", overrides: map[string]string{"RATE_LIMIT_PER_MIN": "0"}, wantErr: "RATE_LIMIT_PER_MIN"},
		{name: "публичный адрес без схемы", overrides: map[string]string{"PUBLIC_BASE_URL": "localhost:8080"}, wantErr: "PUBLIC_BASE_URL"},
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

// pprof, открытый наружу, — это дамп памяти процесса по HTTP.
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

func TestKafkaConfig_Validation(t *testing.T) {
	t.Parallel()

	t.Run("группы подсчёта и анализа обязаны различаться", func(t *testing.T) {
		t.Parallel()

		// Одна группа означала бы, что анализ забирает сообщения у подсчёта:
		// Kafka делит партиции между членами группы, а не дублирует их.
		_, err := config.LoadFrom(envWith(map[string]string{
			"KAFKA_CONSUMER_GROUP": "same",
			"KAFKA_FRAUD_GROUP":    "same",
		}))
		require.Error(t, err)
	})

	t.Run("Kafka без брокеров не даёт стартовать", func(t *testing.T) {
		t.Parallel()

		// Приём голосов идёт только через Kafka: без брокеров инстанс не
		// примет ни одного голоса, и падать надо на старте, а не в эфире.
		_, err := config.LoadFrom(envWith(map[string]string{"KAFKA_BROKERS": ""}))
		require.Error(t, err)
	})
}

// Контракт: .env.example — единственный источник правды про переменные.
// Если в него добавили переменную, а в Config — нет, тест обязан упасть.
func TestConfig_CoversEveryVariableInEnvExample(t *testing.T) {
	t.Parallel()

	path := filepath.Join("..", "..", ".env.example")
	f, err := os.Open(path) //nolint:gosec // путь фиксированный, внутри репозитория
	if err != nil {
		t.Skipf("%s недоступен: %v", path, err)
	}
	t.Cleanup(func() { _ = f.Close() })

	// Переменные стенда: их читает docker-compose через ${VAR:-default},
	// в бинарь они не попадают и попадать не должны.
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

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()

	a, err := netip.ParseAddr(s)
	require.NoError(t, err)
	return a
}
