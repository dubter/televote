// Package config держит единственную структуру конфигурации сервиса и её
// валидацию. Контракт — файл .env.example в корне репозитория: каждая
// переменная оттуда имеет здесь поле, и это проверяется тестом.
package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Значения ENV.
const (
	EnvDev        = "dev"
	EnvStaging    = "staging"
	EnvProduction = "production"
)

// debugDisabled — значение DEBUG_ADDR, выключающее pprof-сервер.
// Пустая строка не годится: env-библиотека подставляет вместо неё envDefault.
const debugDisabled = "off"

// ballotKeySize — размер ключа HMAC для ballot-токенов, байт.
const ballotKeySize = 32

var (
	// ErrInvalidConfig — зонтичная ошибка: её оборачивает любой отказ валидации.
	ErrInvalidConfig = errors.New("некорректная конфигурация")

	// ErrInsecureDefault — в production остался секрет из .env.example либо
	// ключ не дотягивает до требований к продовому секрету.
	ErrInsecureDefault = errors.New("небезопасный секрет: дефолт из .env.example или слабый ключ")

	// ErrDedupTTLTooShort — первый пункт таблицы тихих отказов в CLAUDE.md:
	// ключ дедупа, истекающий раньше токена, открывает окно для replay.
	ErrDedupTTLTooShort = errors.New("ttl дедупа короче времени жизни ballot-токена")
)

// FieldError связывает отказ валидации с именем переменной окружения:
// сообщение «invalid duration» без имени переменной бесполезно в 03:00.
type FieldError struct {
	Key      string
	Reason   string
	sentinel error
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("%s: %s", e.Key, e.Reason)
}

// Unwrap возвращает и конкретную причину, и зонтичную ошибку, поэтому
// errors.Is работает и с ErrInvalidConfig, и с ErrDedupTTLTooShort.
func (e *FieldError) Unwrap() []error {
	if e.sentinel == nil || errors.Is(e.sentinel, ErrInvalidConfig) {
		return []error{ErrInvalidConfig}
	}
	return []error{e.sentinel, ErrInvalidConfig}
}

// Config — вся конфигурация сервиса. Одна плоская структура: поля читают
// параллельные пакеты, вложенность здесь только добавила бы им работы.
type Config struct {
	// ─── общее ───
	Env      string `env:"ENV" envDefault:"dev"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	// ─── HTTP ───
	HTTPAddr string `env:"HTTP_ADDR" envDefault:":8080"`
	// DebugAddr — pprof. "off" выключает его целиком.
	DebugAddr string `env:"DEBUG_ADDR" envDefault:"127.0.0.1:6060"`
	// ReadHeaderTimeout защищает от Slowloris: без него соединение,
	// отдающее заголовок по байту в минуту, живёт вечно.
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"3s"`
	ReadTimeout       time.Duration `env:"READ_TIMEOUT" envDefault:"5s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT" envDefault:"10s"`
	IdleTimeout       time.Duration `env:"IDLE_TIMEOUT" envDefault:"60s"`
	ShutdownGrace     time.Duration `env:"SHUTDOWN_GRACE" envDefault:"25s"`
	MaxBodyBytes      int64         `env:"MAX_BODY_BYTES" envDefault:"1024"`

	// ─── Redis Cluster ───
	RedisAddrs       []string      `env:"REDIS_ADDRS" envSeparator:","`
	RedisDialTimeout time.Duration `env:"REDIS_DIAL_TIMEOUT" envDefault:"2s"`
	RedisCmdTimeout  time.Duration `env:"REDIS_CMD_TIMEOUT" envDefault:"250ms"`
	// VoteRetryBudget — бюджет синхронного ретрая голоса. Длиннее — копим
	// соединения и умираем сами; голос хранит браузер, а не сервер.
	VoteRetryBudget   time.Duration `env:"VOTE_RETRY_BUDGET" envDefault:"250ms"`
	BreakerErrorRatio float64       `env:"BREAKER_ERROR_RATIO" envDefault:"0.5"`
	BreakerWindow     time.Duration `env:"BREAKER_WINDOW" envDefault:"5s"`

	// ─── Postgres ───
	PostgresDSN string `env:"POSTGRES_DSN"`
	// PostgresReadDSN — реплика для конфига опросов. Пустой — читаем с primary.
	PostgresReadDSN  string `env:"POSTGRES_READ_DSN"`
	PostgresMaxConns int32  `env:"POSTGRES_MAX_CONNS" envDefault:"20"`

	// ─── кэш конфига опроса ───
	// Фоновый рефрешер, а не ленивый TTL: истечение при 2M RPS даёт
	// thundering herd из тысяч одновременных промахов.
	PollConfigRefresh time.Duration `env:"POLL_CONFIG_REFRESH" envDefault:"2s"`

	// PollMinLeadTime — насколько заранее обязан создаваться опрос.
	PollMinLeadTime time.Duration `env:"POLL_MIN_LEAD_TIME" envDefault:"1h"`

	// ─── ballot-токены ───
	// Два ключа: подписываем текущим, проверяем обоими. Иначе ротация в эфире
	// инвалидирует все выданные токены разом.
	KafkaBrokers        []string      `env:"KAFKA_BROKERS" envSeparator:","`
	KafkaTopic          string        `env:"KAFKA_TOPIC" envDefault:"votes"`
	KafkaConsumerGroup  string        `env:"KAFKA_CONSUMER_GROUP" envDefault:"televote-counting"`
	KafkaFraudGroup     string        `env:"KAFKA_FRAUD_GROUP" envDefault:"televote-fraud"`
	KafkaLinger         time.Duration `env:"KAFKA_LINGER" envDefault:"5ms"`
	KafkaProduceTimeout time.Duration `env:"KAFKA_PRODUCE_TIMEOUT" envDefault:"2s"`

	// ─── дедупликация ───
	DedupTTL time.Duration `env:"DEDUP_TTL" envDefault:"30m"`
	// DedupTTLJitter — разброс TTL. Без него 30 млн ключей истекут разом.
	DedupTTLJitter float64 `env:"DEDUP_TTL_JITTER" envDefault:"0.1"`

	// ─── rate limit ───
	RateLimitPerMin  int `env:"RATE_LIMIT_PER_MIN" envDefault:"6000"`
	RateLimitBurst   int `env:"RATE_LIMIT_BURST" envDefault:"200"`
	RateLimitMaxKeys int `env:"RATE_LIMIT_MAX_KEYS" envDefault:"200000"`
	// TrustedProxies — X-Forwarded-For принимается только от этих адресов.
	// Иначе лимит обходится одной строкой в curl.
	TrustedProxies []netip.Prefix `env:"TRUSTED_PROXIES" envSeparator:","`

	// ─── антинакрутка ───
	ASNBlocklistPath  string  `env:"ASN_BLOCKLIST_PATH" envDefault:"/etc/televote/datacenter-ranges.txt"`
	ASNBlockEnabled   bool    `env:"ASN_BLOCK_ENABLED" envDefault:"true"`
	AnomalySampleRate float64 `env:"ANOMALY_SAMPLE_RATE" envDefault:"0.01"`

	// ─── админка ───
	AdminJWTKey            string        `env:"ADMIN_JWT_KEY"`
	AdminJWTTTL            time.Duration `env:"ADMIN_JWT_TTL" envDefault:"30m"`
	AdminBootstrapLogin    string        `env:"ADMIN_BOOTSTRAP_LOGIN" envDefault:"admin"`
	AdminBootstrapPassword string        `env:"ADMIN_BOOTSTRAP_PASSWORD"`

	// ─── снапшоты ───
	SnapshotInterval time.Duration `env:"SNAPSHOT_INTERVAL" envDefault:"5s"`
	// SnapshotFinalGrace — задержка перед финальным снапшотом, иначе теряется
	// хвост голосов в полёте.
	SnapshotFinalGrace time.Duration `env:"SNAPSHOT_FINAL_GRACE" envDefault:"30s"`

	// ─── наблюдаемость ───
	OTLPEndpoint     string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"http://otel-lgtm:4317"`
	OTelServiceName  string  `env:"OTEL_SERVICE_NAME" envDefault:"televote"`
	TraceSampleRatio float64 `env:"OTEL_TRACE_SAMPLE_RATIO" envDefault:"0.0001"`
	SentryDSN        string  `env:"SENTRY_DSN"`

	// ─── публичный адрес ───
	PublicBaseURL string `env:"PUBLIC_BASE_URL" envDefault:"http://localhost:8080"`
}

// Load читает конфигурацию из окружения процесса.
func Load() (*Config, error) {
	return LoadFrom(env.ToMap(os.Environ()))
}

// LoadFrom читает конфигурацию из явной карты переменных. Тесты и встраивание
// сервиса в стенд получают детерминированный конфиг без глобального окружения.
func LoadFrom(environ map[string]string) (*Config, error) {
	if environ == nil {
		// nil означал бы для env-библиотеки «возьми os.Environ()»,
		// а вызвавший LoadFrom(nil) просил ровно обратного.
		environ = map[string]string{}
	}

	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environ}); err != nil {
		return nil, translateParseError(err)
	}

	// Реплика необязательна: без неё конфиг опросов читается с primary.
	// Пустая строка здесь означала бы пул без адреса и падение при первом чтении.
	if cfg.PostgresReadDSN == "" {
		cfg.PostgresReadDSN = cfg.PostgresDSN
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// EnvKeys перечисляет имена переменных окружения, которые читает Config.
// Используется тестом-контрактом против .env.example.
func EnvKeys() []string {
	t := reflect.TypeOf(Config{})
	keys := make([]string, 0, t.NumField())
	for i := range t.NumField() {
		if key := envKeyOf(t.Field(i)); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

// IsProduction сообщает, что действуют строгие правила по секретам.
func (c *Config) IsProduction() bool { return c.Env == EnvProduction }

// DebugEnabled сообщает, нужно ли поднимать pprof-сервер.
func (c *Config) DebugEnabled() bool {
	return c.DebugAddr != "" && c.DebugAddr != debugDisabled
}

// UsesInsecureDefaults сообщает, что запущено с секретами из .env.example.
// В production Load() до этого не доходит — там это ошибка старта; в dev
// main.go обязан написать предупреждение в лог.
func (c *Config) UsesInsecureDefaults() bool {
	for _, v := range []string{
		c.AdminJWTKey, c.AdminBootstrapPassword,
	} {
		if isPlaceholderSecret(v) {
			return true
		}
	}
	return dsnHasDefaultCredentials(c.PostgresDSN) || dsnHasDefaultCredentials(c.PostgresReadDSN)
}

// DedupTTLLowerBound — наименьший TTL, который может выдать джиттер.
// Именно он, а не номинальный DedupTTL, обязан перекрывать окно дренажа.
func (c *Config) DedupTTLLowerBound() time.Duration {
	return time.Duration(float64(c.DedupTTL) * (1 - c.DedupTTLJitter))
}

// DrainBudget — сколько времени отводится на дренаж, с запасом.
//
// Дедуп-ключ создаёт консьюмер, а не приём: два сообщения одного человека
// могут быть обработаны в начале и в конце дренажа, и ключ обязан пережить
// этот разрыв. Иначе второй голос будет засчитан как первый.
func (c *Config) DrainBudget() time.Duration {
	return c.SnapshotFinalGrace + drainSafetyMargin
}

// drainSafetyMargin — запас поверх grace-периода.
const drainSafetyMargin = 10 * time.Minute

//nolint:gocyclo,gocognit // это один список правил; разбиение на функции здесь только прячет его.
func (c *Config) validate() error {
	var errs []error

	fail := func(key, format string, args ...any) {
		errs = append(errs, &FieldError{Key: key, Reason: fmt.Sprintf(format, args...)})
	}

	switch c.Env {
	case EnvDev, EnvStaging, EnvProduction:
	default:
		fail("ENV", "ожидается один из dev|staging|production, получено %q", c.Env)
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		fail("LOG_LEVEL", "ожидается один из debug|info|warn|error, получено %q", c.LogLevel)
	}

	// ─── HTTP ───
	if c.HTTPAddr == "" {
		fail("HTTP_ADDR", "обязателен")
	}
	if c.ReadHeaderTimeout <= 0 {
		fail("READ_HEADER_TIMEOUT", "должен быть положительным: без него соединение Slowloris живёт вечно")
	}
	if c.ReadTimeout <= 0 {
		fail("READ_TIMEOUT", "должен быть положительным")
	}
	if c.ReadHeaderTimeout > c.ReadTimeout {
		fail("READ_HEADER_TIMEOUT", "%s больше READ_TIMEOUT=%s и потому никогда не сработает",
			c.ReadHeaderTimeout, c.ReadTimeout)
	}
	if c.WriteTimeout <= 0 {
		fail("WRITE_TIMEOUT", "должен быть положительным")
	}
	if c.IdleTimeout <= 0 {
		fail("IDLE_TIMEOUT", "должен быть положительным")
	}
	if c.ShutdownGrace <= 0 {
		fail("SHUTDOWN_GRACE", "должен быть положительным: иначе SIGTERM рвёт запросы в полёте")
	}
	if c.MaxBodyBytes <= 0 {
		fail("MAX_BODY_BYTES", "должен быть положительным")
	}
	if c.DebugEnabled() {
		if err := validateDebugAddr(c.DebugAddr, c.IsProduction()); err != nil {
			fail("DEBUG_ADDR", "%s", err.Error())
		}
	}

	// ─── Redis ───
	if len(c.RedisAddrs) == 0 {
		fail("REDIS_ADDRS", "обязателен: адреса нод кластера через запятую")
	}
	for _, addr := range c.RedisAddrs {
		if strings.TrimSpace(addr) == "" {
			fail("REDIS_ADDRS", "содержит пустой адрес")
			break
		}
	}
	if c.RedisDialTimeout <= 0 {
		fail("REDIS_DIAL_TIMEOUT", "должен быть положительным")
	}
	if c.RedisCmdTimeout <= 0 {
		fail("REDIS_CMD_TIMEOUT", "должен быть положительным")
	}
	if c.VoteRetryBudget <= 0 {
		fail("VOTE_RETRY_BUDGET", "должен быть положительным")
	}
	if c.VoteRetryBudget >= c.WriteTimeout {
		fail("VOTE_RETRY_BUDGET", "%s не меньше WRITE_TIMEOUT=%s — сервер оборвёт ответ раньше, чем ретрай закончится",
			c.VoteRetryBudget, c.WriteTimeout)
	}
	if c.BreakerErrorRatio <= 0 || c.BreakerErrorRatio > 1 {
		fail("BREAKER_ERROR_RATIO", "ожидается доля в (0,1], получено %v", c.BreakerErrorRatio)
	}
	if c.BreakerWindow <= 0 {
		fail("BREAKER_WINDOW", "должно быть положительным")
	}

	// ─── Postgres ───
	if c.PostgresDSN == "" {
		fail("POSTGRES_DSN", "обязателен")
	}
	if c.PostgresMaxConns <= 0 {
		fail("POSTGRES_MAX_CONNS", "должно быть положительным")
	}

	if c.PollMinLeadTime < 0 {
		fail("POLL_MIN_LEAD_TIME", "не может быть отрицательным")
	}
	if c.PollConfigRefresh <= 0 {
		fail("POLL_CONFIG_REFRESH", "должен быть положительным: горячий путь читает только память")
	}

	// ─── Kafka ───
	if len(c.KafkaBrokers) == 0 {
		fail("KAFKA_BROKERS", "нужен хотя бы один брокер: приём голосов идёт только через Kafka")
	}
	if c.KafkaTopic == "" {
		fail("KAFKA_TOPIC", "не задан")
	}
	if c.KafkaConsumerGroup == "" || c.KafkaFraudGroup == "" {
		fail("KAFKA_CONSUMER_GROUP", "группы подсчёта и анализа обязаны быть заданы")
	}
	if c.KafkaConsumerGroup == c.KafkaFraudGroup {
		fail("KAFKA_FRAUD_GROUP",
			"совпадает с группой подсчёта: анализ обязан читать топик независимо, иначе он крадёт сообщения у подсчёта")
	}

	// ─── дедуп ───
	if c.DedupTTL <= 0 {
		fail("DEDUP_TTL", "должен быть положительным")
	}
	if c.DedupTTLJitter < 0 || c.DedupTTLJitter > 0.5 {
		fail("DEDUP_TTL_JITTER", "ожидается доля в [0,0.5], получено %v", c.DedupTTLJitter)
	}

	// ─── rate limit ───
	if c.RateLimitPerMin <= 0 {
		fail("RATE_LIMIT_PER_MIN", "должен быть положительным")
	}
	if c.RateLimitBurst <= 0 {
		fail("RATE_LIMIT_BURST", "должен быть положительным")
	}
	if c.RateLimitMaxKeys <= 0 {
		fail("RATE_LIMIT_MAX_KEYS", "должен быть положительным: таблица лимитера обязана быть ограничена")
	}

	// ─── антинакрутка и наблюдаемость ───
	if c.ASNBlockEnabled && c.ASNBlocklistPath == "" {
		fail("ASN_BLOCKLIST_PATH", "обязателен при ASN_BLOCK_ENABLED=true")
	}
	if c.AnomalySampleRate < 0 || c.AnomalySampleRate > 1 {
		fail("ANOMALY_SAMPLE_RATE", "ожидается доля в [0,1], получено %v", c.AnomalySampleRate)
	}
	if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
		fail("OTEL_TRACE_SAMPLE_RATIO", "ожидается доля в [0,1], получено %v", c.TraceSampleRatio)
	}
	if c.OTelServiceName == "" {
		fail("OTEL_SERVICE_NAME", "обязателен")
	}

	// ─── админка и снапшоты ───
	if c.AdminJWTTTL <= 0 {
		fail("ADMIN_JWT_TTL", "должен быть положительным")
	}
	if c.AdminBootstrapLogin == "" {
		fail("ADMIN_BOOTSTRAP_LOGIN", "обязателен")
	}
	if c.SnapshotInterval <= 0 {
		fail("SNAPSHOT_INTERVAL", "должен быть положительным")
	}
	if c.SnapshotFinalGrace < 0 {
		fail("SNAPSHOT_FINAL_GRACE", "не может быть отрицательным")
	}

	if err := validatePublicBaseURL(c.PublicBaseURL); err != nil {
		fail("PUBLIC_BASE_URL", "%s", err.Error())
	}

	if err := c.validateDedupInvariant(); err != nil {
		errs = append(errs, err)
	}
	errs = append(errs, c.validateProductionSecrets()...)

	return errors.Join(errs...)
}

// validateDedupInvariant — инвариант ttl(dedup) ≥ exp(token) + skew.
//
// Проверяется по нижней границе джиттера, а не по номиналу: DEDUP_TTL=16m при
// джиттере 0.1 даёт ключи, живущие 14.4 минуты, тогда как токен принимается
// 16 минут — и полутора минут хватает, чтобы переголосовать тем же токеном.
func (c *Config) validateDedupInvariant() error {
	if c.DedupTTL <= 0 || c.SnapshotFinalGrace <= 0 {
		return nil // о нулевых значениях уже сообщено отдельно
	}

	lower, need := c.DedupTTLLowerBound(), c.DrainBudget()
	if lower >= need {
		return nil
	}
	return &FieldError{
		Key: "DEDUP_TTL",
		Reason: fmt.Sprintf(
			"нижняя граница с джиттером %s (%s × (1−%v)) короче окна дренажа %s — ключ истечёт между сообщениями одного голосующего, и повторный голос будет засчитан",
			lower, c.DedupTTL, c.DedupTTLJitter, need),
		sentinel: ErrDedupTTLTooShort,
	}
}

// validateProductionSecrets: в production дефолтный секрет — ошибка старта,
// а не предупреждение. Предупреждение в логе на пике никто не прочитает.
func (c *Config) validateProductionSecrets() []error {
	if !c.IsProduction() {
		return nil
	}

	var errs []error
	insecure := func(key, format string, args ...any) {
		errs = append(errs, &FieldError{
			Key: key, Reason: fmt.Sprintf(format, args...), sentinel: ErrInsecureDefault,
		})
	}

	switch {
	case c.AdminJWTKey == "":
		insecure("ADMIN_JWT_KEY", "обязателен в production")
	case isPlaceholderSecret(c.AdminJWTKey):
		insecure("ADMIN_JWT_KEY", "оставлен дефолт из .env.example")
	case len(c.AdminJWTKey) < ballotKeySize:
		insecure("ADMIN_JWT_KEY", "короче %d символов", ballotKeySize)
	}

	switch {
	case c.AdminBootstrapPassword == "":
		insecure("ADMIN_BOOTSTRAP_PASSWORD", "обязателен в production")
	case isPlaceholderSecret(c.AdminBootstrapPassword):
		insecure("ADMIN_BOOTSTRAP_PASSWORD", "оставлен дефолтный пароль из .env.example")
	}

	for _, d := range []struct{ key, dsn string }{
		{"POSTGRES_DSN", c.PostgresDSN},
		{"POSTGRES_READ_DSN", c.PostgresReadDSN},
	} {
		if dsnHasDefaultCredentials(d.dsn) {
			insecure(d.key, "дефолтные креды из .env.example")
		}
	}

	// Джиттер обязателен: 30 млн ключей с одинаковым сроком истекут разом
	// и добьют Redis ровно на хвосте эфира.
	if c.DedupTTLJitter <= 0 {
		insecure("DEDUP_TTL_JITTER", "должен быть положительным в production")
	}

	return errs
}

// ─── helpers ───

func validateDebugAddr(addr string, production bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("ожидается host:port, получено %q", addr)
	}
	if !production {
		return nil
	}
	// pprof, открытый наружу, — это дамп памяти процесса по HTTP.
	// В памяти лежат ballot-токены и ключи подписи.
	if host == "" {
		return errors.New(`в production pprof обязан слушать loopback; "" означает все интерфейсы — используйте 127.0.0.1 или off`)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		//nolint:nilerr // имя хоста разрешит рантайм; запретить можем только явную публичность
		return nil
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("в production pprof обязан слушать loopback, получено %q — используйте 127.0.0.1 или off", host)
	}
	return nil
}

func validatePublicBaseURL(raw string) error {
	if raw == "" {
		return errors.New("обязателен: из него собираются ссылки и QR")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("не разбирается как URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("ожидается абсолютный http(s) URL, получено %q", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("URL без хоста: %q", raw)
	}
	return nil
}

// placeholderMarkers — маркеры незаполненных секретов из .env.example.
var placeholderMarkers = []string{"change_me", "changeme", "dev-only", "example", "placeholder", "secret123"}

func isPlaceholderSecret(v string) bool {
	lower := strings.ToLower(v)
	for _, m := range placeholderMarkers {
		if strings.Contains(lower, m) {
			return true
		}
	}
	return false
}

// dsnHasDefaultCredentials ловит `televote:televote` и прочие «логин равен
// паролю» из стендового .env.example.
func dsnHasDefaultCredentials(dsn string) bool {
	if dsn == "" {
		return false
	}
	if isPlaceholderSecret(dsn) {
		return true
	}
	u, err := url.Parse(dsn)
	if err != nil || u.User == nil {
		return false
	}
	user := u.User.Username()
	pass, hasPass := u.User.Password()
	if !hasPass || pass == "" {
		return false // без пароля — доверенная аутентификация, не наше дело
	}
	if pass == user {
		return true
	}
	switch strings.ToLower(pass) {
	case "postgres", "password", "televote", "admin", "root":
		return true
	default:
		return false
	}
}

// translateParseError переводит ошибки env-библиотеки на язык переменных
// окружения: она сообщает имя поля структуры, а в логе нужно имя переменной.
func translateParseError(err error) error {
	var agg env.AggregateError
	if !errors.As(err, &agg) {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	byField := fieldToEnvKey()
	out := make([]error, 0, len(agg.Errors))
	for _, e := range agg.Errors {
		var pe env.ParseError
		if errors.As(e, &pe) {
			key := byField[pe.Name]
			if key == "" {
				key = pe.Name
			}
			out = append(out, &FieldError{Key: key, Reason: pe.Err.Error()})
			continue
		}
		out = append(out, fmt.Errorf("%w: %w", ErrInvalidConfig, e))
	}
	return errors.Join(out...)
}

func fieldToEnvKey() map[string]string {
	t := reflect.TypeOf(Config{})
	m := make(map[string]string, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		if key := envKeyOf(f); key != "" {
			m[f.Name] = key
		}
	}
	return m
}

func envKeyOf(f reflect.StructField) string {
	tag, ok := f.Tag.Lookup("env")
	if !ok {
		return ""
	}
	key, _, _ := strings.Cut(tag, ",")
	if key == "-" {
		return ""
	}
	return key
}
