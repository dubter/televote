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

const (
	EnvDev        = "dev"
	EnvStaging    = "staging"
	EnvProduction = "production"
)

const debugDisabled = "off"

const minSecretLen = 32

var (
	ErrInvalidConfig = errors.New("invalid configuration")

	ErrInsecureDefault = errors.New("insecure secret: default from .env.example or a weak key")

	ErrDedupTTLTooShort = errors.New("dedup ttl is shorter than the drain window")
)

type FieldError struct {
	Key      string
	Reason   string
	sentinel error
}

func (e *FieldError) Error() string {
	return fmt.Sprintf("%s: %s", e.Key, e.Reason)
}

func (e *FieldError) Unwrap() []error {
	if e.sentinel == nil || errors.Is(e.sentinel, ErrInvalidConfig) {
		return []error{ErrInvalidConfig}
	}
	return []error{e.sentinel, ErrInvalidConfig}
}

type Config struct {
	Env      string `env:"ENV" envDefault:"dev"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`

	HTTPAddr          string        `env:"HTTP_ADDR" envDefault:":8080"`
	DebugAddr         string        `env:"DEBUG_ADDR" envDefault:"127.0.0.1:6060"`
	ReadHeaderTimeout time.Duration `env:"READ_HEADER_TIMEOUT" envDefault:"3s"`
	ReadTimeout       time.Duration `env:"READ_TIMEOUT" envDefault:"5s"`
	WriteTimeout      time.Duration `env:"WRITE_TIMEOUT" envDefault:"10s"`
	IdleTimeout       time.Duration `env:"IDLE_TIMEOUT" envDefault:"60s"`
	ShutdownGrace     time.Duration `env:"SHUTDOWN_GRACE" envDefault:"25s"`
	MaxBodyBytes      int64         `env:"MAX_BODY_BYTES" envDefault:"1024"`

	RedisAddrs        []string      `env:"REDIS_ADDRS" envSeparator:","`
	RedisDialTimeout  time.Duration `env:"REDIS_DIAL_TIMEOUT" envDefault:"2s"`
	RedisCmdTimeout   time.Duration `env:"REDIS_CMD_TIMEOUT" envDefault:"250ms"`
	VoteRetryBudget   time.Duration `env:"VOTE_RETRY_BUDGET" envDefault:"30s"`
	BreakerErrorRatio float64       `env:"BREAKER_ERROR_RATIO" envDefault:"0.5"`
	BreakerWindow     time.Duration `env:"BREAKER_WINDOW" envDefault:"5s"`

	PostgresDSN      string `env:"POSTGRES_DSN"`
	PostgresReadDSN  string `env:"POSTGRES_READ_DSN"`
	PostgresMaxConns int32  `env:"POSTGRES_MAX_CONNS" envDefault:"20"`

	PollConfigRefresh time.Duration `env:"POLL_CONFIG_REFRESH" envDefault:"2s"`

	PollMinLeadTime time.Duration `env:"POLL_MIN_LEAD_TIME" envDefault:"1h"`

	KafkaBrokers        []string      `env:"KAFKA_BROKERS" envSeparator:","`
	KafkaTopic          string        `env:"KAFKA_TOPIC" envDefault:"votes"`
	KafkaConsumerGroup  string        `env:"KAFKA_CONSUMER_GROUP" envDefault:"televote-counting"`
	KafkaLinger         time.Duration `env:"KAFKA_LINGER" envDefault:"5ms"`
	KafkaProduceTimeout time.Duration `env:"KAFKA_PRODUCE_TIMEOUT" envDefault:"2s"`

	DedupTTL       time.Duration `env:"DEDUP_TTL" envDefault:"30m"`
	DedupTTLJitter float64       `env:"DEDUP_TTL_JITTER" envDefault:"0.1"`

	RateLimitPerMin  int            `env:"RATE_LIMIT_PER_MIN" envDefault:"6000"`
	RateLimitBurst   int            `env:"RATE_LIMIT_BURST" envDefault:"200"`
	RateLimitMaxKeys int            `env:"RATE_LIMIT_MAX_KEYS" envDefault:"200000"`
	TrustedProxies   []netip.Prefix `env:"TRUSTED_PROXIES" envSeparator:","`

	ASNBlocklistPath  string  `env:"ASN_BLOCKLIST_PATH" envDefault:"/etc/televote/datacenter-ranges.txt"`
	ASNBlockEnabled   bool    `env:"ASN_BLOCK_ENABLED" envDefault:"true"`
	AnomalySampleRate float64 `env:"ANOMALY_SAMPLE_RATE" envDefault:"0.01"`

	AdminJWTKey            string        `env:"ADMIN_JWT_KEY"`
	AdminJWTTTL            time.Duration `env:"ADMIN_JWT_TTL" envDefault:"30m"`
	AdminBootstrapLogin    string        `env:"ADMIN_BOOTSTRAP_LOGIN" envDefault:"admin"`
	AdminBootstrapPassword string        `env:"ADMIN_BOOTSTRAP_PASSWORD"`

	SnapshotInterval   time.Duration `env:"SNAPSHOT_INTERVAL" envDefault:"5s"`
	SnapshotFinalGrace time.Duration `env:"SNAPSHOT_FINAL_GRACE" envDefault:"30s"`

	DrainWindow time.Duration `env:"DRAIN_WINDOW" envDefault:"5m"`

	OTLPEndpoint     string  `env:"OTEL_EXPORTER_OTLP_ENDPOINT" envDefault:"http://otel-lgtm:4317"`
	OTelServiceName  string  `env:"OTEL_SERVICE_NAME" envDefault:"televote"`
	TraceSampleRatio float64 `env:"OTEL_TRACE_SAMPLE_RATIO" envDefault:"0.0001"`
	SentryDSN        string  `env:"SENTRY_DSN"`

	PublicBaseURL string `env:"PUBLIC_BASE_URL" envDefault:"http://localhost:8080"`
}

func Load() (*Config, error) {
	return LoadFrom(env.ToMap(os.Environ()))
}

func LoadFrom(environ map[string]string) (*Config, error) {
	if environ == nil {
		environ = map[string]string{}
	}

	var cfg Config
	if err := env.ParseWithOptions(&cfg, env.Options{Environment: environ}); err != nil {
		return nil, translateParseError(err)
	}

	if cfg.PostgresReadDSN == "" {
		cfg.PostgresReadDSN = cfg.PostgresDSN
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func EnvKeys() []string {
	t := reflect.TypeFor[Config]()
	keys := make([]string, 0, t.NumField())
	for field := range t.Fields() {
		if key := envKeyOf(field); key != "" {
			keys = append(keys, key)
		}
	}
	return keys
}

func (c *Config) IsProduction() bool { return c.Env == EnvProduction }

func (c *Config) DebugEnabled() bool {
	return c.DebugAddr != "" && c.DebugAddr != debugDisabled
}

func (c *Config) UsesInsecureDefaults() bool {
	return isPlaceholderSecret(c.AdminJWTKey) ||
		isPlaceholderSecret(c.AdminBootstrapPassword) ||
		dsnHasDefaultCredentials(c.PostgresDSN) ||
		dsnHasDefaultCredentials(c.PostgresReadDSN)
}

func (c *Config) DedupTTLLowerBound() time.Duration {
	return time.Duration(float64(c.DedupTTL) * (1 - c.DedupTTLJitter))
}

func (c *Config) DrainBudget() time.Duration {
	return c.DrainWindow + c.SnapshotFinalGrace + drainSafetyMargin
}

const drainSafetyMargin = 10 * time.Minute

//nolint:gocyclo,gocognit // one flat list of rules; splitting it only hides it
func (c *Config) validate() error {
	var errs []error

	fail := func(key, format string, args ...any) {
		errs = append(errs, &FieldError{Key: key, Reason: fmt.Sprintf(format, args...)})
	}

	switch c.Env {
	case EnvDev, EnvStaging, EnvProduction:
	default:
		fail("ENV", "expected one of dev|staging|production, got %q", c.Env)
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		fail("LOG_LEVEL", "expected one of debug|info|warn|error, got %q", c.LogLevel)
	}

	if c.HTTPAddr == "" {
		fail("HTTP_ADDR", "is required")
	}
	if c.ReadHeaderTimeout <= 0 {
		fail("READ_HEADER_TIMEOUT", "must be positive: without it a Slowloris connection lives forever")
	}
	if c.ReadTimeout <= 0 {
		fail("READ_TIMEOUT", "must be positive")
	}
	if c.ReadHeaderTimeout > c.ReadTimeout {
		fail("READ_HEADER_TIMEOUT", "%s is greater than READ_TIMEOUT=%s and therefore never fires",
			c.ReadHeaderTimeout, c.ReadTimeout)
	}
	if c.WriteTimeout <= 0 {
		fail("WRITE_TIMEOUT", "must be positive")
	}
	if c.IdleTimeout <= 0 {
		fail("IDLE_TIMEOUT", "must be positive")
	}
	if c.ShutdownGrace <= 0 {
		fail("SHUTDOWN_GRACE", "must be positive: otherwise SIGTERM tears in-flight requests apart")
	}
	if c.MaxBodyBytes <= 0 {
		fail("MAX_BODY_BYTES", "must be positive")
	}
	if c.DebugEnabled() {
		if err := validateDebugAddr(c.DebugAddr, c.IsProduction()); err != nil {
			fail("DEBUG_ADDR", "%s", err.Error())
		}
	}

	if len(c.RedisAddrs) == 0 {
		fail("REDIS_ADDRS", "is required: comma-separated cluster node addresses")
	}
	for _, addr := range c.RedisAddrs {
		if strings.TrimSpace(addr) == "" {
			fail("REDIS_ADDRS", "contains an empty address")
			break
		}
	}
	if c.RedisDialTimeout <= 0 {
		fail("REDIS_DIAL_TIMEOUT", "must be positive")
	}
	if c.RedisCmdTimeout <= 0 {
		fail("REDIS_CMD_TIMEOUT", "must be positive")
	}
	if c.VoteRetryBudget <= 0 {
		fail("VOTE_RETRY_BUDGET", "must be positive")
	}
	if c.VoteRetryBudget >= c.DrainWindow {
		fail("VOTE_RETRY_BUDGET", "%s is not less than DRAIN_WINDOW=%s — a single message delays the whole drain",
			c.VoteRetryBudget, c.DrainWindow)
	}
	if c.BreakerErrorRatio <= 0 || c.BreakerErrorRatio > 1 {
		fail("BREAKER_ERROR_RATIO", "expected a ratio in (0,1], got %v", c.BreakerErrorRatio)
	}
	if c.BreakerWindow <= 0 {
		fail("BREAKER_WINDOW", "must be positive")
	}

	if c.PostgresDSN == "" {
		fail("POSTGRES_DSN", "is required")
	}
	if c.PostgresMaxConns <= 0 {
		fail("POSTGRES_MAX_CONNS", "must be positive")
	}

	if c.DrainWindow <= 0 {
		fail("DRAIN_WINDOW", "must be positive: capacity is derived from it")
	}
	if c.PollMinLeadTime < 0 {
		fail("POLL_MIN_LEAD_TIME", "must not be negative")
	}
	if c.PollConfigRefresh <= 0 {
		fail("POLL_CONFIG_REFRESH", "must be positive: the hot path reads memory only")
	}

	if len(c.KafkaBrokers) == 0 {
		fail("KAFKA_BROKERS", "at least one broker is required: votes are accepted only through kafka")
	}
	if c.KafkaTopic == "" {
		fail("KAFKA_TOPIC", "is required")
	}
	if c.KafkaConsumerGroup == "" {
		fail("KAFKA_CONSUMER_GROUP", "is required")
	}

	if c.DedupTTL <= 0 {
		fail("DEDUP_TTL", "must be positive")
	}
	if c.DedupTTLJitter < 0 || c.DedupTTLJitter > 0.5 {
		fail("DEDUP_TTL_JITTER", "expected a ratio in [0,0.5], got %v", c.DedupTTLJitter)
	}

	if c.RateLimitPerMin <= 0 {
		fail("RATE_LIMIT_PER_MIN", "must be positive")
	}
	if c.RateLimitBurst <= 0 {
		fail("RATE_LIMIT_BURST", "must be positive")
	}
	if c.RateLimitMaxKeys <= 0 {
		fail("RATE_LIMIT_MAX_KEYS", "must be positive: the limiter table has to stay bounded")
	}

	if c.ASNBlockEnabled && c.ASNBlocklistPath == "" {
		fail("ASN_BLOCKLIST_PATH", "is required when ASN_BLOCK_ENABLED=true")
	}
	if c.AnomalySampleRate < 0 || c.AnomalySampleRate > 1 {
		fail("ANOMALY_SAMPLE_RATE", "expected a ratio in [0,1], got %v", c.AnomalySampleRate)
	}
	if c.TraceSampleRatio < 0 || c.TraceSampleRatio > 1 {
		fail("OTEL_TRACE_SAMPLE_RATIO", "expected a ratio in [0,1], got %v", c.TraceSampleRatio)
	}
	if c.OTelServiceName == "" {
		fail("OTEL_SERVICE_NAME", "is required")
	}

	if c.AdminJWTTTL <= 0 {
		fail("ADMIN_JWT_TTL", "must be positive")
	}
	if c.AdminBootstrapLogin == "" {
		fail("ADMIN_BOOTSTRAP_LOGIN", "is required")
	}
	if c.SnapshotInterval <= 0 {
		fail("SNAPSHOT_INTERVAL", "must be positive")
	}
	if c.SnapshotFinalGrace < 0 {
		fail("SNAPSHOT_FINAL_GRACE", "must not be negative")
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

func (c *Config) validateDedupInvariant() error {
	if c.DedupTTL <= 0 || c.DrainWindow <= 0 {
		return nil
	}

	lower, need := c.DedupTTLLowerBound(), c.DrainBudget()
	if lower >= need {
		return nil
	}
	return &FieldError{
		Key: "DEDUP_TTL",
		Reason: fmt.Sprintf(
			"jittered lower bound %s (%s × (1−%v)) is shorter than the drain window %s — the key expires between messages of the same voter and a repeat vote gets counted",
			lower, c.DedupTTL, c.DedupTTLJitter, need),
		sentinel: ErrDedupTTLTooShort,
	}
}

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
		insecure("ADMIN_JWT_KEY", "is required in production")
	case isPlaceholderSecret(c.AdminJWTKey):
		insecure("ADMIN_JWT_KEY", "left at the default from .env.example")
	case len(c.AdminJWTKey) < minSecretLen:
		insecure("ADMIN_JWT_KEY", "is shorter than %d characters", minSecretLen)
	}

	switch {
	case c.AdminBootstrapPassword == "":
		insecure("ADMIN_BOOTSTRAP_PASSWORD", "is required in production")
	case isPlaceholderSecret(c.AdminBootstrapPassword):
		insecure("ADMIN_BOOTSTRAP_PASSWORD", "left at the default password from .env.example")
	}

	for _, d := range []struct{ key, dsn string }{
		{"POSTGRES_DSN", c.PostgresDSN},
		{"POSTGRES_READ_DSN", c.PostgresReadDSN},
	} {
		if dsnHasDefaultCredentials(d.dsn) {
			insecure(d.key, "default credentials from .env.example")
		}
	}

	if c.DedupTTLJitter <= 0 {
		insecure("DEDUP_TTL_JITTER", "must be positive in production")
	}

	return errs
}

func validateDebugAddr(addr string, production bool) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("expected host:port, got %q", addr)
	}
	if !production {
		return nil
	}
	if host == "" {
		return errors.New(`in production pprof must listen on loopback; "" means all interfaces — use 127.0.0.1 or off`)
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		//nolint:nilerr // hostname is resolved at runtime; only explicit public addresses are rejected
		return nil
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("in production pprof must listen on loopback, got %q — use 127.0.0.1 or off", host)
	}
	return nil
}

func validatePublicBaseURL(raw string) error {
	if raw == "" {
		return errors.New("is required: links and QR codes are built from it")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("cannot be parsed as a URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("expected an absolute http(s) URL, got %q", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("URL without host: %q", raw)
	}
	return nil
}

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
		return false
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

func translateParseError(err error) error {
	var agg env.AggregateError
	if !errors.As(err, &agg) {
		return fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	byField := fieldToEnvKey()
	out := make([]error, 0, len(agg.Errors))
	for _, e := range agg.Errors {
		if pe, ok := errors.AsType[env.ParseError](e); ok {
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
	t := reflect.TypeFor[Config]()
	m := make(map[string]string, t.NumField())
	for f := range t.Fields() {
		f := f
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
