package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/adapter/postgres"
	"github.com/dubter/televote/internal/adapter/redis"
	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/platform/observability"
	"github.com/dubter/televote/internal/service/auth"
	"github.com/dubter/televote/internal/service/pollcfg"
	"github.com/dubter/televote/internal/transport/httpapi"
)

const (
	adminRateLimit      = 120
	lookupRefreshFactor = 3
	loginAttempts       = 5
	loginLimiterKeys    = 10_000
)

func (rt *runtime) openPostgres(ctx context.Context, dsn string) (*postgres.DB, error) {
	return postgres.Open(ctx, postgres.Config{DSN: dsn, MaxConns: rt.cfg.PostgresMaxConns})
}

func (rt *runtime) openRedis(ctx context.Context) (*redis.Client, error) {
	return redis.Open(ctx, redis.Config{Addrs: rt.cfg.RedisAddrs, DialTimeout: rt.cfg.RedisDialTimeout})
}

func (rt *runtime) newTally(client *redis.Client) (*redis.Tally, error) {
	return redis.NewTally(client, rt.cfg.DedupTTL, rt.cfg.DedupTTLJitter, rt.cfg.RedisCmdTimeout)
}

func (rt *runtime) openKafka(opts ...kgo.Opt) (*kgo.Client, error) {
	base := []kgo.Opt{
		kgo.WithHooks(observability.KafkaHooks(rt.cfg.KafkaConsumerGroup)...),
		kgo.SeedBrokers(rt.cfg.KafkaBrokers...),
		kgo.ConsumerGroup(rt.cfg.KafkaConsumerGroup),
		kgo.DisableAutoCommit(),
	}
	client, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	return client, nil
}

func (rt *runtime) warmPollCache(ctx context.Context, db *postgres.DB) (*pollcfg.Cache, error) {
	polls, err := postgres.NewPollRepo(db)
	if err != nil {
		return nil, fmt.Errorf("poll repository (read): %w", err)
	}

	cache, err := pollcfg.NewCache(polls, rt.cfg.PollConfigRefresh, pollcfg.WithLogger(rt.log))
	if err != nil {
		return nil, fmt.Errorf("poll config cache: %w", err)
	}
	cache = cache.WithObserver(rt.metrics)

	if err := cache.Warm(ctx); err != nil {
		return nil, fmt.Errorf("poll config cache warmup: %w", err)
	}
	return cache, nil
}

func (rt *runtime) buildAdmin(ctx context.Context, db *postgres.DB) (*httpapi.AdminHandler, error) {
	polls, err := postgres.NewPollRepo(db)
	if err != nil {
		return nil, fmt.Errorf("poll repository: %w", err)
	}
	results, err := postgres.NewResultRepo(db)
	if err != nil {
		return nil, fmt.Errorf("result repository: %w", err)
	}
	admins, err := postgres.NewAdminRepo(db)
	if err != nil {
		return nil, fmt.Errorf("admin repository: %w", err)
	}

	tokens, err := auth.NewTokenService(adminJWTBytes(rt.cfg.AdminJWTKey), rt.cfg.AdminJWTTTL)
	if err != nil {
		return nil, fmt.Errorf("token service: %w", err)
	}

	if err := rt.bootstrapAdmin(ctx, admins); err != nil {
		return nil, err
	}

	limiter := auth.NewLoginLimiter(loginAttempts, time.Minute, loginLimiterKeys)
	return httpapi.NewAdminHandler(polls, results, admins, tokens, limiter, time.Now, rt.cfg.PollMinLeadTime)
}

func (rt *runtime) bootstrapAdmin(ctx context.Context, admins *postgres.AdminRepo) error {
	if rt.cfg.AdminBootstrapLogin == "" || rt.cfg.AdminBootstrapPassword == "" {
		return nil
	}

	hash, err := auth.HashPassword(rt.cfg.AdminBootstrapPassword)
	if err != nil {
		return fmt.Errorf("admin password hash: %w", err)
	}

	created, err := admins.EnsureAdmin(ctx, domain.Admin{
		Login:        rt.cfg.AdminBootstrapLogin,
		PasswordHash: hash,
		Role:         string(auth.RoleAdmin),
	})
	if err != nil {
		return fmt.Errorf("create admin: %w", err)
	}
	if created {
		rt.log.Info("admin account created", slog.String("login", rt.cfg.AdminBootstrapLogin))
	}
	return nil
}

func (rt *runtime) datacenterRanges() []netip.Prefix {
	if !rt.cfg.ASNBlockEnabled {
		return nil
	}

	data, err := os.ReadFile(rt.cfg.ASNBlocklistPath)
	if err != nil {
		rt.log.Warn("datacenter network list not read, filter disabled",
			slog.String("path", rt.cfg.ASNBlocklistPath), slog.String("error", err.Error()))
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
			rt.log.Warn("datacenter list line skipped", slog.String("line", line))
			continue
		}
		out = append(out, prefix)
	}

	rt.log.Info("datacenter network filter loaded", slog.Int("prefixes", len(out)))
	return out
}

func adminJWTBytes(raw string) []byte {
	if decoded, err := hex.DecodeString(raw); err == nil && len(decoded) >= 32 {
		return decoded
	}
	sum := sha256.Sum256([]byte("televote-dev-admin-jwt:" + raw))
	return sum[:]
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
