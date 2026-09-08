package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/rueidis"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/adapter/httpapi"
	"github.com/dubter/televote/internal/adapter/postgres"
	"github.com/dubter/televote/internal/adapter/producer"
	"github.com/dubter/televote/internal/domain"
	"github.com/dubter/televote/internal/platform/config"
	"github.com/dubter/televote/internal/platform/health"
	"github.com/dubter/televote/internal/platform/metrics"
	"github.com/dubter/televote/internal/platform/observability"
	"github.com/dubter/televote/internal/service/auth"
	"github.com/dubter/televote/internal/service/capacity"
	"github.com/dubter/televote/internal/service/consumer"
	"github.com/dubter/televote/internal/service/pollcfg"
	"github.com/dubter/televote/internal/service/snapshot"
	"github.com/dubter/televote/internal/service/vote"
)

type Role string

const (
	adminRateLimit      = 120
	lookupRefreshFactor = 3
)

const (
	RoleAPI      Role = "api"
	RoleConsumer Role = "consumer"
	RoleSnapshot Role = "snapshot"
)

func (r Role) servesHTTP() bool { return r == RoleAPI }
func (r Role) consumes() bool   { return r == RoleConsumer }

func (r Role) snapshots() bool { return r == RoleSnapshot }

func (r Role) needsRedis() bool { return r.consumes() || r.snapshots() }

type app struct {
	cfg    *config.Config
	log    *slog.Logger
	role   Role
	router http.Handler

	redis    rueidis.Client
	pgWrite  *pgxpool.Pool
	pgRead   *pgxpool.Pool
	kafka    *kgo.Client
	producer *producer.Producer

	background  sync.WaitGroup
	metrics     *metrics.Metrics
	cache       *pollcfg.Cache
	counting    *consumer.Counting
	snapshotter *snapshot.Snapshotter
	advisor     *capacity.Advisor
}

func buildApp(ctx context.Context, cfg *config.Config, log *slog.Logger, r Role) (*app, error) {
	a := &app{cfg: cfg, log: log, role: r, metrics: metrics.New(prometheus.DefaultRegisterer)}

	if err := a.connectStores(ctx); err != nil {
		a.Close()
		return nil, err
	}
	if err := a.buildDomainServices(ctx); err != nil {
		a.Close()
		return nil, err
	}
	if r.servesHTTP() {
		if err := a.buildHTTP(); err != nil {
			a.Close()
			return nil, err
		}
	}
	return a, nil
}

func (a *app) connectStores(ctx context.Context) error {
	var err error

	if a.pgWrite, err = postgres.NewPool(ctx, a.cfg.PostgresDSN, postgres.WithMaxConns(a.cfg.PostgresMaxConns)); err != nil {
		return fmt.Errorf("postgres (write): %w", err)
	}
	if a.pgRead, err = postgres.NewPool(ctx, a.cfg.PostgresReadDSN, postgres.WithMaxConns(a.cfg.PostgresMaxConns)); err != nil {
		return fmt.Errorf("postgres (read): %w", err)
	}

	if a.role.needsRedis() {
		a.redis, err = rueidis.NewClient(rueidis.ClientOption{
			InitAddress:  a.cfg.RedisAddrs,
			Dialer:       netDialer(a.cfg.RedisDialTimeout),
			ShuffleInit:  true,
			DisableCache: true,
		})
		if err != nil {
			return fmt.Errorf("redis cluster: %w", err)
		}
	}

	if a.role.servesHTTP() {
		a.producer, err = producer.New(producer.Config{
			Brokers:        a.cfg.KafkaBrokers,
			Topic:          a.cfg.KafkaTopic,
			Linger:         a.cfg.KafkaLinger,
			ProduceTimeout: a.cfg.KafkaProduceTimeout,
			Hooks:          observability.KafkaHooks(a.cfg.KafkaConsumerGroup),
		})
		if err != nil {
			return fmt.Errorf("kafka producer: %w", err)
		}
	}

	if a.role.needsRedis() {
		opts := []kgo.Opt{
			kgo.WithHooks(observability.KafkaHooks(a.cfg.KafkaConsumerGroup)...),
			kgo.SeedBrokers(a.cfg.KafkaBrokers...),
			kgo.ConsumerGroup(a.cfg.KafkaConsumerGroup),
			kgo.DisableAutoCommit(),
		}
		if a.role.consumes() {
			opts = append(opts, kgo.ConsumeTopics(a.cfg.KafkaTopic))
		}
		a.kafka, err = kgo.NewClient(opts...)
		if err != nil {
			return fmt.Errorf("kafka: %w", err)
		}
	}
	return nil
}

func (a *app) buildDomainServices(ctx context.Context) error {
	pollsRead, err := postgres.NewPollRepo(a.pgRead)
	if err != nil {
		return fmt.Errorf("poll repository (read): %w", err)
	}

	a.cache, err = pollcfg.NewCache(pollsRead, a.cfg.PollConfigRefresh, pollcfg.WithLogger(a.log))
	if err != nil {
		return fmt.Errorf("poll config cache: %w", err)
	}
	a.cache = a.cache.WithObserver(a.metrics)

	if err := a.cache.Warm(ctx); err != nil {
		return fmt.Errorf("poll config cache warmup: %w", err)
	}

	if !a.role.needsRedis() {
		return nil
	}

	caster, err := vote.NewCaster(a.redis, a.cfg.DedupTTL, a.cfg.DedupTTLJitter, a.cfg.RedisCmdTimeout)
	if err != nil {
		return fmt.Errorf("vote applier: %w", err)
	}

	if a.role.consumes() {
		a.counting, err = consumer.NewCounting(a.kafka, caster, a.cache, a.metrics, a.log, consumer.Config{
			RetryBudget:   a.cfg.VoteRetryBudget,
			LookupBudget:  a.cfg.PollConfigRefresh * lookupRefreshFactor,
			ErrorRatio:    a.cfg.BreakerErrorRatio,
			BreakerWindow: a.cfg.BreakerWindow,
		})
		if err != nil {
			return fmt.Errorf("counting consumer: %w", err)
		}
	}

	if !a.role.snapshots() {
		return nil
	}

	pollsWrite, err := postgres.NewPollRepo(a.pgWrite)
	if err != nil {
		return fmt.Errorf("poll repository (write): %w", err)
	}
	results, err := postgres.NewResultRepo(a.pgWrite)
	if err != nil {
		return fmt.Errorf("result repository: %w", err)
	}

	lag := kafkaLag{a.kafka, a.cfg.KafkaTopic}

	a.snapshotter, err = snapshot.New(caster, results, pollsWrite, lag, snapshot.Config{
		Interval: a.cfg.SnapshotInterval,
		Grace:    a.cfg.SnapshotFinalGrace,
		Log:      a.log,
		Observer: a.metrics,
	})
	if err != nil {
		return fmt.Errorf("snapshotter: %w", err)
	}

	a.advisor, err = capacity.New(pollsRead, lag, capacity.Config{
		DrainWindow: a.cfg.DrainWindow,
		PrewarmLead: a.cfg.PollMinLeadTime,
	})
	if err != nil {
		return fmt.Errorf("capacity advisor: %w", err)
	}
	return nil
}

func (a *app) buildHTTP() error {
	public, err := httpapi.NewPublicHandler(a.cache, a.producer, a.metrics, time.Now, a.cfg.MaxBodyBytes)
	if err != nil {
		return fmt.Errorf("public handler: %w", err)
	}

	admin, err := a.buildAdmin()
	if err != nil {
		return err
	}

	a.router = httpapi.NewRouter(public, admin, httpapi.StaticRoutes(a.cfg.PublicBaseURL), httpapi.RouterConfig{
		TrustedProxies:   a.cfg.TrustedProxies,
		DatacenterRanges: a.datacenterRanges(),
		VoteRateLimit:    a.cfg.RateLimitPerMin,
		AdminRateLimit:   adminRateLimit,
		Logger:           a.log,
		RequestObserver:  a.metrics,
		RateWindow:       time.Minute,
		ServiceName:      a.cfg.OTelServiceName,
	})
	return nil
}

func (a *app) buildAdmin() (*httpapi.AdminHandler, error) {
	polls, err := postgres.NewPollRepo(a.pgWrite)
	if err != nil {
		return nil, fmt.Errorf("poll repository: %w", err)
	}
	results, err := postgres.NewResultRepo(a.pgWrite)
	if err != nil {
		return nil, fmt.Errorf("result repository: %w", err)
	}
	admins, err := postgres.NewAdminRepo(a.pgWrite)
	if err != nil {
		return nil, fmt.Errorf("admin repository: %w", err)
	}

	tokens, err := auth.NewTokenService(adminJWTBytes(a.cfg.AdminJWTKey), a.cfg.AdminJWTTTL)
	if err != nil {
		return nil, fmt.Errorf("token service: %w", err)
	}

	if err := a.bootstrapAdmin(admins); err != nil {
		return nil, err
	}

	limiter := auth.NewLoginLimiter(5, time.Minute, 10_000)
	return httpapi.NewAdminHandler(polls, results, admins, tokens, limiter, time.Now, a.cfg.PollMinLeadTime)
}

//nolint:contextcheck // runs at startup, before any request context exists
func (a *app) bootstrapAdmin(admins *postgres.AdminRepo) error {
	if a.cfg.AdminBootstrapLogin == "" || a.cfg.AdminBootstrapPassword == "" {
		return nil
	}

	hash, err := auth.HashPassword(a.cfg.AdminBootstrapPassword)
	if err != nil {
		return fmt.Errorf("admin password hash: %w", err)
	}

	created, err := admins.EnsureAdmin(context.Background(), domain.Admin{
		Login:        a.cfg.AdminBootstrapLogin,
		PasswordHash: hash,
		Role:         string(auth.RoleAdmin),
	})
	if err != nil {
		return fmt.Errorf("create admin: %w", err)
	}
	if created {
		a.log.Info("admin account created",
			slog.String("login", a.cfg.AdminBootstrapLogin))
	}
	return nil
}

func (a *app) runBackground(ctx context.Context) {
	a.background.Go(func() { a.cache.Run(ctx) })

	if a.counting != nil {
		a.background.Go(func() {
			if err := a.counting.Run(ctx); err != nil {
				a.log.ErrorContext(ctx, "counting consumer stopped", slog.Any("error", err))
			}
		})
	}
	if a.snapshotter != nil {
		a.background.Go(func() { a.snapshotter.Run(ctx) })
	}
}

func (a *app) waitBackground(timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		a.background.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
		a.log.Warn("background workers did not stop in time",
			slog.String("timeout", timeout.String()))
	}
}

func (a *app) readiness() []health.Checker {
	var checks []health.Checker

	if a.role.servesHTTP() {
		if a.producer != nil {
			checks = append(checks, a.producer.Ping)
		}
	} else {
		checks = append(checks, a.pgRead.Ping)
	}

	if a.redis != nil {
		client := a.redis
		checks = append(checks, func(ctx context.Context) error {
			return client.Do(ctx, client.B().Ping().Build()).Error()
		})
	}
	if a.kafka != nil {
		client := a.kafka
		checks = append(checks, func(ctx context.Context) error { return client.Ping(ctx) })
	}
	return checks
}

//nolint:contextcheck // runs after the root context is cancelled: drain has its own deadline
func (a *app) Close() {
	var errs []error

	if a.producer != nil {
		if err := a.producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("producer drain: %w", err))
		}
	}
	if a.kafka != nil {
		a.kafka.Close()
	}
	if a.redis != nil {
		a.redis.Close()
	}
	for _, p := range []*pgxpool.Pool{a.pgRead, a.pgWrite} {
		if p != nil {
			p.Close()
		}
	}

	if err := errors.Join(errs...); err != nil {
		a.log.Error("not all resources were released cleanly", slog.Any("error", err))
	}
}
