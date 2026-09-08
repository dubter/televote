package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/rueidis"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/dubter/televote/internal/auth"
	"github.com/dubter/televote/internal/capacity"
	"github.com/dubter/televote/internal/config"
	"github.com/dubter/televote/internal/consumer"
	"github.com/dubter/televote/internal/httpapi"
	"github.com/dubter/televote/internal/pollcfg"
	"github.com/dubter/televote/internal/producer"
	"github.com/dubter/televote/internal/snapshot"
	"github.com/dubter/televote/internal/storage/postgres"
	"github.com/dubter/televote/internal/vote"
	"github.com/dubter/televote/pkg/health"
)

// role — что делает процесс. В проде это разные деплойменты: приём
// масштабируется под окно эфира, консьюмеры — под дренаж, и их графики
// нагрузки не совпадают. В стенде одного процесса достаточно.
type role string

const (
	roleAPI      role = "api"      // только приём голосов и админка
	roleConsumer role = "consumer" // только подсчёт и снапшоты
	roleAll      role = "all"      // всё сразу, для локального стенда
)

func (r role) servesHTTP() bool { return r == roleAPI || r == roleAll }
func (r role) consumes() bool   { return r == roleConsumer || r == roleAll }

// app — собранное приложение. Поля здесь только те, у которых есть жизненный
// цикл: их нужно закрыть, проверить в /readyz или запустить в фоне.
type app struct {
	cfg    *config.Config
	log    *slog.Logger
	role   role
	router http.Handler

	redis      rueidis.Client
	pgWrite    *pgxpoolWrapper
	pgRead     *pgxpoolWrapper
	kafka      *kgo.Client
	fraudKafka *kgo.Client
	producer   *producer.Producer

	cache       *pollcfg.Cache
	counting    *consumer.Counting
	fraud       *consumer.Fraud
	snapshotter *snapshot.Snapshotter
	advisor     *capacity.Advisor
}

// buildApp собирает зависимости в порядке «от внешних к внутренним».
//
// Каждый шаг возвращает ошибку наверх, а не логирует и продолжает: инстанс,
// стартовавший без Kafka или без Redis, выглядит живым и молча теряет голоса.
func buildApp(ctx context.Context, cfg *config.Config, log *slog.Logger, r role) (*app, error) {
	a := &app{cfg: cfg, log: log, role: r}

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

	if a.pgWrite, err = openPool(ctx, a.cfg.PostgresDSN, a.cfg.PostgresMaxConns); err != nil {
		return fmt.Errorf("postgres (запись): %w", err)
	}
	// Конфиг опросов читается с реплики: failover primary блокирует только
	// админку, а приём голосов его не замечает.
	if a.pgRead, err = openPool(ctx, a.cfg.PostgresReadDSN, a.cfg.PostgresMaxConns); err != nil {
		return fmt.Errorf("postgres (чтение): %w", err)
	}

	if a.role.consumes() {
		a.redis, err = rueidis.NewClient(rueidis.ClientOption{
			InitAddress:  a.cfg.RedisAddrs,
			Dialer:       netDialer(a.cfg.RedisDialTimeout),
			ShuffleInit:  true,
			DisableCache: true, // счётчики меняются постоянно, клиентский кэш вреден
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
		})
		if err != nil {
			return fmt.Errorf("kafka producer: %w", err)
		}
	}

	if a.role.consumes() {
		a.kafka, err = kgo.NewClient(
			kgo.SeedBrokers(a.cfg.KafkaBrokers...),
			kgo.ConsumeTopics(a.cfg.KafkaTopic),
			kgo.ConsumerGroup(a.cfg.KafkaConsumerGroup),
			// Оффсет коммитится вручную ПОСЛЕ применения голоса: автокоммит
			// вперёд терял бы голоса при падении между коммитом и записью.
			kgo.DisableAutoCommit(),
		)
		if err != nil {
			return fmt.Errorf("kafka consumer: %w", err)
		}
	}
	return nil
}

func (a *app) buildDomainServices(ctx context.Context) error {
	pollsRead, err := postgres.NewPollRepo(a.pgRead.pool)
	if err != nil {
		return fmt.Errorf("репозиторий опросов (чтение): %w", err)
	}

	a.cache, err = pollcfg.NewCache(pollsRead, a.cfg.PollConfigRefresh, pollcfg.WithLogger(a.log))
	if err != nil {
		return fmt.Errorf("кэш конфигов: %w", err)
	}
	// Прогрев ДО открытия трафика: инстанс с пустым кэшем ответил бы 404
	// на живой опрос.
	if err := a.cache.Warm(ctx); err != nil {
		return fmt.Errorf("прогрев кэша конфигов: %w", err)
	}

	if !a.role.consumes() {
		return nil
	}

	caster, err := vote.NewCaster(a.redis, a.cfg.DedupTTL, a.cfg.DedupTTLJitter)
	if err != nil {
		return fmt.Errorf("применение голосов: %w", err)
	}

	a.counting, err = consumer.NewCounting(a.kafka, caster, a.cache, nil, a.log)
	if err != nil {
		return fmt.Errorf("консьюмер подсчёта: %w", err)
	}

	pollsWrite, err := postgres.NewPollRepo(a.pgWrite.pool)
	if err != nil {
		return fmt.Errorf("репозиторий опросов (запись): %w", err)
	}
	results, err := postgres.NewResultRepo(a.pgWrite.pool)
	if err != nil {
		return fmt.Errorf("репозиторий результатов: %w", err)
	}

	lag := kafkaLag{a.kafka, a.cfg.KafkaTopic}

	a.snapshotter, err = snapshot.New(caster, results, pollsWrite, lag, snapshot.Config{
		Interval: a.cfg.SnapshotInterval,
		Grace:    a.cfg.SnapshotFinalGrace,
		Log:      a.log,
	})
	if err != nil {
		return fmt.Errorf("снапшотер: %w", err)
	}

	// Анализ накрутки читает тот же топик ОТДЕЛЬНОЙ группой: общая забирала бы
	// сообщения у подсчёта, потому что Kafka делит партиции между членами группы.
	fraudClient, err := kgo.NewClient(
		kgo.SeedBrokers(a.cfg.KafkaBrokers...),
		kgo.ConsumeTopics(a.cfg.KafkaTopic),
		kgo.ConsumerGroup(a.cfg.KafkaFraudGroup),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return fmt.Errorf("kafka consumer (анализ): %w", err)
	}
	a.fraudKafka = fraudClient

	a.fraud, err = consumer.NewFraud(fraudClient, a.redis, a.log, int64(a.cfg.DedupTTL.Seconds()))
	if err != nil {
		return fmt.Errorf("консьюмер анализа: %w", err)
	}

	a.advisor, err = capacity.New(pollsRead, lag, capacity.Config{
		DrainWindow: a.cfg.DrainWindow,
		PrewarmLead: a.cfg.PollMinLeadTime,
	})
	if err != nil {
		return fmt.Errorf("советчик ёмкости: %w", err)
	}
	return nil
}

func (a *app) buildHTTP() error {
	public, err := httpapi.NewPublicHandler(a.cache, a.producer, time.Now)
	if err != nil {
		return fmt.Errorf("публичный обработчик: %w", err)
	}

	admin, err := a.buildAdmin()
	if err != nil {
		return err
	}

	a.router = httpapi.NewRouter(public, admin, httpapi.StaticRoutes(a.cfg.PublicBaseURL), httpapi.RouterConfig{
		TrustedProxies:   a.cfg.TrustedProxies,
		DatacenterRanges: a.datacenterRanges(),
		VoteRateLimit:    a.cfg.RateLimitPerMin,
		RateWindow:       time.Minute,
		ServiceName:      a.cfg.OTelServiceName,
	})
	return nil
}

func (a *app) buildAdmin() (*httpapi.AdminHandler, error) {
	polls, err := postgres.NewPollRepo(a.pgWrite.pool)
	if err != nil {
		return nil, fmt.Errorf("репозиторий опросов: %w", err)
	}
	results, err := postgres.NewResultRepo(a.pgWrite.pool)
	if err != nil {
		return nil, fmt.Errorf("репозиторий результатов: %w", err)
	}
	admins, err := postgres.NewAdminRepo(a.pgWrite.pool)
	if err != nil {
		return nil, fmt.Errorf("репозиторий администраторов: %w", err)
	}

	tokens, err := auth.NewTokenService(adminJWTBytes(a.cfg.AdminJWTKey), a.cfg.AdminJWTTTL)
	if err != nil {
		return nil, fmt.Errorf("сервис токенов: %w", err)
	}

	if err := a.bootstrapAdmin(admins); err != nil {
		return nil, err
	}

	limiter := auth.NewLoginLimiter(5, time.Minute, 10_000)
	return httpapi.NewAdminHandler(polls, results, admins, tokens, limiter, time.Now, a.cfg.PollMinLeadTime)
}

// bootstrapAdmin заводит первую учётную запись, если её ещё нет.
//
//nolint:contextcheck // выполняется на старте, до появления контекста запроса
func (a *app) bootstrapAdmin(admins *postgres.AdminRepo) error {
	if a.cfg.AdminBootstrapLogin == "" || a.cfg.AdminBootstrapPassword == "" {
		return nil
	}

	hash, err := auth.HashPassword(a.cfg.AdminBootstrapPassword)
	if err != nil {
		return fmt.Errorf("хэш пароля администратора: %w", err)
	}

	created, err := admins.EnsureAdmin(context.Background(), postgres.Admin{
		Login:        a.cfg.AdminBootstrapLogin,
		PasswordHash: hash,
		Role:         string(auth.RoleAdmin),
	})
	if err != nil {
		return fmt.Errorf("создание администратора: %w", err)
	}
	if created {
		a.log.Info("создана учётная запись администратора",
			slog.String("login", a.cfg.AdminBootstrapLogin))
	}
	return nil
}

// runBackground поднимает фоновые задачи роли.
func (a *app) runBackground(ctx context.Context) {
	go a.cache.Run(ctx)

	if a.counting != nil {
		go func() {
			if err := a.counting.Run(ctx); err != nil {
				a.log.ErrorContext(ctx, "консьюмер подсчёта остановлен", slog.Any("error", err))
			}
		}()
	}
	if a.fraud != nil {
		go func() {
			if err := a.fraud.Run(ctx); err != nil {
				a.log.ErrorContext(ctx, "консьюмер анализа остановлен", slog.Any("error", err))
			}
		}()
	}
	if a.snapshotter != nil {
		go a.snapshotter.Run(ctx)
	}
}

// readiness — проверки для /readyz.
//
// Проверки настоящие: под, отвечающий 200 из воздуха, встанет в балансировку
// и начнёт отдавать ошибки на голосах.
func (a *app) readiness() []health.Checker {
	checks := []health.Checker{a.pgRead.checker()}

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

// Close освобождает ресурсы в порядке, обратном захвату.
//
//nolint:contextcheck // вызывается после отмены корневого контекста: дренаж
func (a *app) Close() {
	var errs []error

	if a.producer != nil {
		if err := a.producer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("дренаж продюсера: %w", err))
		}
	}
	for _, c := range []*kgo.Client{a.kafka, a.fraudKafka} {
		if c != nil {
			c.Close()
		}
	}
	if a.redis != nil {
		a.redis.Close()
	}
	for _, p := range []*pgxpoolWrapper{a.pgRead, a.pgWrite} {
		if p != nil {
			p.close()
		}
	}

	if err := errors.Join(errs...); err != nil {
		a.log.Error("не все ресурсы освобождены штатно", slog.Any("error", err))
	}
}
