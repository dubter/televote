# Televote — план реализации

> **Для агентов:** читай `CLAUDE.md` (инварианты), `docs/design.md` (раздел под свою задачу),
> `docs/design.md` §1 (требования FR/NFR).

**Goal:** приём 30 млн голосов в окне 60 с через Kafka, дедуп и подсчёт консьюмером, результат eventually.

**Spec:** `docs/design.md`

## Global Constraints

- Go 1.26, module `github.com/dubter/televote`
- **go.mod трогает только задача, которая добавляет новую библиотеку**; `go mod tidy` не запускать
- **Никто не делает `git commit`** — параллельные агенты дерутся за `index.lock`
- Зависимости однонаправленные: `domain ← application ← adapters`, проверяет `depguard`
- `ctx` первым, таймаут на каждом внешнем вызове, ошибки типизированные
- Единственная точка маппинга ошибок — `internal/httpapi/errors.go`
- Integration-тесты Redis **только на кластере**; Kafka — через testcontainers
- Приёмочные тесты называются по идентификатору требования: `TestFR4_...`, `TestNFR3_...`
- Перед завершением: `go build ./... && go vet ./... && go test ./<свой пакет>/...`

## Готово

| | Пакет | Состояние |
|---|---|---|
| T1 | `config`, `observability/health`, `cmd/televote` | 25 тестов, зелёное |
| T2 | `domain` | 31 тест, зелёное |

## Граф

```
T1,T2 готовы
  ├─ W1  T3 storage/postgres · T4 vote(Lua) · T5 pollcfg
  ├─ W2  T6 producer · T7 middleware · T8 auth · T9 capacity
  │      gate A
  ├─ W3  T10 httpapi public · T11 consumer/counting · T12 snapshot
  ├─ W4  T13 httpapi admin · T14 consumer/fraud · T15 web
  ├─ W5  T16 observability + wiring
  │      gate B
  ├─ W6  T17 deploy · T18 tests · T19 CI+docs
  └─ W7  финальный прогон: demo, smoke, load
```

---

## T3 · storage/postgres

**Files:** `migrations/0001_init.sql`, `internal/storage/postgres/{pool,polls,results,admins}.go` + integration-тесты

Схема — `design.md` §6. Внимание к новым полям: `expected_audience`, `expected_conversion`, `salt bytea`, и **двум таблицам результатов**.

**Produces:**
```go
func NewPool(ctx context.Context, dsn string) (*pgxpool.Pool, error)   // + otelpgx

type PollRepo struct{ }
func (r *PollRepo) Create(ctx context.Context, p *domain.Poll) error    // транзакция poll+options, генерит salt
func (r *PollRepo) GetBySlug(ctx context.Context, slug string) (*domain.Poll, error)
func (r *PollRepo) ListActive(ctx context.Context) ([]*domain.Poll, error)
func (r *PollRepo) ListUpcoming(ctx context.Context, within time.Duration) ([]*domain.Poll, error)
func (r *PollRepo) Transition(ctx context.Context, id uuid.UUID, to domain.Status, version uint32) error
func (r *PollRepo) HasCountedVotes(ctx context.Context, id uuid.UUID) (bool, error)

type ResultRepo struct{ }
func (r *ResultRepo) Upsert(ctx context.Context, pollID uuid.UUID, a domain.Aggregate) error  // GREATEST
func (r *ResultRepo) Get(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, error)
func (r *ResultRepo) SaveAdjusted(ctx context.Context, pollID uuid.UUID, a domain.Aggregate, excludedNets []string) error
func (r *ResultRepo) GetAdjusted(ctx context.Context, pollID uuid.UUID) (domain.Aggregate, []string, error)

type AdminRepo struct{ }
func (r *AdminRepo) ByLogin(ctx context.Context, login string) (*Admin, error)
func (r *AdminRepo) Audit(ctx context.Context, actor, action, entity string, payload any) error

var ErrSlugTaken, ErrVersionConflict, ErrNotFound error
```

**Шаги**
- [ ] Миграция. `poll_results` и `poll_results_adjusted` **с явным PK** — без него `ON CONFLICT` падает в рантайме
- [ ] Тесты: `TestCreate_PollAndOptionsAtomic`, `TestCreate_DuplicateSlug`, `TestCreate_GeneratesUniqueSalt`, `TestTransition_VersionConflict`, `TestListUpcoming_RespectsHorizon`, `TestUpsert_MonotonicWithGreatest`, `TestUpsert_ConcurrentSnapshottersAreSafe`, `TestSaveAdjusted_DoesNotTouchRawResults`
- [ ] Реализовать, все запросы параметризованные
- [ ] `go test -tags=integration -race`

---

## T4 · vote: Lua, ключи, шардирование, voterID

**Files:** `internal/vote/{keys,voter,script,caster}.go`, `internal/vote/vote.lua` + integration-тесты на **Redis Cluster**

**Produces:**
```go
type VoterID [16]byte
func DeriveVoterID(salt []byte, clientID string) (VoterID, error)   // HMAC-SHA256, обрезка до 16
var ErrBadClientID = errors.New("bad_client_id")                     // пустой, константа, слишком длинный

func ShardFor(v VoterID, shardCount uint16) uint16                   // xxhash64 % shardCount
func HashTag(pollID uuid.UUID, shard uint16) string                  // "p:<id>:s<shard>"
func DedupKey(pollID uuid.UUID, shard uint16, v VoterID) string
func CounterKey(pollID uuid.UUID, shard uint16) string

type Result uint8
const (ResultCounted Result = 1; ResultAlreadyCounted Result = 2)    // НЕ 0: нулевое значение не должно означать успех

type Caster struct{ }
func NewCaster(c rueidis.Client, ttl time.Duration, jitter float64) *Caster
func (c *Caster) Cast(ctx context.Context, pollID uuid.UUID, shardCount uint16, v VoterID, choices []uint8) (Result, error)
func (c *Caster) Aggregate(ctx context.Context, pollID uuid.UUID, shardCount uint16) (domain.Aggregate, error)
func IsRetryable(err error) bool
```

**Шаги**
- [ ] `vote.lua` по `design.md` §4
- [ ] `DeriveVoterID`: клиент присылает вход, ключ выводит сервер. Тесты: `TestDeriveVoterID_SameInputSameOutput`, `TestDeriveVoterID_DifferentSaltsUnlinkable`, `TestDeriveVoterID_RejectsEmptyAndConstant`, `TestDeriveVoterID_BoundsKeyLength`
- [ ] Тесты ключей: `TestDedupAndCounterKeys_ShareHashTag`, `TestShardFor_UniformDistribution` (100k образцов), `TestShardFor_Deterministic`
- [ ] **Харнесс Redis Cluster на testcontainers** — 3 мастера. Одиночный Redis не воспроизводит `CROSSSLOT`, и тесты на нём бесполезны. Опиши в notes, как поднял
- [ ] Тесты: `TestCast_FirstVoteCounted`, `TestCast_SecondIsAlreadyCounted`, `TestNFR3_RetryDoesNotDoubleCount`, `TestCast_ConcurrentSameVoterYieldsOneCounted` (1000 горутин), `TestCast_MultipleIncrementsAllChosen`, `TestCast_NoCrossSlotError`, `TestVote_TTLHasJitter`, `TestAggregate_SumsAllShards`
- [ ] `Result` не имеет нулевого значения — провалившийся вызов не должен выглядеть успехом

---

## T5 · pollcfg: кэш конфига

**Files:** `internal/pollcfg/cache.go` + тесты

**Produces:**
```go
type HotConfig struct {
    ID          uuid.UUID
    Slug        string
    Question    string          // нужен публичному GET конфига
    Options     []domain.Option
    Rules       domain.ChoiceRules
    Window      domain.Window
    ShardCount  uint16
    Salt        []byte
}
type Repo interface { ListActive(context.Context) ([]*domain.Poll, error) }

type Cache struct{ }   // atomic.Pointer на неизменяемую карту
func NewCache(r Repo, interval time.Duration) *Cache
func (c *Cache) Warm(ctx context.Context) error        // ДО readiness
func (c *Cache) Run(ctx context.Context)               // фоновый рефрешер
func (c *Cache) BySlug(slug string) (*HotConfig, bool) // только память
func (c *Cache) LastRefresh() time.Time                // через atomic, не через поле
```

**Шаги**
- [ ] Тесты: `TestBySlug_ReturnsWarmedConfig`, `TestRun_PicksUpNewPoll`, `TestRun_KeepsStaleConfigWhenRepoFails`, `TestWarm_ErrorsWhenRepoFails`, `TestPollCfg_NoIOOnHotPath` (Repo не вызывается при чтении), `TestCache_RaceFree` (`-race`, 100 читателей)
- [ ] Фоновый рефрешер, **не ленивый TTL**: истечение при 2M RPS даёт thundering herd
- [ ] `HotConfig` содержит всё, что нужно и приёму, и публичному конфигу

---

## T6 · producer: отправка в Kafka

**Files:** `internal/producer/producer.go` + тесты на testcontainers Kafka. Добавляет `twmb/franz-go` в go.mod.

**Produces:**
```go
type VoteMessage struct {
    PollID   uuid.UUID `json:"p"`
    VoterID  string    `json:"v"`   // hex
    Choices  []uint8   `json:"c"`
    Net16    string    `json:"n"`   // /16 подсеть для fraud-анализа, НЕ полный IP
    UAClass  string    `json:"u"`   // "iOS 18" — класс, не User-Agent
    ProducedAt time.Time `json:"t"` // серверная метка, по ней проверяется окно
}

type Producer struct{ }
func New(brokers []string, topic string, cfg Config) (*Producer, error)
func (p *Producer) Send(ctx context.Context, m VoteMessage) error   // ключ партиции = VoterID
func (p *Producer) Close() error
```

**Шаги**
- [ ] Тесты: `TestSend_PartitionKeyIsVoterID`, `TestSend_SetsProducedAt`, `TestSend_ErrorsWhenBrokerDown`, `TestVoteMessage_ContainsNoRawIP` (в JSON нет полного адреса и User-Agent)
- [ ] `acks=all`, батчинг с linger, идемпотентный продюсер
- [ ] Последний тест обязателен: сообщение — это то, что живёт в Kafka на время retention, и полный IP там означал бы утечку

---

## T7 · middleware

**Files:** `internal/httpapi/{clientip,ratelimit,asn}.go` + тесты. В этом пакете ничего больше не создавать.

**Produces:**
```go
func ClientIP(trusted []netip.Prefix) func(http.Handler) http.Handler
func IPFromContext(ctx context.Context) netip.Addr
func LimitKey(a netip.Addr) string            // IPv4 → /32, IPv6 → /64
func Net16(a netip.Addr) string               // для fraud-агрегатов
func RateLimit(perMin, burst, maxKeys int) func(http.Handler) http.Handler
func BlockDatacenterASN(ranges []netip.Prefix) func(http.Handler) http.Handler
func Recovery(l *slog.Logger) func(http.Handler) http.Handler
func UAClass(userAgent string) string         // "iOS 18", "Android 14" — грубый класс
```

**Шаги**
- [ ] Тесты: `TestClientIP_TrustsProxyHop`, `TestNFR9_ForgedXFFIgnored`, `TestNFR9_IPv6LimitedByPrefix` (два адреса одной /64 → один ключ), `TestRateLimit_TableIsBounded`, `TestBlockDatacenterASN_Returns403`, `TestUAClass_CollapsesMinorVersions`
- [ ] Три ошибки из `CLAUDE.md`: лимит частоты (не количества), по префиксу /64, XFF только от доверенных

---

## T8 · auth

**Files:** `internal/auth/{password,jwt,rbac}.go` + тесты

**Produces:**
```go
func HashPassword(plain string) (string, error)          // argon2id
func VerifyPassword(hash, plain string) (bool, error)

type Role string   // "admin" | "editor" | "viewer"
func (r Role) AtLeast(min Role) bool                     // явный порядок ролей

type Claims struct{ Sub uuid.UUID; Role Role; jwt.RegisteredClaims }
type TokenService struct{ }
func NewTokenService(key []byte, ttl time.Duration) (*TokenService, error)
func (t *TokenService) Issue(userID uuid.UUID, role Role) (string, error)
func (t *TokenService) Parse(raw string) (*Claims, error)
func RequireRole(t *TokenService, min Role) func(http.Handler) http.Handler
type LoginLimiter struct{ }                              // лимит попыток: argon2id усиливает DoS
func (l *LoginLimiter) Allow(login string) bool
```

**Шаги**
- [ ] Тесты: `TestHashVerify_RoundTrip`, `TestVerify_WrongPassword`, `TestParse_ExpiredToken`, `TestParse_TamperedSignature`, `TestRole_AtLeastOrdering`, `TestFR7_ViewerCannotWrite`, `TestLoginLimiter_BlocksAfterN`
- [ ] JWT в заголовке, не в cookie — снимает вопрос CSRF

---

## T9 · capacity

**Files:** `internal/domain/capacity.go`, `internal/capacity/advisor.go` + тесты

**Produces:**
```go
// domain — чистая функция, табличный тест
type Capacity struct{ VoteAPI, Consumers, RedisMasters, KafkaPartitions int }
func CapacityFor(expectedVotes int64, drainWindow time.Duration) Capacity

type Phase string  // "idle" | "prewarm" | "live" | "drain"

// capacity — HTTP-шов для KEDA, никаких вызовов k8s API
type Advisor struct{ }
func NewAdvisor(polls PollLister, lag LagReader) *Advisor
func (a *Advisor) Handler() http.HandlerFunc      // GET /internal/capacity
```

**Шаги**
- [ ] Тесты `CapacityFor`: 30 млн за 5 мин → 8 мастеров; 30 млн за 60 с → 32; монотонность по объёму; ноль голосов → базовая линия, не ноль
- [ ] Тесты фаз: `idle` дальше горизонта, `prewarm` ступенями, `live` при open, `drain` при `lag > 0`, обратно в `idle` при `lag == 0`
- [ ] Ёмкость выводится из `expected_audience` опроса, не из константы

---

## Gate A

- [ ] `go build ./...`, `go vet ./...`, `go test -race ./...` — починить
- [ ] Сверить фактические сигнатуры между пакетами с блоками Produces
- [ ] **Не ослаблять тесты ради зелёного.** Что осталось сломанным — в `remaining`

---

## T10 · httpapi public

**Files:** `internal/httpapi/{router,public,errors}.go` + тесты. Файлы `clientip.go`, `ratelimit.go`, `asn.go` уже есть.

**Produces:**
```go
type PublicHandler struct{ }
func NewPublicHandler(cache *pollcfg.Cache, prod *producer.Producer, now func() time.Time) *PublicHandler
func (h *PublicHandler) Routes() chi.Router
func WriteError(w http.ResponseWriter, r *http.Request, err error)   // ЕДИНСТВЕННАЯ точка маппинга
```

Маршруты: `POST /api/v1/polls/{slug}/vote`, `GET /api/v1/polls/{slug}`.

**Шаги**
- [ ] Тесты по таблице `design.md` §7: `TestFR3_VoteWithoutRegistration` (202), `TestFR3_RejectVoteWithoutToken` → тут: пустой `voter` → 400, `TestFR1_2_RejectBelowMinChoices`, `TestFR1_2_RejectAboveMaxChoices`, `TestFR8_VoteBeforeOpensAt`, `TestFR8_VoteAfterClosesAt`, `TestNFR9_BodySizeLimitEnforced`, `TestVote_KafkaDownReturns503`, `TestPollConfig_ContainsServerTime`, `TestPollConfig_HasCacheControlHeader`, `TestNFR9_SecurityHeadersPresent`
- [ ] `errors.go` — один `switch`, доменная ошибка → код
- [ ] Приём: rate limit → ASN → тело ≤1 КБ → cfg из кэша → валидация → `DeriveVoterID` → `producer.Send` → **202**
- [ ] Ни Redis, ни Postgres на этом пути

---

## T11 · consumer/counting

**Files:** `internal/consumer/counting.go` + integration-тесты (Kafka + Redis Cluster)

**Produces:**
```go
type Counting struct{ }
func NewCounting(client *kgo.Client, caster *vote.Caster, cache *pollcfg.Cache, m *observability.Metrics) *Counting
func (c *Counting) Run(ctx context.Context) error
```

**Шаги**
- [ ] Тесты: `TestCounting_AppliesVote`, `TestCounting_DuplicateDeliveryDoesNotDoubleCount` (переиграть сообщение), `TestCounting_RejectsVoteProducedAfterClosesAt` (окно по метке produce, **не** по времени обработки), `TestCounting_RetriesOnRedisError`, `TestCounting_CommitsOffsetOnlyAfterApply`
- [ ] Второй тест — про at-least-once: без идемпотентности Lua ребаланс завысил бы результат
- [ ] Оффсет коммитится **после** применения, иначе падение теряет голоса

---

## T12 · snapshot

**Files:** `internal/snapshot/snapshotter.go` + integration-тесты

**Produces:**
```go
type Snapshotter struct{ }
func NewSnapshotter(c *vote.Caster, rr *postgres.ResultRepo, pr *postgres.PollRepo,
                    lag LagReader, interval, grace time.Duration, now func() time.Time) *Snapshotter
func (s *Snapshotter) Run(ctx context.Context)
func (s *Snapshotter) TickOnce(ctx context.Context, p *domain.Poll) (domain.Aggregate, error)
func (s *Snapshotter) Finalize(ctx context.Context, p *domain.Poll, excludedNets []string) error
```

**Шаги**
- [ ] Тесты: `TestTickOnce_SumsShardsIntoPostgres`, `TestTickOnce_IsIdempotent`, `TestNFR3_SnapshotIsMonotonic` (Redis обнулился → Postgres не откатился), `TestFR6_ScheduledOpensAutomatically`, `TestFinalize_WaitsForZeroLag`, `TestFinalize_AppliesExclusionsToAdjustedOnly`, `TestSnapshotter_TwoInstancesAreSafe`
- [ ] `Run` попутно открывает опросы: `domain.ShouldOpenAt` — переход `scheduled → open` по расписанию
- [ ] `Finalize` ждёт **`lag == 0`**, а не таймаут: это критерий, что все голоса доехали
- [ ] Инъекция времени через `now func() time.Time`, иначе `Finalize` нечем протестировать

---

## T13 · httpapi admin

**Files:** `internal/httpapi/admin.go` + тесты. `errors.go` уже есть — использовать `WriteError`.

Маршруты: `POST /admin/login`, `POST /admin/polls`, `GET /admin/polls`, `POST …/{id}/open`, `POST …/{id}/close`, `GET …/{id}/results`, `GET …/{id}/fraud`, `POST …/{id}/exclude`.

**Шаги**
- [ ] Тесты: `TestFR7_AdminEndpointRequiresJWT`, `TestFR1_CreateSingleChoicePoll`, `TestFR1_CreateMultipleChoicePoll`, `TestFR1_RejectUnknownPollType`, `TestFR1_RejectEmptyOptions`, `TestCreate_RejectsOpensAtTooSoon` (меньше часа — Kafka не успеет), `TestFR6_RejectIllegalTransition`, `TestUpdateOptions_ForbiddenAfterFirstVote`, `TestFR5_ResultsReturnAggregateOnly`, `TestFR5_PercentagesAreOfBallotsTotal`, `TestFR6_ExtendClosesAtIsAudited`, `TestExclude_PreviewsDeltaAndAudits`
- [ ] Результаты отдаются с прогрессом дренажа: `{"processed_pct": 87, "final": false}`
- [ ] `POST /exclude` — явное действие с предпросмотром дельты и записью в `admin_audit`. Автоматического порога нет

---

## T14 · consumer/fraud

**Files:** `internal/consumer/fraud.go` + тесты

**Produces:**
```go
type Fraud struct{ }
func NewFraud(client *kgo.Client, redis rueidis.Client, sample float64) *Fraud
func (f *Fraud) Run(ctx context.Context) error
type Signals struct {
    ByNet16   map[string]int64
    ArrivalCurve []int64        // голосов по секунде от начала окна
    UAClasses map[string]int64
}
func (f *Fraud) Read(ctx context.Context, pollID uuid.UUID) (Signals, error)
```

**Шаги**
- [ ] Тесты: `TestFraud_AggregatesByNet16`, `TestFraud_BuildsArrivalCurve`, `TestNFR4_AnomalyKeysAggregateOnly` (в ключах нет ни `voterID`, ни полного IP), `TestFraud_SeparateConsumerGroup` (не влияет на оффсеты counting), `TestFraud_FailureDoesNotAffectCounting`
- [ ] **Отдельный `group.id`.** Смысл именно в изоляции: анализ не может замедлить или сломать подсчёт
- [ ] Только агрегаты. Отдельные голоса не сохраняются

---

## T15 · web + QR

**Files:** `web/vote.html`, `web/admin.html`, `internal/httpapi/static.go`, `internal/httpapi/qr.go` + тесты

**Важно:** `//go:embed` не выходит за пределы своего каталога. Либо `web/` внутри `internal/httpapi/`, либо `embed.go` в корне с передачей `fs.FS` внутрь. Выбери и объясни в notes.

Страница голосования: один документ без внешних ресурсов, **≤8 КБ gzip** — 30 млн загрузок, и лишний КБ равен 30 ГБ трафика.

```js
let voter = localStorage.getItem('v');          // localStorage, НЕ sessionStorage
if (!voter) { voter = crypto.randomUUID(); localStorage.setItem('v', voter); }
```

**Шаги**
- [ ] Тесты: `TestFR2_ShortLinkServesVotingPage`, `TestFR2_QREncodesPublicVotingURL`, `TestVotePage_UnderEightKilobytesGzipped`, `TestVotePage_UsesLocalStorageNotSession` (грепом по содержимому)
- [ ] Все обращения к `localStorage` в `try/catch` — в приватном Safari бросают
- [ ] Работа по `server_time`, проверка на `pageshow`, осмысленный `<noscript>`
- [ ] Ретрая с джиттером **больше нет** — Kafka принимает с первой попытки
- [ ] Админка: логин, список, создание, результаты с прогрессом, панель сигналов накрутки с кнопкой исключения

---

## T16 · observability + wiring

**Files:** `internal/observability/{otel,metrics}.go`, `cmd/televote/main.go` — **единственный агент, который правит main.go**

**Produces:**
```go
func Setup(ctx context.Context, cfg Config) (shutdown func(context.Context) error, err error)
type Metrics struct{ }
func NewMetrics(m metric.Meter) (*Metrics, error)
func (m *Metrics) VoteAccepted(ctx context.Context)
func (m *Metrics) VoteRejected(ctx context.Context, reason string)
func (m *Metrics) VoteCounted(ctx context.Context, result vote.Result)
func (m *Metrics) ConsumerLag(ctx context.Context, n int64)
func (m *Metrics) ProduceLatency(ctx context.Context, d time.Duration)
```

**Шаги**
- [ ] Тест `TestNFR7_MetricLabelsAreBounded`: набор `reason` конечен и не содержит пользовательских данных
- [ ] Тест `TestNFR7_MetricsExposeBusinessCounters`, `TestNFR7_ReadyzChecksDependencies`
- [ ] Сэмплирование трейсов `ParentBased(TraceIDRatioBased(0.0001))`, **разрешение метрик 5 с** (дефолтные 60 дадут одну точку на всё окно)
- [ ] Инструментация: `otelchi`, `rueidisotel`, `otelpgx`, ручные спаны на produce и consume
- [ ] `main.go`: config → observability → pgxpool (primary + read) → rueidis cluster → `pollcfg.Warm` до readiness → producer → counting и fraud консьюмеры → snapshotter → advisor → router → graceful shutdown с дренажом продюсера
- [ ] Роли процесса через флаг: `--role=api|consumer|all` — в проде это разные деплойменты, в стенде `all`

---

## Gate B

- [ ] Полная сборка, `go vet`, `go test -race ./...`, `gofmt -w -s .`
- [ ] `golangci-lint run` если установлен
- [ ] **Для каждой строки таблицы инвариантов `CLAUDE.md` найти в коде место, которое её обеспечивает.** Отсутствующие — в `remaining`

---

## T17 · deploy

**Files:** `deploy/docker-compose.yml`, `deploy/{nginx.conf,redis.conf,kafka.env,otel-config.yaml}`, `deploy/grafana/`, `scripts/{wait-ready.sh,seed-demo.sh}`

Состав — `design.md` §18.

`redis.conf` обязателен именно такой: `appendonly yes`, `appendfsync everysec`, `save ""`, `no-appendfsync-on-rewrite yes`, `auto-aof-rewrite-percentage 0`, **`maxmemory-policy noeviction`**, `maxmemory` задан явно, `cluster-enabled yes`, `cluster-require-full-coverage no`, `cluster-node-timeout 5000`.

**Шаги**
- [ ] Kafka в KRaft, один брокер, без ZooKeeper; `kafka-init` создаёт топик с N партициями
- [ ] Redis: 6 нод + `redis-init` с `--cluster-yes --cluster-replicas 1`
- [ ] Порядок старта: healthcheck у postgres и redis, `redis-init`/`kafka-init` через `service_healthy`, `migrate` через `service_completed_successfully`, `vote-api` после обоих
- [ ] Два инстанса `vote-api` за nginx плюс прямые порты для smoke; nginx проставляет `X-Real-IP`
- [ ] Все хост-порты через `${VAR:-default}`
- [ ] `otel-lgtm` + provisioning четырёх дашбордов из §13
- [ ] `wait-ready.sh` ждёт `/readyz` обоих инстансов; `seed-demo.sh` создаёт опрос и печатает ссылки
- [ ] Проверить `docker compose config`

---

## T18 · smoke, chaos, load

**Files:** `scripts/smoke.sh`, `scripts/chaos/{redis-master,consumer,postgres,kafka-broker}.sh`, `load/k6/{vote.js,summary.js}`

**Шаги**
- [ ] `smoke.sh` — семь шагов из `design.md` §18. Пятый принципиален: **после `lag == 0` в результатах ровно один голос**, хотя отправляли с двух инстансов
- [ ] Каждый chaos-сценарий заканчивается **сверкой агрегата**, а не «сервис отвечает»:
      `redis-master` (дренаж замедлился, сумма сходится), `consumer` (ребаланс не удвоил),
      `postgres` (приём и подсчёт идут), `kafka-broker` (лидер переехал, ни одного 503)
- [ ] k6: один `POST /vote` на виртуального пользователя. В `teardown` — **сумма счётчиков после дренажа равна числу `202` минус дубли**
- [ ] Собрать стоимость одного голоса: CPU-мс на под, байт по сети, аллокаций, команд в Redis
- [ ] **Отключать rate limit в профиле нагрузки**, иначе тест померит лимитер
- [ ] `set -euo pipefail`, ненулевой код при провале, `chmod +x`

---

## T19 · CI + документация

**Files:** `.github/workflows/ci.yml` (дополнить), `README.md`, `ARCHITECTURE.md`, `docs/ai/README.md`

**Шаги**
- [ ] CI: интеграционные тесты поднимают **кластер** Redis и Kafka через testcontainers, а не `services:` с одиночным Redis
- [ ] `README.md`: `make demo` первой командой, требования к машине, что доказывает каждая проверка, структура репозитория
- [ ] `ARCHITECTURE.md`: самостоятельный документ. Раскрыть — почему в окне только приём; почему Kafka; почему автоскейлинг неприменим; почему self-hosted Redis; почему нет fingerprint; почему нет аудит-трейла и чем платим. Трейд-оффы таблицей
- [ ] `docs/ai/README.md`: навигация по артефактам, **включая места, где решения менялись** — буфер голосов оказался ошибкой, конфиг в Redis лишним, оценка стоимости `fork` неверно откалибрована, две ручки схлопнулись в одну, fingerprint отвергнут дважды. Честная история решений ценнее приглаженной

---

## W7 · финальный прогон

- [ ] `cp -n .env.example .env`, `go build ./...`, `go test -race ./...`
- [ ] `make demo` — поднять стенд, дать Kafka и Redis время на инициализацию
- [ ] Руками через curl: создать опрос → проголосовать → дождаться дренажа → результаты
- [ ] `make smoke` — все семь шагов
- [ ] `make load` — отчёт со стоимостью голоса, записать в `docs/load-report.md` с указанием железа
- [ ] `make chaos` — четыре сценария
- [ ] Чинить код, не тесты. `make down`
