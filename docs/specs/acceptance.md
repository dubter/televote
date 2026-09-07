# Приёмочная спецификация

Требования из `docs/requirements.md`, переписанные как исполняемые сценарии.
Документ — источник истины: тест, которого здесь нет, не доказывает требование.

## Как это работает

**Имя теста несёт идентификатор требования.** Трассировка не поддерживается руками,
а вычисляется:

```bash
make verify-requirements     # грепает имена тестов, сверяет с этим файлом,
                             # печатает непокрытые требования и падает, если такие есть
```

Соглашение об именах — обязательное:

```
TestFR4_DedupWorksAcrossInstances
TestNFR3_SumOfCountersEqualsAcceptedResponses
     ^^^ идентификатор требования, дальше утверждение
```

Требование без теста с его префиксом считается непокрытым. Автоматически, без ревью.

## Уровни

| Уровень | Где | Что доказывает | Тег |
|---|---|---|---|
| unit | рядом с кодом | правила домена, крипто, ключи | — |
| integration | рядом с кодом | Redis Cluster, Postgres | `integration` |
| acceptance | `test/acceptance/` | требование целиком, через HTTP | `acceptance` |
| chaos | `scripts/chaos/` | NFR-5, с проверкой агрегата | — |
| load | `load/k6/` | NFR-1, NFR-3 | — |

---

## Функциональные требования

### FR-1 · Создать опрос: один вопрос, N вариантов, тип single или multiple

```
TestFR1_CreateSingleChoicePoll         дано валидный запрос → создан, slug уникален
TestFR1_CreateMultipleChoicePoll       дано min=2 max=3 → создан
TestFR1_RejectUnknownPollType          дано type="ranking" → 400
TestFR1_RejectEmptyOptions             дано options=[] → 400
TestFR1_1_ABIsSingleWithTwoOptions     дано две опции, single → голос принят
TestFR1_2_RejectBelowMinChoices        дано min=2, выбран 1 → 400
TestFR1_2_RejectAboveMaxChoices        дано max=2, выбрано 3 → 400
```

### FR-2 · Опрос доступен по короткой ссылке и QR

```
TestFR2_ShortLinkServesVotingPage      GET /p/{slug} → 200, text/html
TestFR2_QREncodesPublicVotingURL       GET /p/{slug}/qr.png → PNG, декодируется в тот же URL
```

### FR-3 · Проголосовать анонимно, без регистрации

```
TestFR3_VoteWithoutRegistration        ballot → vote → 200 counted
TestFR3_RejectVoteWithoutToken         vote без токена → 401
TestFR3_RejectForgedToken              подпись испорчена → 401
TestFR3_RejectTokenFromAnotherPoll     токен опроса A в опрос B → 401
```

### FR-4 · Дедупликация уровня «обычный пользователь»

```
TestFR4_SecondVoteSameTokenIsAlreadyCounted    второй голос → 200 already_counted
TestFR4_CounterDoesNotGrowOnDuplicate          счётчик остался прежним
TestFR4_DedupWorksAcrossInstances              ← ключевой: токен выдан app-1, голос на app-1,
                                                 повтор на app-2 → already_counted
TestFR4_ConcurrentSameVoterYieldsExactlyOne    1000 горутин, ровно один counted
```

`TestFR4_DedupWorksAcrossInstances` — прямое доказательство того, ради чего дедуп унесли
в Redis. Требует двух инстансов, живёт в acceptance.

### FR-5 · Обезличенные результаты в админке

```
TestFR5_ResultsReturnAggregateOnly     ответ содержит счётчики, но ни одного voterID
TestFR5_PercentagesAreOfBallotsTotal   multiple: сумма голосов > числа бюллетеней,
                                       проценты считаются от бюллетеней
TestFR5_ResultsRequireAuth             без JWT → 401
```

### FR-6 · Управление окном голосования

```
TestFR6_ScheduledOpensAutomatically    ← находка ревью: scheduled → open по расписанию
TestFR6_ManualCloseTakesEffect         close → следующий голос 409
TestFR6_RejectIllegalTransition        closed → open отвергается FSM
TestFR6_ExtendClosesAtIsAudited        продление пишет запись в admin_audit
```

### FR-7 · Аутентификация админки

```
TestFR7_AdminEndpointRequiresJWT       без токена → 401
TestFR7_ExpiredJWTRejected             истёкший → 401
TestFR7_ViewerCannotCreatePoll         роль viewer → 403
TestFR7_DefaultCredentialsFailInProd   ENV=production + дефолтный пароль → ошибка старта
```

### FR-8 · Голоса вне окна отвергаются

```
TestFR8_VoteBeforeOpensAt              → 409
TestFR8_VoteAfterClosesAt              → 409
TestFR8_TokenIssuedBeforeCloseAccepted ← находка ревью: токен выдан до закрытия,
                                         голос пришёл в grace → принят
TestFR8_TokenIssuedAfterCloseRejected  токен выдан после закрытия → 409 даже в grace
```

---

## Нефункциональные требования

### NFR-1 · Пиковая нагрузка

```
k6: load/k6/vote.js
  измеряет стоимость одного голоса: команд в Redis, байт по сети, CPU из /metrics
  печатает линейную экстраполяцию до 2M RPS
```

Абсолютный RPS ноутбука ничего не доказывает — доказывает стоимость единицы работы.

### NFR-2 · Доступность в окне эфира

```
scripts/chaos/redis-master.sh    доля 503 выросла и вернулась к нулю
scripts/chaos/app.sh             приём голосов не прерывался
scripts/chaos/postgres.sh        голосование продолжается при недоступном Postgres
```

### NFR-3 · Корректность подсчёта: завышение недопустимо

```
TestNFR3_SumOfCountersEqualsAcceptedResponses   ← в k6 teardown и в acceptance
TestNFR3_RetryDoesNotDoubleCount                 повтор того же токена не двоит счётчик
TestNFR3_SnapshotIsMonotonic                     Redis обнулился → Postgres не откатился
```

### NFR-4 · Анонимность

```
TestNFR4_NoColumnLinksVoteToPerson    статическая проверка схемы: ни в одной таблице
                                      нет столбца, связывающего голос с человеком
TestNFR4_DedupKeyStoresNoChoice       значение дедуп-ключа не содержит выбор
TestNFR4_LogsContainNoVoterIDOrRawIP  прогон запроса, проверка захваченных логов
TestNFR4_AnomalyKeysAggregateOnly     ключи детекта содержат подсеть, но не адрес
```

Единственное требование, где часть доказательства структурная: отсутствие возможности
нельзя доказать сценарием, поэтому первый тест проверяет схему, а не поведение.

### NFR-5 · Отказоустойчивость

Каждый chaos-сценарий заканчивается сверкой агрегата, а не проверкой «сервис отвечает».

```
scripts/chaos/redis-master.sh    сумма счётчиков == число ответов counted
scripts/chaos/app.sh             то же + дедуп ловит повтор на другом инстансе
scripts/chaos/postgres.sh        то же + снапшоты возобновились после подъёма
scripts/chaos/latency.sh         то же + p99 вырос без потери голосов
```

### NFR-6 · Масштабируемость

```
make load с одним и с двумя инстансами → пропускная способность растёт линейно
```

### NFR-7 · Наблюдаемость

```
TestNFR7_MetricsExposeBusinessCounters   /metrics содержит votes_accepted, votes_rejected
TestNFR7_MetricLabelsAreBounded          набор reason конечен, нет пользовательских данных
TestNFR7_ReadyzChecksDependencies        Redis недоступен → /readyz отдаёт 503
```

### NFR-8 · Поддерживаемость

```
make lint    depguard валит сборку при импорте из domain чего-либо, кроме stdlib и uuid
```

### NFR-9 · Безопасность

```
TestNFR9_BodySizeLimitEnforced          тело больше 1 КБ → 400
TestNFR9_ForgedXFFIgnored               XFF от недоверенного источника не влияет на лимит
TestNFR9_IPv6LimitedByPrefix            два адреса одной /64 делят лимит
TestNFR9_SecurityHeadersPresent         HSTS, CSP, X-Frame-Options, Referrer-Policy
make vuln                               govulncheck чист
```

---

## Доказательство инвариантов: сломай и жди красного

TDD по истории коммитов здесь не проверить — агенты пишут пачками. Проверяется результат:
**для каждого инварианта из `CLAUDE.md` ломаем реализацию и убеждаемся, что тест краснеет.**
Если не покраснел — инвариант не защищён, каким бы очевидным он ни казался.

```bash
make verify-invariants     # применяет мутацию, ждёт красного, откатывает
```

| Инвариант | Мутация | Обязан упасть |
|---|---|---|
| `ttl(dedup) ≥ exp(token) + skew` | уменьшить `DEDUP_TTL` ниже `BALLOT_TTL` | `TestConfig_RejectsShortDedupTTL` |
| Общий hash tag у дедупа и счётчика | убрать `{}` из ключа счётчика | `TestVote_NoCrossSlotError` |
| `shard_count` из строки опроса | читать из глобального конфига | `TestVote_ShardCountFromPoll` |
| Дубль → 200 | вернуть 409 на `already_counted` | `TestFR4_SecondVoteSameTokenIsAlreadyCounted` |
| `/ballot` только POST | разрешить GET | `TestBallot_IsPOSTOnly` |
| Rate limit IPv6 по /64 | считать по полному адресу | `TestNFR9_IPv6LimitedByPrefix` |
| Доверенный XFF | брать заголовок как есть | `TestNFR9_ForgedXFFIgnored` |
| Джиттер TTL | убрать джиттер | `TestVote_TTLHasJitter` |
| Снапшотер пишет абсолютные значения | заменить `GREATEST` на присваивание | `TestNFR3_SnapshotIsMonotonic` |
| Фоновый рефрешер конфига | сделать ленивый TTL | `TestPollCfg_NoIOOnHotPath` |
| Postgres вне горячего пути | читать конфиг из БД в хендлере | `TestPollCfg_NoIOOnHotPath` |
| Успех только за принятый голос | отвечать 200 при ошибке Redis | `TestVote_RedisErrorReturns503` |
| `hmac.Equal` | заменить на `bytes.Equal` | `TestBallot_VerifyIsConstantTime` |
| Голоса не хранятся | добавить таблицу с отдельными голосами | `TestNFR4_NoColumnLinksVoteToPerson` |

Мутация, после которой всё зелёное, означает одно из двух: тест — декорация, либо инвариант
на самом деле не реализован. Оба случая — дефект.
