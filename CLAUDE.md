# Televote

Anonymous voting at the peak of a TV broadcast: 100M viewers, a 60-second window.
Inside that window the system **only accepts**. Deduplication and counting are done by the
consumer, and the result lands eventually, after roughly 5 minutes of drain. The spec does not
require freshness, and that is what gave the system its shape.

Architectural reasoning and AI artifacts: `README.md`

## Commands

```
make demo     bring up the stand, create a demo poll, print the links   ← start here
make test     unit + integration (testcontainers, Redis Cluster)
make lint     golangci-lint v2
make smoke    end-to-end check
make chaos    failure scenarios
make load     k6
```

## Layer boundaries

```
domain  ◄──  application  ◄──  adapters (httpapi, storage, vote, snapshot)
```

`internal/domain` imports **nothing** from the project. Reverse imports are forbidden by
`depguard` in `.golangci.yml` — that is a check, not a preference.

## Invariants that break silently

None of these produces a compile error, a failing test, or a log line.
Re-read this before changing the related code.

| Invariant | What happens if it is broken |
|---|---|
| `DEDUP_TTL >= drain window x margin` | the key is created by the consumer; two messages from one person arrive at the start and the end of the drain |
| **`localStorage`, never `sessionStorage`** | F5 in the same tab is caught, but closing the tab yields a new vote — the test must emulate closing |
| The Lua script is idempotent per `voterID` | Kafka delivers **at-least-once**: a rebalance replays the message, and without idempotency every rebalance inflates the result |
| The window is checked against the produce timestamp | not the processing time: a vote from second 59 is consumed at second 300 |
| `maxmemory-policy noeviction` | Redis evicts dedup keys, and repeat votes start counting |
| Redis integration tests run on a node with `cluster-enabled` | without it the server never checks `CROSSSLOT`, and the test proves nothing |
| The dedup key and the counter share a hash tag | otherwise `CROSSSLOT`, and the script does not execute |
| `shard_count` is read from the poll row | not from global config: changing the value on air would reopen voting |
| The ingest response is **`202 accepted`** | `200 counted` would be a lie: the vote has not been counted yet |
| IPv6 rate limiting is per /64 prefix | the client owns the whole /64, so limiting per address is useless |
| `X-Forwarded-For` is trusted only from our own ingress | otherwise the limit is bypassed with one line of curl |
| Dedup TTL carries +/-10% jitter | 30M keys expiring at once would finish Redis off on the tail |
| The snapshotter writes **absolute** values through `GREATEST` | deltas plus a retry give an inflated result |
| Poll config uses a background refresher, not a lazy TTL | a TTL expiry at 2M RPS gives a thundering herd |
| Postgres is not on the hot path | otherwise its failure stops the voting |
| Closing by hand moves `closes_at`, not the status | status `closed` drops the poll out of the snapshotter's selection: the final seconds never land in the result, and the published result is not written at all |
| Never answer success for a vote Redis did not accept | lying to the client is worse than showing a 503 |

## Privacy

The data model contains **no** "vote ↔ person" link, and one cannot be added.
The dedup key records the fact of voting, not the choice.

- `voter_id` comes from `crypto/rand`, and is not derived from an IP or a fingerprint
- IPs in logs and metrics appear only as a hash with a rotating salt
- Individual votes are not stored anywhere, analytics included
- Anomaly detection aggregates by /16 subnet, not by address

## Conventions

| Subject | Rule |
|---|---|
| Constructors | `New*(deps...) (*T, error)`, dependencies as parameters. No globals and no `init()` |
| Errors | typed domain errors; the **single** mapping point is `internal/httpapi/errors.go` |
| Context | `ctx` first, a timeout on every external call |
| Interfaces | declared on the consumer side, kept narrow |
| Logs | `slog`, `trace_id` on every record. On the hot path, sampling and errors only |
| Config | one struct with `env` tags, validated at startup, fail fast |
| Comments | explain "why", not "what" |

The single error-mapping point is not a style choice: it guarantees that `already_counted`
returns 200 from every handler, rather than 409 from whichever one was written last.

## Tests

Table-driven, next to the code, with names that read as assertions:
`TestVote_DuplicateTokenReturnsAlreadyCounted`.

Integration tests run against a real Redis (cluster-enabled) and Postgres through
testcontainers, behind the `integration` build tag. The voting Lua script executes there for
real: deduplication, idempotency under concurrent delivery, counter TTL, and CROSSSLOT rejection.

The load test measures the **cost of a single vote** rather than absolute RPS, and checks that
the sum of the counters equals the number of successful responses.

## What not to do

- Do not add storage of individual votes — it breaks the anonymity the project claims
- Do not move deduplication into process memory — correctness requires Redis
- Do not use a fingerprint as a dedup key or as a signal: there is not enough entropy
- Do not answer `200` for a vote that has only been placed into Kafka
- Do not reshard Redis and do not ship a release inside the broadcast window
- Do not log `voter_id`, a raw IP, or token contents
- Do not replace `GREATEST` with assignment in the snapshotter
- Do not add k8s manifests: the stand runs on docker-compose, production is described in `README.md`
