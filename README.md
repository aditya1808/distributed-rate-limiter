# Distributed Rate Limiter

A rate limiting REST service in Go. Many stateless instances share one
Valkey/Redis store and enforce quotas **exactly**, not approximately: every
decision is a single atomic Lua script that uses the store's clock.

- Two algorithms per rule: **token bucket** and **sliding window log**, both
  with separate *sustained* and *burst* limits.
- Rules managed at runtime over an authenticated API, propagated to every
  instance in milliseconds (pub/sub) with a periodic reload as a safety net.
- Observable quota endpoint, Prometheus metrics on an internal port, structured
  JSON logs, and an admin dashboard.
- Adaptive limits: report a downstream error rate and limits tighten
  temporarily.
- Tested for contention: unit, contract (Lua vs Go reference), HTTP
  integration validated against the OpenAPI spec, multi-instance, and an e2e
  suite that runs against the live DigitalOcean deployment from CI.

Architecture, request flow diagrams and the contention analysis are in
[docs/architecture.md](docs/architecture.md). Design decisions are in
[docs/adr](docs/adr). The API contract is
[apispec/ratelimiter.openapi.yaml](apispec/ratelimiter.openapi.yaml).
Rules for AI coding agents (and a good checklist for humans) are in
[AGENTS.md](AGENTS.md): invariants that must not be broken, where code goes,
and what must pass before a change is done. `CLAUDE.md` imports it for Claude
Code.

## Live deployment

Deployed on DigitalOcean App Platform (blr1): two instances behind the platform
load balancer, sharing a managed Valkey cluster. Every push to `main` is
tested, built into an image, rolled out, and then verified by the e2e suite and
a cross-instance load test against the public URL.

- URL: <https://rate-limiter-dgmu7.ondigitalocean.app> (try
  `curl -s https://rate-limiter-dgmu7.ondigitalocean.app/readyz`)
- Admin endpoints and the dashboard require the admin token.

Results from the CI deploy job (GitHub-hosted runner → DigitalOcean blr1, a new
TLS connection per request so the platform load balancer spreads traffic):

```
e2e  token_bucket    allowed=50 errors=0 served_by=map[api-…-9dgpz:199 api-…-h8rhf:201]
e2e  sliding_window  allowed=50 errors=0 served_by=map[api-…-9dgpz:190 api-…-h8rhf:210]

load test: 2000 requests, 100 concurrent, burst 100, refill 1/h
allowed: 100  denied: 1900  errors: 0  expected allowed: 100
  api-7fc5856f85-9dgpz   served   989  allowed   54
  api-7fc5856f85-h8rhf   served  1011  allowed   46
latency p50 301ms  p95 336ms  p99 417ms   (client side, incl. US→India RTT + TLS handshake)
```

Both instances admitted part of the quota, and together they admitted exactly
the burst. Server-side `/v1/check` latency from the access logs over 3,445
production requests: p50 1.4 ms, p95 2.3 ms, p99 5.5 ms; the client-side
figure is dominated by network distance and a fresh TLS handshake per request.

## Quick start

Requirements: Go 1.26+, Docker.

```bash
# Two instances + Redis behind nginx on http://localhost:8080
export ADMIN_TOKEN=$(openssl rand -hex 32)
make up

# Create a rule: 10 requests/second sustained, bursts of up to 20
curl -s -X POST localhost:8080/v1/rules \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"tenant":"acme","namespace":"orders","algorithm":"token_bucket","limit":10,"period":"1s","burst":20}'

# Decision endpoints need a Bearer JWT (format-checked only for now, see Limitations)
JWT=eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZW1vIn0.c2lnbmF0dXJl

# Ask for a decision
curl -s -X POST localhost:8080/v1/check -H "Authorization: Bearer $JWT" -d '{"tenant":"acme","namespace":"orders"}'
# {"allowed":true,"tenant":"acme","namespace":"orders","rule_id":"r_01...","algorithm":"token_bucket",
#  "limit":20,"remaining":19,"reset_at":"...","retry_after_ms":0,"degraded":false,"adaptive_multiplier":1}

# Observe without consuming
curl -s localhost:8080/v1/quota/acme/orders -H "Authorization: Bearer $JWT"

# Prove cross-instance correctness: exactly the burst is admitted, split across both instances
make e2e load

# Dashboard: http://localhost:8080/dashboard  (any username, admin token as password)
```

Run the test suite (needs a Redis on `localhost:6379`; `make redis` starts one):

```bash
make redis test      # all tests with the race detector
make stress          # concurrency tests repeated 20x
make lint
```

## API

| Method | Path | Auth | Purpose |
|---|---|---|---|
| `POST` | `/v1/check` | JWT | Decide and consume quota: `{tenant, namespace, cost?}` |
| `GET` | `/v1/quota/{tenant}/{namespace}` | JWT | Current quota without consuming |
| `POST` | `/v1/rules` | admin | Create a rule |
| `GET` | `/v1/rules` | admin | List rules (`?tenant=&namespace=`) |
| `GET` `PUT` `DELETE` | `/v1/rules/{id}` | admin | Read, replace (`If-Match` required), delete |
| `POST` | `/v1/feedback` | admin | Report downstream error rate → adaptive limit |
| `GET` | `/v1/tenants/active` | admin | Recently active tenants with quota (dashboard data) |
| `GET` | `/dashboard` | admin | HTML dashboard |
| `GET` | `/healthz`, `/readyz` | none | Liveness, readiness |
| `GET` | `:9090/metrics` | internal port | Prometheus |

Design choices worth calling out:

- **Deny is `200`, not `429`.** `/v1/check` answers a question; `allowed:false`
  is a successful answer. The calling service returns `429` to *its* client
  using `retry_after_ms`. HTTP errors mean the question could not be answered.
- **The body is the contract.** No `RateLimit-*` headers: the caller is a
  service reading JSON, and headers would duplicate the body.
  `X-Request-ID` and `X-Instance-ID` are returned for tracing.
- **Errors are RFC 7807** `application/problem+json` with per-field validation
  messages.
- **Decision endpoints require a Bearer JWT.** Today only its format is
  checked (three base64url segments, JSON header with a real `alg`, JSON
  payload, non-empty signature); full verification is a tracked TODO.
- **Admin auth** is a bearer token (or Basic with the token as password, for
  the dashboard in a browser), compared in constant time. Metrics live on a
  separate port that is never exposed publicly. The OpenAPI spec is in the
  repo, not served.
- **Optimistic concurrency** on rules: `ETag` on every read, `If-Match`
  required on `PUT` (`428` if missing, `412` if stale).

### Rules

```jsonc
{
  "tenant": "acme",            // or "*" for a per-tenant default rule
  "namespace": "orders",
  "algorithm": "sliding_window",
  "limit": 100,                // sustained: 100 per period
  "period": "1m",
  "burst": 20,                 // burst cap
  "burst_window": "1s",        // sliding window only: at most 20 in any 1s
  "fail_open": false           // decision when the store is unavailable
}
```

Lookup is exact `(tenant, namespace)` first, then the `"*"` default for the
namespace. A default rule gives **each tenant its own quota** (the real tenant
is always part of the state key), never one shared bucket.

## Algorithms and why

| | Token bucket | Sliding window log |
|---|---|---|
| Sustained limit | refill rate `limit / period` | at most `limit` in any rolling `period` |
| Burst | bucket capacity `burst` (defaults to `limit`) | at most `burst` in any rolling `burst_window` |
| Memory per key | O(1): `{tokens, ts}` | O(limit): one entry per admitted request |
| Boundary behaviour | smooth | exact, no fixed-window edge doubling |
| Best for | high limits, APIs that tolerate bursts | low limits that must never be exceeded in any window (logins, payments, SMS) |

Fixed windows were rejected: a client can send `2 × limit` across a window
boundary. The sliding window *counter* approximation was rejected because it
is approximate, and exactness is the point of this service. The log's memory
cost is bounded by capping sliding-window limits at 10,000; higher limits use
the token bucket.

Both algorithms:

- run atomically in Redis and read Redis `TIME` (one clock for all instances);
- support a request `cost` (weighted requests), rejected up front if it can
  never fit;
- return `remaining`, `reset_at` (when the quota is fully restored) and
  `retry_after_ms` (when this request would succeed);
- set TTLs so idle keys disappear on their own;
- have a Go reference implementation, and a contract test runs the same
  scenarios through both and requires identical results.

### Adaptive limits

`POST /v1/feedback {tenant, namespace, error_rate, ttl_seconds}` sets a
multiplier read by the scripts: error rate ≥ 50 % → 0.25×, ≥ 20 % → 0.5×,
≥ 5 % → 0.8×, otherwise back to 1×. It expires automatically (default 5 min),
so a forgotten degradation heals itself. The multiplier is visible in every
check and quota response and on the dashboard.

## Correctness across instances and under contention

The short version (full table in
[docs/architecture.md](docs/architecture.md#correctness-under-contention)):

- **Same key, same instant, many instances:** one atomic script per decision.
  Tests fire 500 concurrent requests at a burst of 50 and require *exactly*
  50 admitted, against Redis directly, through two in-process instances, and
  through the deployed load balancer. A naive GET-then-SET limiter run under
  the same test admits all 500.
- **Clock skew:** instances never send time; Redis `TIME` is used.
- **Rule propagation:** milliseconds via pub/sub; worst case bounded by the
  5 s reload. Worst-case over-admission during that window is one extra burst.
- **Concurrent rule edits:** atomic version checks in Lua; `409` / `412`.
- **Store slow or down:** 50 ms timeout, per-rule fail mode, `degraded:true`.

## Testing

| Layer | Where | What it proves |
|---|---|---|
| Unit | `limiter`, `types`, `manager`, `cache` | Algorithms, validation, adaptive maths, cache versioning |
| Contract | `limiter/contract_test.go` | Lua scripts == Go reference, including script-cache flush |
| Concurrency | `limiter/concurrency_test.go` | Exact admission at 500-way contention; naive control over-admits |
| Store | `persistence/redisstore` | Atomic create/update races, pub/sub, schema versioning |
| HTTP integration | `controllers/*` | Every endpoint; every response validated against the OpenAPI spec |
| Multi-instance | `test/multiinstance_test.go` | Two app instances, one Redis: exact admission and rule propagation |
| End-to-end | `test/e2e` (`-tags e2e`) | Real deployment over HTTP: exact admission across ≥ 2 instances |
| Load | `tools/loadtest` | Throughput, latency, per-instance split, exact admission |

CI ([.github/workflows/ci.yml](.github/workflows/ci.yml)) runs on every push:
module verification, vet, golangci-lint, the full suite with `-race`, the
concurrency tests repeated 5×, and a Docker build. On `main` it pushes the
image to GHCR, rolls it out to DigitalOcean, then runs the e2e suite and load
test against the public URL.

## Configuration

| Variable | Default | Notes |
|---|---|---|
| `ADMIN_TOKEN` | required | ≥ 16 characters |
| `REDIS_URL` | `redis://localhost:6379/0` | `rediss://` for TLS |
| `REDIS_TIMEOUT` | `50ms` | Hard limit per store call on the hot path |
| `REDIS_POOL_SIZE` | `50` | |
| `REDIS_MIN_IDLE_CONNS` | `REDIS_POOL_SIZE` | Dialed before the instance reports ready and kept open, so bursts never pay for TLS dials inside `REDIS_TIMEOUT` |
| `KEY_PREFIX` | `rl` | Isolates environments sharing a store |
| `DEFAULT_POLICY` | `allow` | Decision when no rule matches (`allow` / `deny`) |
| `RULE_REFRESH_INTERVAL` | `5s` | Full rule reload, repairs missed pub/sub |
| `PORT` / `INTERNAL_PORT` | `8080` / `9090` | Public API / metrics |
| `INSTANCE_ID` | hostname | Returned as `X-Instance-ID` |
| `EXPOSE_INSTANCE_ID` | `true` | |
| `SHUTDOWN_DRAIN_DELAY` | `5s` | Readiness fails first, then requests drain |
| `SHUTDOWN_TIMEOUT` | `15s` | |
| `LOG_LEVEL` | `info` | |

## Deployment

The app spec is [.do/app.yaml](.do/app.yaml): two `apps-s-1vcpu-1gb`
instances, readiness on `/readyz`, liveness on `/healthz`, `REDIS_URL` bound to
the managed Valkey cluster (`${rl-valkey.DATABASE_URL}`, TLS, trusted-source
restricted to the app). `ADMIN_TOKEN` is a GitHub secret injected as an App
Platform secret at deploy time; nothing sensitive is committed.

Valkey runs with `maxmemory-policy volatile-ttl`. Rules have no TTL, so they
are never evicted. Under memory pressure the quota keys closest to expiry go
first; for a token bucket that is the bucket closest to full, so evicting it
barely changes any decision. The default `noeviction` would instead fail every
write and degrade every decision.

Valkey also runs with `timeout 0`: idle connections are never closed by the
server. Each instance dials its whole pool before reporting ready and keeps
it open, because a dial on the request path (TLS to the managed store) can
exceed the 100 ms store deadline and turn decisions into degraded denies.

To recreate from scratch:

```bash
doctl databases create rl-valkey --engine valkey --version 8 --region blr1 --size db-s-1vcpu-1gb
doctl databases configuration update <db-id> --engine valkey --config-json '{"valkey_maxmemory_policy":"volatile-ttl","valkey_timeout":0}'
IMAGE_TAG=<sha> ADMIN_TOKEN=... envsubst '$IMAGE_TAG $ADMIN_TOKEN' < .do/app.yaml > /tmp/app.yaml
doctl apps create --spec /tmp/app.yaml --wait
gh variable set DO_APP_ID -b <app-id>; gh variable set DO_APP_URL -b <url>   # enables CD
```

## Project layout

```
main.go            process lifecycle, graceful shutdown, healthcheck subcommand
core/              dependency wiring
config/            environment configuration
routes/            route table
middlewares/       recovery, request id, body limit, access log, metrics, admin auth
controllers/       HTTP handlers (+ HTTP integration tests)
schemas/           request/response wire types
validations/       custom validators
manager/           business logic: evaluator, rules, adaptive, activity
limiter/           algorithms: Lua scripts + Go reference
cache/             in-process rule snapshot
persistence/       store interfaces; redisstore implementation
types/             domain types and errors
metrics/           Prometheus collectors
apispec/           OpenAPI 3 contract
test/              multi-instance and e2e tests
tools/loadtest/    load generator
docs/              architecture and ADRs
```

## Limitations and what I would do next

- **Rules live in Redis.** Their durability equals the store's (managed Valkey
  persists, and Compose enables AOF). At larger scale rules belong in a
  durable database, with Redis only for counters and the change channel.
- **Failover can reset counters.** Redis replication is asynchronous, so a
  failover can lose the latest writes and admit up to one extra burst per key.
- **Rule changes reset quota.** State keys are per rule version, so editing a
  rule starts fresh buckets; this avoids misinterpreting old counters but gives
  each key a fresh burst once.
- **Single region.** One store means one region. Multi-region would need
  per-region quotas with periodic reconciliation, accepting bounded
  over-admission.
- **Hot keys.** One extremely hot (tenant, namespace) pair is bound to one
  Redis shard. Mitigations: local pre-admission (lease a block of tokens per
  instance) for very high limits.
- **TODO: real authentication and authorization.** Auth was not part of the
  brief, so the JWT on decision endpoints is only format-checked: any
  well-formed token is accepted. Before production:
  - authn: verify signatures against the identity provider's JWKS
    (RS256/ES256 only) and check `iss`, `aud`, `exp`, `nbf`;
  - authz: per-route scopes (`ratelimit:check`, `ratelimit:admin`) and a
    `tenants` claim so a caller can only consume its own tenants' quota;
  - replace the shared `ADMIN_TOKEN` with the same scheme and audit-log rule
    changes with the caller identity.
- **Total store outage** takes every instance out of readiness, so the load
  balancer answers `503`; callers should apply their own fallback (ADR 0004).
- **Sliding window memory** is O(limit) per key, hence the 10,000 cap.
