# Architecture

The service answers one question at high volume: *may this tenant make one more
request to this namespace right now?* Every instance is stateless; all quota
state and all rules live in one shared Redis-compatible store (Valkey on
DigitalOcean, Redis locally). Correctness across instances comes from doing every
read-modify-write inside the store as a single atomic Lua script, using the
store's own clock.

## Components

```mermaid
flowchart LR
    client([Client service]) -->|POST /v1/check| lb[Load balancer]
    admin([Operator]) -->|/v1/rules, /v1/feedback, /dashboard<br/>admin token| lb
    lb --> i1 & i2

    subgraph i1 [Instance 1]
        direction TB
        mw1[Middlewares<br/>recovery, request id, body limit,<br/>access log, metrics, admin auth] --> ctl1[Controllers]
        ctl1 --> ev1[Evaluator]
        ctl1 --> rm1[Rule manager]
        ev1 --> rc1[(Rule cache<br/>atomic snapshot)]
        ev1 --> lim1[Redis limiter<br/>Lua scripts]
    end

    subgraph i2 [Instance 2]
        direction TB
        mw2[Middlewares] --> ctl2[Controllers] --> ev2[Evaluator]
        ev2 --> rc2[(Rule cache)]
        ev2 --> lim2[Redis limiter]
    end

    lim1 & lim2 -->|EVALSHA token_bucket / sliding_window| redis[(Valkey / Redis<br/>quota state, rules,<br/>adaptive multipliers, activity)]
    rm1 -->|Lua: write rule + PUBLISH| redis
    redis -. pub/sub rules:changed .-> rc1 & rc2
    rc1 & rc2 -. reload every 5s .-> redis
    prom([Prometheus]) -->|:9090 /metrics, internal only| i1 & i2
```

Package responsibilities:

| Package | Responsibility |
|---|---|
| `routes` | Route table; public, admin (auth-gated) and internal (`/metrics`) groups |
| `middlewares` | Cross-cutting HTTP concerns, all problem+json on failure |
| `controllers` | Bind and validate input, call a manager, map the result to a schema |
| `schemas` | Wire request/response types, separate from domain types |
| `manager` | Business logic: evaluate, rule CRUD, adaptive limits, activity |
| `limiter` | Algorithms: Lua scripts for Redis plus an in-memory Go reference |
| `cache` | Lock-free in-process rule snapshot kept in sync via pub/sub |
| `persistence` | Store interfaces; `redisstore` implements them |
| `types` | Domain types, validation, sentinel errors |
| `core` | Wiring and lifecycle |

## Request path: `POST /v1/check`

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant M as Middlewares
    participant K as CheckController
    participant E as Evaluator
    participant RC as Rule cache
    participant R as Redis

    C->>M: POST /v1/check {tenant, namespace, cost}
    M->>M: request id, 64 KB body limit, metrics timer
    M->>K: next
    K->>K: bind JSON (unknown fields rejected), validate identifiers and cost
    alt invalid
        K-->>C: 400 application/problem+json with field errors
    end
    K->>E: Evaluate(tenant, namespace, cost)
    E->>RC: Resolve(tenant, namespace)
    Note over RC: exact rule, else "*" default rule<br/>atomic pointer load, no lock
    alt no rule
        E-->>C: 200 allowed = DEFAULT_POLICY, reason no_rule
    end
    alt cost > rule capacity
        E-->>C: 200 allowed=false, reason cost_exceeds_capacity
    end
    E->>R: EVALSHA script (state key, seq key, adaptive key) with 50 ms timeout
    Note over R: one atomic script:<br/>TIME, read state, refill or trim,<br/>decide, write, PEXPIRE
    alt Redis error or timeout
        E-->>C: 200 allowed = rule.fail_open, degraded=true, reason store_unavailable
    else decision
        R-->>E: allowed, remaining, retry_after, reset_after, now, multiplier
        E->>E: record activity (async, buffered), decision metric
        E-->>C: 200 {allowed, remaining, reset_at, retry_after_ms, ...}
    end
```

The check endpoint returns `200` for both allow and deny. A deny is a successful
answer, not an HTTP error: the *caller* turns it into its own `429` for its own
client. Errors (`400`, `413`, `503`) mean the question itself could not be
answered.

## Rule changes

```mermaid
sequenceDiagram
    autonumber
    participant A as Operator
    participant I1 as Instance 1
    participant R as Redis
    participant I2 as Instance 2

    Note over I1,I2: at startup each instance SUBSCRIBEs first, then loads all rules,<br/>so no change can fall between the load and the subscription
    A->>I1: PUT /v1/rules/{id}, If-Match: "3"
    I1->>R: GET current rule (scope immutability, early 412)
    I1->>R: Lua: check version == 3, HSET rule v4, PUBLISH {upsert, id, 4}
    R-->>I1: OK (or VERSION_MISMATCH → 412)
    I1->>I1: upsert into local cache immediately
    I1-->>A: 200, ETag: "4"
    R-->>I2: message {upsert, id, 4}
    I2->>R: HGET rule
    I2->>I2: swap in new snapshot if version is newer
    Note over I2: if the message is lost (reconnect), the 5s periodic reload converges
```

State keys include the rule version (`rl:{tenant|ns}:<ruleID>:v<version>`), so
an updated rule starts a fresh bucket instead of reinterpreting counters that
were written under different parameters.

## Data model in Redis

| Key | Type | Purpose | TTL |
|---|---|---|---|
| `rl:{rules}:data` | hash `id → rule JSON` (with `schema_version`) | Rule records | none |
| `rl:{rules}:index` | hash `tenant\|ns → id` | One rule per scope (HSETNX) | none |
| `rl:rules:changed` | pub/sub channel | Change events | n/a |
| `rl:{t\|ns}:<id>:v<n>` | hash `{tokens, ts}` | Token bucket state | until full again |
| `rl:{t\|ns}:<id>:v<n>` | sorted set `member → ms` | Sliding window log | window |
| `rl:{t\|ns}:<id>:v<n>:seq` | counter | Unique sorted-set members | window |
| `rl:{t\|ns}:adaptive` | string multiplier | Adaptive tightening | feedback TTL |
| `rl:activity` | sorted set `t\|ns → last seen` | Dashboard | trimmed to 1h |

Everything a script touches for one decision shares the `{tenant|ns}` hash tag,
so the same scripts run unchanged on Redis Cluster.

## Correctness under contention

| Race window | How it is closed or bounded |
|---|---|
| Two instances check the same key at the same instant | Each decision is one Lua script; Redis executes scripts one at a time, so read-decide-write cannot interleave. Proven by tests: 500 concurrent requests against a burst of 50 admit exactly 50 (a naive GET-then-SET limiter admits all 500). |
| Instance clocks disagree | No instance clock is used. Scripts read `TIME` from Redis, the single clock for every decision. A clock moving backwards after failover is clamped (no negative refill). |
| Rule update reaches instances at different times | Pub/sub delivers in milliseconds; a lost message is repaired by the periodic reload (`RULE_REFRESH_INTERVAL`, default 5 s). During that window an instance can enforce the previous version, so worst case is old burst + new burst admitted once. |
| Concurrent rule writes | Create uses `HSETNX` on the scope index (exactly one wins → 409 for the rest). Update compares the stored version inside the script (optimistic concurrency, `If-Match`/`ETag`, 412 on mismatch, 428 if missing). |
| Cache update races (pub/sub vs reload vs local write) | Snapshots are immutable and swapped with an atomic pointer; writers are serialized and only ever move a rule to a *higher* version. |
| Script cache flushed (failover, `SCRIPT FLUSH`) | `EVALSHA` falls back to `EVAL` on `NOSCRIPT`; covered by a test that flushes scripts mid-run. |
| Redis slow or down | Every store call has a hard timeout (`REDIS_TIMEOUT`, 50 ms). The decision degrades to the rule's `fail_open` setting and is marked `degraded: true`; readiness turns red so the load balancer can react. |
| Store call times out after the script already ran | A timeout is ambiguous: the token may be consumed while the caller gets a degraded deny. This can only *under*-admit (one token per degraded response), never over-admit. It's kept rare by pre-dialing connections (`REDIS_MIN_IDLE_CONNS`) so bursts don't pay TLS dials inside the deadline. The e2e and load tests assert `allowed ≤ burst` and `allowed ≥ burst − degraded`. Seen once in production right after a deploy, when the connection pools started empty. |
| Redis failover with async replication | Writes not yet replicated are lost; affected counters restart, which can admit up to one extra burst per key. This is the standard trade-off for an in-memory limiter and is documented as a limitation. |
| Floating-point drift in token refill | Token counts are rounded to 6 decimals with an epsilon compare; contract tests check the Lua scripts against the Go reference implementation scenario by scenario. |

## Deployment topology (DigitalOcean)

```mermaid
flowchart LR
    gh[GitHub push to main] --> ci[GitHub Actions<br/>lint, race tests, stress]
    ci --> img[Build image<br/>ghcr.io/aditya1808/distributed-rate-limiter:sha]
    img --> dep[doctl apps update --wait]
    dep --> app
    subgraph do [DigitalOcean blr1]
        lb[App Platform<br/>load balancer, TLS] --> a1[api instance 1] & a2[api instance 2]
        a1 & a2 -->|rediss:// trusted source| vk[(Managed Valkey 8)]
    end
    app[App: rate-limiter] --- lb
    dep --> e2e[e2e + load test against the public URL<br/>asserts exact admission across ≥2 instances]
```
