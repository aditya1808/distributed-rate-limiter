# ADR 0001: Shared state in Redis, decisions as atomic Lua scripts

## Status

Accepted

## Context

Several stateless instances run behind a load balancer and must enforce one
quota per (tenant, namespace) together. A decision is read-modify-write: read
the counter, decide, write it back. Done as separate commands from several
instances, two requests can read the same value and both be admitted.

Options considered:

1. **Per-instance local limits** (limit / N each). No coordination, but wrong
   whenever traffic is uneven or N changes, and gives no global view.
2. **Redis `WATCH`/`MULTI` optimistic transactions.** Correct, but retries
   explode exactly when contention is high, which is when a limiter matters.
3. **Redis Lua scripts (`EVALSHA`).** Redis runs one script at a time, so the
   whole read-decide-write is atomic with one round trip and no retries.
4. **A consensus store (etcd) or a SQL row lock.** Correct but milliseconds per
   decision and far lower throughput.
5. **Approximate gossip (local buckets periodically reconciled).** Scales
   further but over-admits by design.

## Decision

Keep all quota state in Redis (Valkey in production) and implement each
algorithm as a single Lua script. Scripts are loaded once and invoked by SHA,
falling back to `EVAL` on `NOSCRIPT`. All keys used by one decision share a
`{tenant|ns}` hash tag so the scripts remain valid on Redis Cluster.

## Consequences

- Exact admission under any level of concurrency (verified: 50 of 500).
- One network round trip per decision; throughput scales with Redis and, with
  Cluster, with the number of shards (keys are spread by tenant and namespace).
- Redis is a hard dependency on the hot path; see ADR 0004 for behaviour when
  it is unavailable.
- Scripts must stay short and O(log n); the sliding window limit is capped
  (`MaxSlidingWindowLimit`) because its memory and work grow with the limit.
