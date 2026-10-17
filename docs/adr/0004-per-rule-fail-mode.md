# ADR 0004: Per-rule fail-open / fail-closed with a hard store timeout

## Status

Accepted

## Context

When the state store is slow or unreachable the limiter cannot know the real
quota. Blocking would turn a store problem into an outage of every caller.
The right fallback depends on what the quota protects: a login endpoint should
fail closed, a read-heavy catalogue may prefer to fail open.

## Decision

- Every store call on the hot path has a hard timeout (`REDIS_TIMEOUT`, 50 ms
  by default), so a check always answers quickly.
- On a store error the decision follows the rule's `fail_open` flag (default
  `false`, i.e. deny) and is marked `degraded: true` with reason
  `store_unavailable`, so callers and dashboards can tell it apart from a real
  quota decision.
- Requests with no matching rule follow the service-wide `DEFAULT_POLICY`.
- `/readyz` reports the store as down so the load balancer and alerts react;
  `/healthz` (liveness) stays green so instances are not restarted in a loop
  for a dependency problem.
- Warnings during an outage are sampled (one per second) to avoid log floods.

## Consequences

- Bounded latency and an explicit, per-rule safety choice.
- `GET /v1/quota` has no safe fallback value, so it returns `503` instead.
- Readiness includes the store, so an instance that alone loses Redis is taken
  out of rotation. The trade-off: if Redis is down for *every* instance, the
  load balancer has no ready backend and answers `503` itself, so degraded
  decisions are only seen during the detection window. Callers must therefore
  apply their own fallback for limiter errors and timeouts, which they need
  anyway for network failures between them and the limiter.
