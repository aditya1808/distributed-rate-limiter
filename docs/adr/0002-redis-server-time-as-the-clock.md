# ADR 0002: Redis server time is the only clock

## Status

Accepted

## Context

Both algorithms depend on elapsed time: token refill and window position.
If each instance passed its own `time.Now()` to the script, clock skew between
hosts would hand out extra tokens (a fast clock) or deny valid requests (a slow
clock), and the effect would differ per instance.

## Decision

Scripts call `TIME` inside Redis and use that value for every calculation.
Instances never send a timestamp. Responses carry `reset_at` computed from the
Redis time returned by the script. If Redis time moves backwards (for example
after a failover to a replica with a slightly different clock), the token
bucket clamps to no refill rather than going negative.

## Consequences

- No dependency on NTP quality across application hosts.
- Calling `TIME` makes scripts non-deterministic, which requires effects
  replication. That is the default since Redis 5 and the only mode in Redis 7
  and Valkey.
- A Redis failover can still shift time slightly; the clamp bounds the error.
