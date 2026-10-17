# ADR 0003: In-process rule cache, invalidated by pub/sub, repaired by periodic reload

## Status

Accepted. Storage moved from Redis to Postgres in [ADR 0005](0005-rules-and-assignments-in-postgres.md);
the cache design below still holds.

## Context

Every check needs the matching rule. Reading it from a store on each request
would add a second round trip to the hot path. Rules change rarely, but a
change must reach every instance quickly.

## Decision

Each instance keeps an immutable snapshot of every assignment and its rule
behind an atomic pointer: reads are lock-free and never block.

- On startup an instance subscribes to `rules:changed` **before** the initial
  load, so no change can slip between the two.
- The writing instance reloads synchronously before answering, so it reads
  its own writes.
- Other instances reload when the change event arrives. Bursts of events are
  coalesced into one reload.
- Pub/sub is at-most-once, so a full reload every `RULE_REFRESH_INTERVAL`
  (default 5 s) repairs anything missed during reconnects or a publish that
  failed after the database commit.
- Reloads are serialized and query inside the lock, so a later reload can
  never install older data than an earlier one.
- A pair missing from the snapshot (neither exact nor `*` default) falls back
  to a Redis resolution cache, then Postgres, and the answer is cached for
  `RULE_MISS_TTL` (ADR 0005).

Optimistic concurrency (`ETag` / `If-Match`) protects the admin API from lost
updates.

## Consequences

- Zero extra latency per check for rule lookup in steady state.
- Instances converge within milliseconds normally and within the refresh
  interval in the worst case. During that window two instances may enforce
  different versions; the documented bound is one extra burst.
- A full reload per change is cheap at this scale (one join over all
  assignments). With very many assignments it would become incremental.
