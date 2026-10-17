# ADR 0003: In-process rule cache, invalidated by pub/sub, repaired by periodic reload

## Status

Accepted

## Context

Every check needs the matching rule. Reading it from Redis on each request
would add a second round trip to the hot path. Rules change rarely, but a
change must reach every instance quickly.

## Decision

Each instance keeps an immutable snapshot of all rules behind an atomic
pointer: reads are lock-free and never block. Rule writes run as Lua scripts
that update the record and `PUBLISH` a change event atomically, so a stored
change is never missing its notification.

- On startup an instance subscribes **before** the initial load, so no change
  can slip between the two.
- The writing instance applies its own change immediately.
- Other instances fetch the changed rule when the event arrives.
- Pub/sub is at-most-once, so a full reload every `RULE_REFRESH_INTERVAL`
  (default 5 s) repairs anything missed during reconnects.
- Updates are applied only if they carry a higher version, so the three
  sources (local write, event, reload) cannot move a rule backwards.

Optimistic concurrency (`ETag` / `If-Match`) protects the admin API from lost
updates. Stored rules carry `schema_version` so the record format can evolve
with reader-first rollouts.

## Consequences

- Zero extra latency per check for rule lookup.
- Instances converge within milliseconds normally and within the refresh
  interval in the worst case. During that window two instances may enforce
  different versions; the documented bound is one extra burst.
- Rules live in Redis alongside counters. That is acceptable for this scope
  but means rule durability equals Redis durability (see README limitations).
