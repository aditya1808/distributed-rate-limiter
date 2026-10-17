# ADR 0005: Rules and assignments in Postgres, Redis as a shared miss cache

## Status

Accepted. Supersedes the storage part of [ADR 0003](0003-rule-cache-with-pubsub-and-reload.md).

## Context

Rules used to live in Redis, each carrying its own `(tenant, namespace)`
scope. Two problems:

- Durability of configuration equalled the durability of an in-memory store
  tuned for hot counters (eviction policy, async replication).
- A rule could not be reused. Giving ten tenants the same limit meant ten
  copies, and changing the limit meant ten edits.

The brief keys decisions on an identifier and a namespace, so a tenant can
have different rules in different namespaces, and tenants in one namespace
can have different rules.

## Decision

- **Rules are a separate entity** with no scope: a required display name
  (not unique), algorithm, limit, period, burst, burst window, fail mode,
  version.
- **Assignments** bind `(tenant, namespace)` to a rule. Tenant `*` is the
  namespace default. Each assignment is a resource with its own id and
  version, so a UI or audit log can reference it, and it can be updated with
  `If-Match` like a rule. A unique constraint on the pair keeps one
  assignment per pair; one rule can serve any number of pairs.
- Both live in **Postgres**. The foreign key is `ON DELETE RESTRICT`, so a
  rule that is still assigned cannot be deleted (409 `rule-in-use`). Schema
  changes are embedded, append-only SQL migrations applied at startup under
  an advisory lock.
- Redis keeps quota state and the `rules:changed` channel. Quota keys are
  unchanged (`{tenant|ns}:<ruleID>:v<version>`), so a shared rule still gives
  every pair its own quota.
- Each instance loads every binding into memory (ADR 0003). If a pair is not
  in the snapshot at all, the lookup is a safety net, not the normal path:
  1. local miss cache (`RULE_MISS_TTL`, bounded size),
  2. Redis resolution cache `{tenant|ns}:resolution`, shared by instances,
  3. Postgres, with the answer written back to Redis for `RULE_MISS_TTL`.

  "No rule" is cached too, and concurrent misses for one pair share one
  lookup, so unknown tenants cannot turn into a query per request.

## Consequences

- Configuration is durable and transactional; quota state stays in the
  fastest store.
- Postgres is needed to start and to change configuration, not to serve
  checks. Readiness does not depend on it.
- The Postgres commit and the Redis publish are not atomic. A lost publish
  delays propagation until the next periodic reload (5 s by default).
- A Redis resolution entry can be stale for up to `RULE_MISS_TTL`; writing an
  assignment deletes the entry for that exact pair.
- Rule records written to Redis by earlier versions are no longer read.
  They are left in place and can be deleted by hand.
