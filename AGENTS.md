# AGENTS.md

Instructions for AI coding agents (Cursor, Claude Code, Copilot, Codex, ...)
working in this repository. Humans should read it too. If a request conflicts
with a rule marked **MUST**, stop and ask instead of working around it.

## What this service is

A rate limiting decision service. Stateless Go instances share one
Redis/Valkey; every quota decision is a single atomic Lua script that uses the
store's clock. Rules and their `(tenant, namespace)` assignments live in
Postgres and are cached in memory on every instance. Correctness across instances is the product: a change that makes
the limiter faster but admits even one extra request under contention is a
regression.

Read before changing behaviour: [README.md](README.md),
[docs/architecture.md](docs/architecture.md) (request flow, data model,
race-window table) and [docs/adr](docs/adr).

## Layout and where code goes

| Package | Put here | Never put here |
|---|---|---|
| `routes/` | Route table and which middleware guards which route | Logic |
| `middlewares/` | Cross-cutting HTTP concerns | Business rules |
| `controllers/` | Bind, validate, call a manager, map to a schema, `HandleError` | Redis calls, algorithms |
| `schemas/` | Wire request/response types and their mapping | Domain logic |
| `manager/` | Business logic | HTTP or Gin types |
| `limiter/` | Algorithms: Lua scripts + Go reference implementation | HTTP, rules storage |
| `cache/` | In-process binding snapshot and the miss fallback | Writes to Postgres |
| `persistence/` | Store interfaces; `pgstore/` (rules, assignments, migrations) and `redisstore/` implement them | Business rules |
| `types/` | Domain types, validation, sentinel errors | Imports of other app packages except `constants` |
| `core/` | Wiring and lifecycle | Logic |

Dependencies point inward: `controllers → manager → limiter/persistence/cache → types`.
Do not import `controllers` or `gin` from `manager`, `limiter`, `cache`,
`persistence` or `types`.

## Invariants (MUST)

1. **Every read-modify-write of quota state is one Lua script.** Never
   implement a decision as separate Redis commands from Go (GET then SET,
   pipelines, `WATCH`). Never cache quota state in process.
2. **Redis `TIME` is the only clock** for algorithms. Never pass `time.Now()`
   from Go into a script.
3. **All keys touched by one script share the `{tenant|ns}` hash tag.** Build
   keys only through `constants.Keyspace`; do not format key strings elsewhere.
4. **The Lua scripts and the Go reference in `limiter/memory_limiter.go` must
   agree.** Change both together; `limiter/contract_test.go` must pass.
5. **Rule and assignment writes go through `manager.RuleManager`**, which
   reloads the local cache, publishes `rules:changed` and clears the pair's
   Redis resolution entry. Never write those tables from elsewhere.
6. **Quota state keys include the rule version and the real tenant.** Do not
   drop either from `Keyspace.State`; a shared or `*` rule must still give
   each pair its own quota.
7. **Postgres schema changes are new migration files** in
   `persistence/pgstore/migrations` (`NNNN_name.sql`). Never edit or reorder
   an applied migration. Changes must work with the previous release still
   running (add columns nullable or with defaults; drop in a later release).
8. **Postgres stays off the check path.** Only the snapshot-miss fallback may
   query it, through the singleflight and miss caches in `cache/`. Readiness
   must not depend on Postgres.
9. **Hot path latency is bounded.** Store calls on `/v1/check` go through the
   evaluator's timeout; a store failure yields a degraded decision following
   the rule's `fail_open`, never a hang or a 500.
10. **`/v1/check` returns `200` for allow and deny.** Do not switch denials to
    `429` and do not add `RateLimit-*` headers; the JSON body is the contract.
11. **The OpenAPI spec is the contract.** Any route, status code, field or
    header change updates `apispec/ratelimiter.openapi.yaml` in the same
    change. HTTP tests validate every response against it and fail on
    undocumented routes or statuses.

## Conventions

- Errors to clients: RFC 7807 via `controllers.WriteProblem` /
  `controllers.HandleError`. Map new sentinel errors in `HandleError`; never
  return raw error strings.
- Wrap errors with `fmt.Errorf("context: %w", err)`; compare with `errors.Is/As`.
- Logging: `log/slog` JSON with a `[COMPONENT]` prefix in the message. No
  request bodies, tokens or secrets in logs. Hot-path warnings must be sampled.
- Metrics: add collectors in `metrics/` on the private registry; keep label
  cardinality bounded (never tenant as a label).
- Configuration: environment variables in `config/config.go` with validation
  and a documented default; add every new variable to the README table.
- Validation: request rules in `binding` tags (`validations/` for custom
  tags); domain rules in `types.Rule.Validate` / `types.Assignment.Validate`.
- Comments explain constraints the code cannot show. No narration comments.
- Run `make fmt` (gofmt + goimports, local prefix
  `github.com/aditya1808/distributed-rate-limiter`).

## Tests (MUST pass before you say you are done)

```bash
make redis postgres  # throwaway Redis on :6379 and Postgres on :5432
make lint      # go vet + golangci-lint, must report 0 issues
make test      # everything with -race
make stress    # when touching limiter/, cache/, persistence/ or concurrency
```

- Bug fixes come with a test that fails without the fix.
- Concurrency tests assert exact totals. Do not weaken an exact assertion to
  "at most" or add retries/sleeps to make a flaky test pass; find the race.
- Never delete or skip `TestConcurrentSameKey_NaiveControl`; it proves the
  concurrency test can detect over-admission.
- Tests that need Postgres skip when `POSTGRES_URL` is unset; CI sets it, so
  a skipped test locally is not a pass. Each test app gets its own schema.
- Changing Lua: run `go test ./limiter/...` and `make stress`.
- Changing anything a deployment depends on: `make up && make e2e load`.

## Security

- Never commit secrets, tokens, `.env` files or real connection strings.
  Secrets live in GitHub Actions secrets and App Platform `SECRET` envs.
- `/dashboard` is public by design: keep it read-only and server-rendered,
  with no browser calls to `/v1` endpoints.
- `/metrics` stays on the internal port only. Do not expose the OpenAPI spec,
  pprof or debug endpoints on the public port.
- `middlewares/jwt.go` only checks JWT **format** (tracked TODO). Do not
  describe it as authentication, and do not remove the TODO without
  implementing signature and claim verification.

## Deployment and CI

- `.do/app.yaml` contains `${IMAGE_TAG}` (filled by `envsubst` in CI) and `${rl-valkey.DATABASE_URL}` / `${rl-postgres.DATABASE_URL}` (App Platform bindings).
  Do not hardcode values or rename these placeholders.
- Pushes to `main` deploy to production after tests pass. Do not add steps
  that skip tests, use `continue-on-error`, or deploy from other branches.
- Do not create, modify or delete cloud resources (DigitalOcean, GitHub
  settings) unless the user explicitly asks in the current session.

## Do not

- Add dependencies without a clear need; prefer the standard library.
- Change public API shapes, status codes or algorithm semantics without
  updating the spec, README and tests together.
- Mass-reformat or refactor unrelated code in a feature change.
- Disable linters with `//nolint` without a reason after the directive.
- Commit generated binaries, coverage files or editor settings.
