# Changelog

All notable changes to plinth. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
pre-1.0 semantic versioning: `0.MINOR.PATCH`, a breaking change bumps MINOR.

## Unreleased

## v0.3.0 - 2026-11-11

First release published to the Go module proxy. Earlier versions were
not published; start from this one.

### Added

- `telemetry`: OpenTelemetry instrumentation with OTLP/HTTP export for traces
  and metrics. Provides `Setup` for initialization, HTTP middleware for server
  spans, `ClientTransport` for distributed tracing, and a slog handler for
  trace-correlated logs. Reads standard `OTEL_EXPORTER_OTLP_*` environment
  variables and propagates W3C trace context (traceparent/tracestate).
- `migrate`: optional `NNN_name.down.sql` pairs and `Rollback`, which applies
  down bodies newest first in one transaction and refuses edited migrations;
  a configurable ledger through `Options` (`Schema`, `Table`, `LockKey`); and
  `Execer`/`Queryer` executors so callers can pass a transaction without the
  root module importing another driver.
- `jobs`: durable PostgreSQL job store over `database/sql`. `Enqueue` writes
  inside the caller's transaction; `Claim` takes due or lease-expired jobs with
  `FOR UPDATE SKIP LOCKED`; `Complete` and `Fail` settle them, and `Fail`
  retries with exponential backoff until `max_attempts`, then keeps the job in
  the failed state with `last_error`. `Worker` runs the poll loop: it claims
  jobs, calls the handler registered for each kind under a context bounded by
  the lease, recovers handler panics into `Fail`, and on cancellation stops
  claiming, lets running handlers finish and settles their jobs with a
  detached context.
- `quota`: single-statement atomic PostgreSQL `quota_ledger` and `quota_cap`
  admission (`INSERT ... ON CONFLICT DO UPDATE ... WHERE l.units + excluded.units <= cap RETURNING units`)
  with exempt-caller support, cap table management and window pruning.
- `keel/queue`: `Client`, a River-backed job queue over `database/sql`:
  transactional `InsertTx`, `InsertMany` with idempotency keys, periodic jobs,
  leader-only maintenance, lease-based rescue of stuck jobs, bounded
  exponential backoff and a dead state after the last attempt. `Migrate`
  applies River's schema; `Client.Start` runs it unless `SkipMigrate` is set.
- `keel/outbox`: an outbox whose `Writer.Write` inserts the event row and a
  relay job in the caller's transaction. A queue worker publishes outside any
  row lock and retries with backoff.
- `keel/auth/access`: verifies Cloudflare Access JWTs at the origin (RS256,
  team JWKS with rotation, issuer, audience and clock skew checks) and
  composes with service roles so an Access identity never bypasses
  authorization.
- `keel/leader`: run a background job in one replica at a time with
  PostgreSQL session advisory locks (`TryAcquire`, `Renew`, `Release`).
- `keel/events`: topic publish/subscribe bus on PostgreSQL with monotonic
  sequence cursors, at-least-once delivery, LISTEN/NOTIFY wakeups, and
  publish coalescing.
- `realtime`: Connect-JSON RPC over the keel subset, schema index documents,
  and TypeScript client generation; the conformance suite checks a running
  service's realtime surface.
- `problem`: the v1 error catalogue is checked against the Go package by
  `errors.yaml`.

### Changed

- Package conventions are stated in the package documentation and checked by
  `conformance` rather than in separate specification documents.

## v0.2.0 - 2026-11-10

Not published.

### Changed (breaking)

- Module path is now `github.com/tcpsystems/plinth` (was
  `github.com/tcpsystems/go-kit`). Update every import and the `require` line.
- Documentation, examples and package comments use the `plinth` name.

## v0.1.0 - 2026-11-09

First tagged release. Not published.

### Added

- `service`: public and mesh listeners with graceful draining, and
  `ReadVersion` schema/contract reporting.
- `health`: liveness, readiness, named checks, `/version` and `Mount`.
- `problem`: RFC 9457 `application/problem+json` responses with a shared code
  catalogue.
- `page`: bounded cursor pagination with signed tokens, keyset SQL and a
  response envelope (`Response`, `Write`).
- `idem`: `Idempotency-Key` middleware with memory and SQL stores.
- `settings`: typed settings loader with TTL cache and change notification.
- `outbox`: transactional outbox writes and a polling relay.
- `conformance`: HTTP contract checks a service can run against itself, plus
  a realtime suite.
- `jsonx`, `httpx`, `envx`, `retry`, `mw`, `migrate`: JSON engine switch, JSON
  HTTP helpers, typed environment reads, backoff retries, middleware and
  forward-only migrations.
- `bearer`, `config`, `reqid`, `logx`: bearer parsing and scopes, bootstrap
  config, UUIDv7 request IDs with W3C trace context, request logging.
- `buildinfo`: linker-injected revision, dirty flag, build time and `/version`
  handler.
- `ratelimit`, `webhooks`, `sse`, `realtime`, `events`: IETF rate-limit
  headers, Standard Webhooks signing, outbox-backed SSE, WebSocket and
  Connect-JSON RPC hub, and the event envelope.
- `examples/hello` and `examples/refservice`.
