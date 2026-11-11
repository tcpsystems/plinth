# plinth

plinth is a small Go toolkit for HTTP services. It provides the building
blocks a service needs around its own business logic: a runnable service shell,
health and version endpoints, problem responses, cursor pagination,
idempotency, realtime delivery, a transactional outbox, database migrations,
and conformance checks for the HTTP and realtime conventions the packages
follow.

It is a library, not a service. It keeps no state of its own and starts no
listener unless you call it.

The project is pre-1.0: minor versions may change APIs, and every such change
is listed in [CHANGELOG.md](CHANGELOG.md).

## Modules

| Module | Import path | Third-party modules |
| --- | --- | --- |
| Root | `github.com/tcpsystems/plinth` | none; standard library only |
| `keel/` | `github.com/tcpsystems/plinth/keel` | `github.com/jackc/pgx/v5`, `github.com/riverqueue/river` and their small dependencies |
| `telemetry/` | `github.com/tcpsystems/plinth/telemetry` | `go.opentelemetry.io/otel` and related packages |

The root module works with `database/sql` and a driver you inject. The
`keel` module holds the Postgres-backed pieces (leader election and a shared
event bus), so a service that only needs the HTTP toolkit does not pull in a
database driver. The `telemetry` module provides OpenTelemetry instrumentation
and is off by default.

## Install

```bash
go get github.com/tcpsystems/plinth@v0.3.0
go get github.com/tcpsystems/plinth/keel@v0.3.0         # only for the PostgreSQL pieces
go get github.com/tcpsystems/plinth/telemetry@v0.3.0    # only for OpenTelemetry
```

```go
import (
	"github.com/tcpsystems/plinth/health"
	"github.com/tcpsystems/plinth/problem"
	"github.com/tcpsystems/plinth/service"
)
```

The reference for every package is its documentation on
[pkg.go.dev](https://pkg.go.dev/github.com/tcpsystems/plinth), including the
runnable examples at
<https://pkg.go.dev/github.com/tcpsystems/plinth#section-exampleindex>, and
[examples/hello](examples/hello/main.go) is a complete service in one file
that uses `service`, `health`, `problem`, `page`, `idem`, `reqid` and `logx`
together.

## Try it

`examples/hello` is a complete service built from the library:

```bash
go run ./examples/hello
curl -s localhost:8080/version
```

`examples/refservice` shows the rest: problem responses, pagination,
idempotency, realtime streams, and a conformance run against itself.

Requires Go 1.27 or newer. `go.mod` pins `go 1.27`; check
[go.dev/dl](https://go.dev/dl/) for toolchains.

## Packages

| Package | Purpose |
| --- | --- |
| `service` | Runs a service with separate public and internal listeners, graceful shutdown on SIGTERM/SIGINT, and version metadata. |
| `health` | Liveness, readiness, named checks, and endpoints for `/healthz`, `/readyz`, and `/version`. |
| `problem` | RFC 9457 `application/problem+json` responses with stable error codes, and the v1 error catalogue. Responses never include SQL, stack text, or wrapped internal error text. |
| `page` | Cursor pagination with a bounded page size and opaque tokens. |
| `idem` | `Idempotency-Key` middleware and stores for replay-safe request handling. |
| `ratelimit` | Fixed-window request limiting with IETF RateLimit headers and a 429 Retry-After. |
| `quota` | Single-statement atomic PostgreSQL `quota_ledger` and `quota_cap` admission. |
| `mw` | Recovery, security headers, CSRF, CORS and timeout middleware. |
| `bearer`, `reqid`, `logx` | Bearer token parsing, request IDs, and request logging. |
| `telemetry` | OpenTelemetry traces, metrics, and log correlation with OTLP/HTTP export. |
| `realtime` | In-process `Hub` with ring replay and resume; `SSE` and `WebSocket` transports; Connect-JSON `RPC`; schema and client generation. |
| `sse`, `webhooks` | Server-sent events helpers and signed webhook delivery. |
| `settings` | Runtime settings loader with cache and invalidation. |
| `outbox` | Transactional outbox writer and relay interfaces. |
| [`jobs`](jobs) | Durable PostgreSQL job store: enqueue inside the caller's transaction, leased `SKIP LOCKED` claims, backoff retries and a visible failed state; a `Worker` runs handlers per kind and drains gracefully. |
| `migrate` | Versioned SQL migrations with optional down files, rollback and a configurable ledger. |
| `config`, `envx` | Service configuration loading and environment reads. |
| `jsonx`, `httpx`, `retry` | JSON encoding, HTTP response helpers, and retries with backoff. |
| `buildinfo` | Build revision and version stamping. |
| `conformance` | Checks that a running service follows the HTTP and realtime conventions: shapes, content types, pagination, idempotency, and streams. |
| `keel/leader` | Runs a background job in one replica at a time, using PostgreSQL advisory locks. |
| `keel/events` | Topic publish/subscribe bus with monotonic sequence cursors and at-least-once delivery on PostgreSQL. |
| `keel/queue` | Postgres job queue: batched claims with lease fencing, and a River-backed `Client` with transactional enqueue, idempotent batch inserts, periodic jobs, rescue of stuck jobs and a dead state. |
| `keel/outbox` | Transactional outbox: the event row and its relay job commit with the caller's transaction; a queue worker publishes it without holding row locks. |
| `keel/cron` | Periodic jobs that run in one replica at a time. |
| `keel/quota` | The `quota` ledger for services that already use the keel module. |
| `keel/auth/access` | Verifies Cloudflare Access JWTs at the origin. |
| `testkit/postgres` | Starts a throwaway PostgreSQL server for tests. |

Every package's `go doc` output is the reference. For the Postgres module,
[keel/leader/README.md](keel/leader/README.md) and
[keel/events/README.md](keel/events/README.md) explain how it works and its
delivery guarantees.

Each package's maturity is **settled** (the API covers what the HTTP conventions need
and changes only for a demonstrated defect) or **experimental** (first draft,
expect changes). Maturity is not a 1.0 promise: every package is at 0.x.

| Package | Provides | Maturity |
| --- | --- | --- |
| [`service`](service) | Public and mesh listeners with graceful draining, and read-only schema/contract version info | settled |
| [`health`](health) | Liveness, readiness, named checks, `/version` and `Mount` | settled |
| [`problem`](problem) | RFC 9457 `application/problem+json` errors with a shared code catalogue | settled |
| [`page`](page) | Signed cursor pagination: parsing, keyset SQL, response envelope | settled |
| [`idem`](idem) | `Idempotency-Key` middleware with memory and SQL stores | settled |
| [`ratelimit`](ratelimit) | Fixed-window rate limiting with IETF `RateLimit` headers | settled |
| [`quota`](quota) | Single-statement atomic PostgreSQL `quota_ledger` admission | settled |
| [`reqid`](reqid) | UUIDv7 `X-Request-Id` and W3C `traceparent` propagation | settled |
| [`logx`](logx) | One structured access-log record per HTTP request | settled |
| [`bearer`](bearer) | Bearer token parsing and scope checks | settled |
| [`httpx`](httpx) | JSON response writing and size-capped request decoding | settled |
| [`jsonx`](jsonx) | One JSON API over `encoding/json` or `encoding/json/v2` | settled |
| [`envx`](envx) | Typed, default-aware environment variable readers | settled |
| [`retry`](retry) | Retries with jittered exponential backoff | settled |
| [`mw`](mw) | Panic recovery, security headers, CSRF, CORS and timeouts | settled |
| [`telemetry`](telemetry) | OpenTelemetry traces, metrics, and log correlation with OTLP/HTTP export | experimental |
| [`outbox`](outbox) | Transactional outbox writes and a polling SQL relay | settled |
| [`jobs`](jobs) | Durable job store: transactional enqueue, leased SKIP LOCKED claims, backoff, a failed state and a `Worker` loop | experimental |
| [`sse`](sse) | Server-sent events served from an outbox table | settled |
| [`webhooks`](webhooks) | Standard Webhooks signing and verification | settled |
| [`buildinfo`](buildinfo) | Linker-injected revision, dirty flag and build time | settled |
| [`config`](config) | DSN and secret-reference lookup | settled |
| [`realtime`](realtime) | Hub with replay, SSE, WebSocket and Connect-JSON RPC | experimental |
| [`settings`](settings) | Typed settings with TTL caching and change notification | experimental |
| [`migrate`](migrate) | Versioned SQL migrations with optional rollback and a configurable ledger | experimental |
| [`conformance`](conformance) | Runs the HTTP contract checks against a live service (test-time only) | experimental |
| [`keel/queue`](keel/queue) | Postgres job queue with batched claims and a River-backed client | experimental |
| [`keel/outbox`](keel/outbox) | Transactional outbox relayed through the job queue | experimental |

The `examples/` directory has runnable services.

## Versioning

plinth uses pre-1.0 semantic versioning: versions are `0.MINOR.PATCH`. A
breaking change bumps MINOR, a compatible feature or fix bumps PATCH, and a
notable feature may bump MINOR. There is no `v1` import path.

What will define 1.0: every settled package has at least two real services
using it in production, the conformance suite covers each contract rule it
checks, and no settled package has changed its exported API for two minor
versions. Until then, pin a minor series (`v0.3.x`) and read the
[changelog](CHANGELOG.md) before upgrading a minor version.

## Dependencies

plinth has no `require` directives. It uses only the standard library;
SQL drivers are injected by the caller through `database/sql`. Packages such as
`jsonx` exist so a service can switch between `encoding/json` and
`encoding/json/v2` under the `jsonv2` experiment without changing call sites.

The `keel` sub-module is separate and does have dependencies: `pgx`, and
[River](https://riverqueue.com) for `keel/queue` and `keel/outbox`. Importing
only the root module never pulls them in.

### Telemetry

The [`telemetry`](telemetry) nested module keeps OpenTelemetry dependencies out
of the root module. Call `telemetry.Setup(ctx, telemetry.Options{ServiceName:
"example", SampleRatio: 1})` once at startup and call its returned shutdown
function with a fresh context on exit. Export is off unless an
`OTEL_EXPORTER_OTLP_ENDPOINT` or signal-specific endpoint is configured.
For a local collector, use `OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318`;
standard OTLP headers and per-signal endpoint settings are supported.

Wrap a ServeMux with `telemetry.Middleware("example")` for server spans and
request count/duration metrics labeled by route template, method and status
class. Use `telemetry.ClientTransport("example", nil)` for outgoing propagation
and `telemetry.Logger(nil)` with slog's context-aware methods for correlated
logs. See the [runnable example](examples/telemetry) and
[configuration reference](telemetry/README.md).

## Testing and quality

```bash
tools/gate.sh        # root module
bash tools/keel-gate.sh  # whole repository: root, keel, and the keel dependency budget
```

The root gate checks formatting, `go vet`, race-enabled tests, JSON engine
coverage, the absence of module requirements, and the `examples/hello` size
limit. The keel gate runs from the repository root (its `go vet` and `go test` cover
the root module too) and checks formatting, `go vet`, race-enabled tests, the
keel dependency budget, and lint. Tests that need PostgreSQL skip unless
`KEEL_TEST_DSN` is set; `tools/testdb.sh` starts a throwaway database for
them.

The `jobs` package keeps its PostgreSQL behavioural tests in a nested module
(`jobs/pgtest`) so the root module stays standard-library-only. Run them with:

```bash
(cd jobs/pgtest && KEEL_TEST_DSN="postgres://..." go test -race ./...)
```

or through the gate wrapper:

```bash
bash tools/jobs-gate.sh
```

## Releases

The module path is `github.com/tcpsystems/plinth`. The first published version
is `v0.3.0` for the root module and for every nested module (`keel`,
`telemetry`); earlier version numbers were never published to the module proxy.

Releases are tagged `v0.x.y` and described in [CHANGELOG.md](CHANGELOG.md).
The process is in [docs/RELEASING.md](docs/RELEASING.md), and the roadmap is
in [ROADMAP.md](ROADMAP.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues privately as
described in [SECURITY.md](SECURITY.md).

## License

MIT; see [LICENSE](LICENSE).
