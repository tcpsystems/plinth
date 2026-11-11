# testkit/postgres

One helper that gives a Go test a real PostgreSQL database and the DSN that
reaches it, then removes the database when the test ends. It replaces the
per-repository `tools/testdb.sh` scripts: there is no shell step to copy and
no container helper to keep in sync.

```go
import "github.com/tcpsystems/plinth/testkit/postgres"

func TestStore(t *testing.T) {
	db := postgres.Start(t)
	sqlDB, err := sql.Open("pgx", db.DSN)
	// ... run migrations and the test against sqlDB ...
}
```

`Start` fails the test with `t.Fatalf` when it cannot start or reach a server,
and `t.Cleanup` drops the test's database (and stops the container, when one
was used). It is safe to call from parallel tests and goroutines.

## Where the server comes from

1. **A single-use container** (default). `Start` launches
   `docker.io/library/postgres:18-alpine` with podman, sharing the host
   network namespace, on a free loopback port. The image must be present
   locally; the helper never pulls.
2. **A running server** when a base DSN is supplied. Nothing starts; the
   server named by the DSN is reused and every test still gets its own fresh
   database. The base DSN is the first non-empty of:
   - the optional argument: `postgres.Start(t, "postgres://postgres@127.0.0.1:5432/postgres")`
   - `Options.BaseDSN` through `postgres.StartWithOptions`
   - `TEST_POSTGRES_DSN`

   An application's `DATABASE_URL` is deliberately ignored: a test must not
   reach a database it was not pointed at.

Because `TEST_POSTGRES_DSN` is inherited by every process the test starts, a
local developer or CI job can export it once and the whole suite reuses one
server:

```sh
export TEST_POSTGRES_DSN='postgres://postgres@127.0.0.1:5432/postgres'
go test ./...
```

## Isolation

Every `Start` call creates a new database named after the test
(`test_` + the sanitized test name, with `_1`, `_2`, ... when two tests share
a name), from `TEMPLATE template0`, and drops it at cleanup, terminating any
open sessions first. Two concurrent tests never share a database, so a test
that runs migrations or truncates tables cannot disturb another.

## Safety

- Only databases the helper created are dropped. Each is tagged with a random
  token unique to the calling Go process; cleanup checks the tag first, so a
  name in a reused server that the helper did not create (or that another
  test process created) is never touched.
- A base DSN must name a loopback TCP host (`localhost`, `127.0.0.1`, `::1`,
  or no host at all). The helper refuses anything else, so it is for local
  tests, not managed servers.
- The role in a base DSN must be allowed to `CREATE DATABASE`.

## Options

`postgres.StartWithOptions(t, postgres.Options{...})`:

| Field            | Default                                |
|------------------|----------------------------------------|
| `BaseDSN`        | empty (start a container)              |
| `Image`          | `docker.io/library/postgres:18-alpine` |
| `Podman`         | `podman`                               |
| `StartupTimeout` | 2 minutes                              |

## Requirements

- A container start needs podman and the image present locally.
- No database driver is needed by the helper itself; use whatever driver the
  service uses (`github.com/jackc/pgx/v5/stdlib` is the usual choice).
- The base-DSN tests that need a server start one from `postgres:18-alpine` when no host `postgres` binaries are installed.
