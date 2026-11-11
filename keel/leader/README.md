# leader

Elects one holder for a named background job across replicas, using
PostgreSQL session advisory locks. No extra service, no leader table, no row
locking.

```go
db, err := leader.OpenSQL("pgx", dsn) // remember the DSN for pinned connections
m := leader.New(db, leader.Config{Namespace: 42})
defer m.Close()

ok, err := m.TryAcquire(ctx, "hourly-report", 90*time.Second)
if ok {
    defer m.Release(ctx, "hourly-report")
    // ... do the job, calling m.Renew before each side-effecting step ...
}
```

`Example_worker` in `example_test.go` is a runnable version: `go test
./leader -run Example`.

## How it works

- `TryAcquire` takes `pg_try_advisory_lock` on a connection leased from a
  small pool that the Manager owns. It never waits; a lock held elsewhere
  returns `false, nil`.
- The lock belongs to that session, so the Manager pins one connection per
  held job. Pinned connections never expire and are never closed for
  idleness, so nothing reaps a connection that still holds a lock.
- `Release` unlocks and returns the connection to the pool.
- If the process dies, its connection closes and PostgreSQL drops the session
  locks with it, so the next starter acquires immediately instead of waiting
  for a TTL.
- `Renew` reports whether the lock is still held on the pinned connection.

## Open the pool with `leader.OpenSQL`

`database/sql` gives no portable way to read a pool's DSN, and the pinned
connections must reach the same database as the service's pool. `OpenSQL`
remembers the connector the DSN builds. `New` accepts a pool opened with
`sql.Open`, but `TryAcquire` fails with a hint to use `OpenSQL` rather than
guess the database.

## Keys

`Key(jobName)` hashes the job name (FNV-1a 64). `Config.Namespace` separates
the keys from other advisory lock users in the same database; keep it constant
for a deployment. The lock is `pg_advisory_lock(classid, objid)` with the
namespace in the top bits of `classid`.

## The TTL is liveness, not fencing

The TTL is the caller's budget for how long a leader's work may take, and
`Renew` is the check that the lease is still good. They do **not** stop a
leader whose connection silently stalls from overlapping the next leader:

1. The leader's connection stalls, but the server has not noticed yet, so the
   lock is still held and no one else can become leader.
2. The server notices, drops the session and its locks, and another replica
   acquires.
3. The stalled leader wakes up, `Renew` fails or returns false, and it stops.

Between 2 and 3 the two may run side by side if the old leader had work in
flight. For jobs where overlap is harmful, call `Renew` before every unit of
side-effecting work and make each unit idempotent (for example, guard writes
with a job run id or a transaction check).

## Testing

The tests run against a real PostgreSQL. `tools/testdb.sh` starts a throwaway
`postgres:18-alpine` container on a loopback port:

```sh
cd keel
../tools/testdb.sh go test ./leader/... -race -count=5
../tools/testdb.sh go test ./leader/... -run TestConcurrentLeaders -race
```

Without `KEEL_TEST_DSN` the tests skip, so `go test ./...` still works on a
machine without a database.
