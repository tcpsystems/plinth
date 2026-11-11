# queue

Postgres-backed job queue with batched claims and lease fencing.

A claim is one statement, and therefore one transaction:

```sql
UPDATE job AS t
SET state = 'claimed', lease_epoch = t.lease_epoch + 1
WHERE t.id IN (
    SELECT d.id
    FROM job AS d
    WHERE d.state = 'pending' AND d.next_due <= now()
    ORDER BY d.priority, d.next_due, d.id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING t.id, t.lease_epoch
```

The row locks taken by the subquery are held until that statement commits, and
`SKIP LOCKED` steps over rows another claimer holds, so concurrent claimers get
disjoint batches without waiting on each other. Claim order is lowest priority
value, then earliest `next_due`, then lowest id. The index in
[`schema.sql`](schema.sql) serves both the pending range and that order.

`ClaimN` clamps `n` to `1..MaxBatch` (1000) and returns fewer than `n` rows,
including zero, when fewer are due.

## Fencing

Every claim increments `lease_epoch`. `Settle` and `Fail` only touch a row that
is still `claimed` at the epoch the caller holds. If another owner reclaimed
the row, the epoch no longer matches, no row is updated, and the call returns
`ErrStale`. A stale owner can therefore neither settle nor reschedule a job it
no longer owns.

`Settle` marks the row `done`. `Fail` returns it to `pending` with `next_due`
moved forward by the given backoff (a negative backoff means zero).

## Usage

```go
q, err := queue.New(db) // or queue.NewWithSchema for other column names
if err != nil {
    return err
}

jobs, err := q.ClaimN(ctx, 32)
if err != nil {
    return err
}
for _, job := range jobs {
    if err := handle(ctx, job); err != nil {
        _ = q.Fail(ctx, job.ID, job.Epoch, time.Minute)
        continue
    }
    if err := q.Settle(ctx, job.ID, job.Epoch); err != nil {
        // ErrStale: the lease was reclaimed while the job was in hand.
    }
}
```

The returned slice is not ordered; order only decides which rows are claimed.

## Jobs: leases, retries and dead letters

`Jobs` is a complete job queue on top of the same claim technique. It owns its
table; create it from `JobsDDL(table)` (also in [`jobs_schema.sql`](jobs_schema.sql))
in a migration.

```go
j, err := queue.NewJobs(db, queue.JobsConfig{LeaseTTL: 30 * time.Second})

// Producer. The key makes a retried request add the job once.
job, created, err := j.Enqueue(ctx, queue.NewJob{
    Kind: "email.send", Payload: payload, IdempotencyKey: "order-1842",
})

// Worker.
jobs, err := j.Claim(ctx, 16, queue.Filter{Kinds: []string{"email.send"}})
for _, job := range jobs {
    res, err := handle(ctx, job) // call j.Heartbeat(ctx, job.ID, job.Epoch) while it runs
    if err != nil {
        state, _ := j.Fail(ctx, job.ID, job.Epoch, err) // "pending" (retry) or "dead"
        continue
    }
    err = j.Complete(ctx, job.ID, job.Epoch, res) // ErrStale: lease was lost
}
```

- **Claim** leases up to N due jobs in one `FOR UPDATE SKIP LOCKED` statement.
  Due means `pending` with `run_at` passed, or `running` with an expired lease
  and attempts left, so a crashed worker's jobs come back without a separate
  reaper. Each claim counts one attempt.
- **Fencing.** `Heartbeat`, `Complete` and `Fail` take the `Epoch` from `Claim`
  and return `ErrStale` once the lease was lost, completed or failed. This is
  why they take the epoch as well as the job id.
- **Heartbeat** pushes the lease out by `LeaseTTL`; call it at a fraction of the
  TTL while a job runs.
- **Retries.** `Fail` reschedules after `BackoffBase * 2^(attempts-1)`, capped at
  `BackoffMax`. When `Attempts` reaches `MaxAttempts` (per job, default 5) the
  job becomes `dead`, keeping `LastError`. A job whose lease expires on its
  final attempt is dead-lettered by `Reap`, which `Claim` runs first.
- **Dead letters.** `DeadLetters` lists dead jobs; `Requeue` gives one a fresh
  set of attempts.
- **Idempotency.** `(kind, idempotency_key)` is unique; `Enqueue` returns the
  existing job with `created == false`, including under concurrent enqueues.
- **Admin.** `Depth` (ready, delayed, running, done, dead), `Stuck` (running
  with a lease expired longer than a grace period, unclaimed) and `Throughput`
  (done and dead in a window).

Finished rows are not deleted; prune `done` rows with a retention job of your
own.

## Measured ceiling

One million due rows, one connection, Postgres 18:

| batch | claims/s | rows/s | p99 |
| ----: | -------: | -----: | --: |
|     1 |    700.9 |    700.9 | 12.3 ms |
|    32 |    514.5 |  16,464 |  6.8 ms |

The single-row ceiling is about 700 claims/s on one connection, i.e. 700 jobs/s:
the statement cost is dominated by round trip and commit, not by the row.

A batch of 32 raises row throughput to about 16.5k/s, roughly 23x, while the
p99 of a claim call stays under 10 ms. That is the batch size this package
recommends. Larger batches amortize the same fixed cost over more rows but hold
row locks and connections longer for little further gain; `MaxBatch` caps a
careless request at 1000.

### Reproducing

The numbers come from `BenchmarkClaimN`, over a disposable database:

```sh
../tools/testdb.sh go test ./queue/... -run '^$' -bench BenchmarkClaimN -benchtime=2s
```

The benchmark seeds `dueRows = 1_000_000` due rows with ten distinct priorities
and `next_due` spread across the last hour, builds the claim index after the
load, and reports `claims/s`, `rows/s` and the p99 of each claim call. Absolute
numbers depend on the host; the ratio between the two batch sizes is the point.

## River client

`Client` adds a durable, retrying queue on top of [River](https://riverqueue.com)
over `database/sql`. Use the claim API above for a table you own; use `Client`
for background work enqueued from application transactions.

```go
c, err := queue.NewClient(queue.Config{DB: db, Workers: workers})
if err != nil {
    return err
}
if err := c.Start(ctx); err != nil { // applies River's migrations first
    return err
}
defer c.Stop(ctx)

// In a request: the job exists only if the transaction commits.
_, err = c.InsertTx(ctx, tx, SendEmail{To: addr, Key: msgID}, queue.Idempotent(nil))
```

- **Idempotent inserts.** `Idempotent` skips a second insert with the same args.
  Tag the key field of the args with `river:"unique"`. `InsertMany` and
  `InsertManyTx` report skipped duplicates per job; one batch must not contain
  the same key twice.
- **Periodic jobs.** `Config.Periodic` or `AddPeriodic` take a schedule (`Every`
  or any `river.PeriodicSchedule`, such as a cron parser) and a constructor. Only
  the leader enqueues them.
- **One leader.** River elects one client through the `river_leader` table; that
  client runs the scheduler, the rescuer and the cleaner. `NoLeader` keeps a
  client with a partial worker set out of the election.
- **Rescue.** A job still running after `Lease` is treated as abandoned by a dead
  worker: the leader reschedules it with backoff, or marks it `Dead` once
  `MaxAttempts` runs are used. The per-job timeout is nine tenths of the lease.
  A job that outlives its lease can run twice, so handlers must be idempotent.
- **Backoff.** `Backoff{Base, Max}` doubles the delay per attempt and never
  exceeds `Max`.

Rescue and retry timing is bounded by River's maintenance intervals (the
scheduler runs every 5 seconds and the rescuer every 30), so a rescued job
resumes some seconds after its lease ends, not at the instant.

The client uses the `database/sql` driver, which has no LISTEN/NOTIFY: fetching
and leader election poll. Tune `FetchPollInterval` for latency.

Tests run against Postgres through `../tools/testdb.sh go test ./queue/...` and
skip without `KEEL_TEST_DSN`.
