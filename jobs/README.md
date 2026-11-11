# jobs

Durable job store on PostgreSQL. Jobs are enqueued inside the caller's transaction, claimed with `FOR UPDATE SKIP LOCKED` under a lease, retried with backoff, and left in a failed state with their last error after attempts are exhausted.

## Schema

The `jobs` table and index are defined in [`schema.sql`](schema.sql):

```sql
CREATE TABLE IF NOT EXISTS jobs (
    id           bigserial   PRIMARY KEY,
    kind         text        NOT NULL,
    payload      jsonb       NOT NULL DEFAULT '{}',
    state        text        NOT NULL DEFAULT 'queued'
                 CHECK (state IN ('queued', 'running', 'done', 'failed')),
    attempts     integer     NOT NULL DEFAULT 0,
    max_attempts integer     NOT NULL DEFAULT 10 CHECK (max_attempts > 0),
    run_at       timestamptz NOT NULL DEFAULT now(),
    locked_until timestamptz,
    last_error   text,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS jobs_due_idx ON jobs (state, run_at);
```

## States

| State | Meaning |
|---|---|
| `queued` | Job is claimable once `run_at` has passed |
| `running` | Job is claimed; `locked_until` holds the lease deadline |
| `done` | Job completed successfully; `locked_until` cleared |
| `failed` | Attempts exhausted; `last_error` preserved |

- `attempts`: count of claim attempts so far (incremented on each claim)
- `locked_until`: lease deadline for a running claim; NULL when unclaimed or settled

## Enqueue in the caller's transaction

Always enqueue inside your business transaction so the job commits or rolls back with the write that scheduled it:

```go
err := db.WithTx(ctx, func(tx *sql.Tx) error {
    // ... business write ...
    jobID, err := jobs.Enqueue(ctx, tx, jobs.Job{
        Kind: "email:send",
        Payload: json.RawMessage(`{"to":"user@example.com"}`),
    })
    return err
})
```

The job becomes visible to claimers only when the transaction commits.

## Worker tuning

Create a `Worker` with options; all fields fall back to defaults when zero:

| Option | Default | Purpose |
|---|---|---|
| `Batch` | 10 | Jobs claimed per poll |
| `PollInterval` | 1s | Idle wait before next claim |
| `Lease` | 1m | Lease requested per job; **must exceed** the longest legitimate handler runtime |
| `Concurrency` | 1 | Handlers running at once |
| `Backoff` | `DefaultBackoff` | Retry delay function |
| `Logger` | discard | Structured log sink |

```go
w := jobs.NewWorker(store, jobs.Options{
    Batch:       20,
    PollInterval: 500 * time.Millisecond,
    Lease:       2 * time.Minute,
    Concurrency: 4,
    Logger:      logger,
})
```

**Lease discipline:** `Lease` must exceed the longest handler runtime under normal load. A handler that outlives its lease is cancelled and the job becomes claimable again while the first handler may still be running.

## Draining

Call `Run(ctx)` with a cancellable context. On cancellation:

1. **Stop claiming:** no new `Claim` calls after `ctx.Err()` is set
2. **Wait for handlers:** all running handlers finish and settle with `Complete` or `Fail`
3. **Detach for settlement:** settle calls use `context.WithoutCancel` so shutdown does not abandon a claim mid-flight
4. **Abandon in-flight claims:** a job claimed in the same poll as cancellation is not started; it is failed with an "abandoned on shutdown" error and retries after backoff

For graceful shutdown, cancel the context, wait for `Run` to return, then exit:

```go
ctx, cancel := context.WithCancel(context.Background())
go w.Run(ctx)
// ... on SIGTERM ...
cancel()
// w.Run returns after all handlers settle
```

## Operating queries

### Due backlog by kind

```sql
SELECT kind, COUNT(*) AS due_count
FROM jobs
WHERE state = 'queued' AND run_at <= now()
GROUP BY kind
ORDER BY due_count DESC;
```

### Expired leases

```sql
SELECT id, kind, attempts, locked_until, now() - locked_until AS age
FROM jobs
WHERE state = 'running' AND locked_until < now()
ORDER BY age DESC;
```

### Failed jobs with last_error

```sql
SELECT id, kind, attempts, last_error, updated_at
FROM jobs
WHERE state = 'failed'
ORDER BY updated_at DESC;
```

### Throughput by minute

```sql
SELECT date_trunc('minute', updated_at) AS minute,
       COUNT(*) FILTER (WHERE state = 'done') AS completed,
       COUNT(*) FILTER (WHERE state = 'failed') AS failed
FROM jobs
WHERE updated_at >= now() - interval '1 hour'
GROUP BY minute
ORDER BY minute DESC;
```
