-- Durable job store. A job is enqueued in the caller's business transaction,
-- claimed with FOR UPDATE SKIP LOCKED, retried with backoff and left in the
-- failed state, with last_error kept, after max_attempts.
--
-- state:    queued | running | done | failed
-- run_at:   earliest time the job may be claimed (backoff pushes it out)
-- locked_until: the lease of a running claim; an expired lease is claimable
--          again by any claimer.
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
