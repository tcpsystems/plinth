-- Cron schedule table with lease-based claiming for failover-safe runs.
CREATE SCHEMA IF NOT EXISTS cron;

CREATE TABLE IF NOT EXISTS cron.schedule (
    schedule_id   text        NOT NULL,
    next_due      timestamptz NOT NULL,
    owner         text        NULL,
    lease_until   timestamptz NULL,
    attempt       int         NOT NULL DEFAULT 0,
    max_attempts  int         NOT NULL DEFAULT 3,
    backoff       interval    NOT NULL DEFAULT '1 minute',
    every         interval    NULL,
    status        text        NOT NULL DEFAULT 'pending',
    payload       jsonb       NULL,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schedule_id, next_due),
    CONSTRAINT cron_schedule_status_check
        CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'dead')),
    CONSTRAINT cron_schedule_attempt_check CHECK (attempt >= 0 AND max_attempts >= 1)
);

-- One unclaimed pending run per schedule: the next occurrence that has not
-- started yet. Claiming (pending -> running) frees the slot, and a finished
-- run stays in its terminal state, so a survey of due rows never sees the
-- same schedule twice. A running run that is still leased is covered by the
-- primary key alone: it keeps its next_due until a new leader reclaims it.
CREATE UNIQUE INDEX IF NOT EXISTS cron_schedule_one_pending
    ON cron.schedule (schedule_id) WHERE status = 'pending';

CREATE INDEX IF NOT EXISTS idx_cron_schedule_due ON cron.schedule (next_due)
    WHERE status IN ('pending', 'running');

COMMENT ON TABLE cron.schedule IS 'Cron schedule rows with lease-based claiming, keyed on (schedule_id, next_due). A run is claimed by setting owner and lease_until under the scheduler''s leader lock. On leader loss the lease expires and the next leader reclaims the same row, including its next_due, so a due slot is never scheduled twice.';
COMMENT ON COLUMN cron.schedule.owner IS 'Identity of the scheduler that claimed this run.';
COMMENT ON COLUMN cron.schedule.lease_until IS 'Lease expiry; after this another leader may reclaim the run. Renewed while it runs.';
COMMENT ON COLUMN cron.schedule.attempt IS 'Attempts made so far for this run; reset when a new next_due row is scheduled.';
COMMENT ON COLUMN cron.schedule.max_attempts IS 'Attempts after which a failing run is marked dead.';
COMMENT ON COLUMN cron.schedule.backoff IS 'Delay before the slot is retried after a failure. The retried run keeps its next_due.';
COMMENT ON COLUMN cron.schedule.every IS 'Schedule interval of a recurring job; a run that succeeds schedules the next next_due. NULL for one-off runs.';
COMMENT ON COLUMN cron.schedule.status IS 'Run state: pending, running, succeeded, failed, dead. A failed run is retried after backoff; the run is marked dead at max_attempts.';
