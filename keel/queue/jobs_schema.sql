-- Job table used by Jobs. {{table}} is the relation name and {{name}} its
-- last part; JobsDDL substitutes both. Apply through migrate.
--
-- state is one of pending, running, done, dead. A dead row is the dead-letter
-- queue: it stays in the table with last_error until Requeue or deletion.
CREATE TABLE IF NOT EXISTS {{table}} (
    id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kind             text        NOT NULL,
    payload          jsonb       NOT NULL DEFAULT '{}',
    idempotency_key  text,
    state            text        NOT NULL DEFAULT 'pending'
                     CHECK (state IN ('pending', 'running', 'done', 'dead')),
    attempts         integer     NOT NULL DEFAULT 0,
    max_attempts     integer     NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    run_at           timestamptz NOT NULL DEFAULT now(),
    lease_epoch      bigint      NOT NULL DEFAULT 0,
    lease_expires_at timestamptz,
    result           jsonb,
    last_error       text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    finished_at      timestamptz
);

-- Enqueue deduplicates on (kind, idempotency_key).
CREATE UNIQUE INDEX IF NOT EXISTS {{name}}_idem_idx
    ON {{table}} (kind, idempotency_key) WHERE idempotency_key IS NOT NULL;
-- Claim reads due pending rows in run order.
CREATE INDEX IF NOT EXISTS {{name}}_pending_idx
    ON {{table}} (run_at, id) WHERE state = 'pending';
-- Claim, Reap and Stuck read running rows by lease expiry.
CREATE INDEX IF NOT EXISTS {{name}}_lease_idx
    ON {{table}} (lease_expires_at) WHERE state = 'running';
-- Throughput and DeadLetters read finished rows by time.
CREATE INDEX IF NOT EXISTS {{name}}_finished_idx
    ON {{table}} (finished_at) WHERE finished_at IS NOT NULL;
