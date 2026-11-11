-- Job table claimed by Queue (Default schema). Apply with migrate: the whole
-- file runs in one transaction.
--
-- The claimable set is "pending and due", so the claim index leads with state,
-- then the claim order (priority, next_due, id). A partial index would be
-- smaller, but an index on state also serves reclaimed and settled rows.
CREATE TABLE IF NOT EXISTS job (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    priority    integer     NOT NULL DEFAULT 100,
    next_due    timestamptz NOT NULL DEFAULT now(),
    state       text        NOT NULL DEFAULT 'pending',
    lease_epoch bigint      NOT NULL DEFAULT 0,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS job_claim_idx ON job (state, priority, next_due, id);
