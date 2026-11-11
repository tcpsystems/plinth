-- Idempotency records. Safe to run under a non-owner migration role.
CREATE TABLE IF NOT EXISTS idempotency_key (
    key          text        PRIMARY KEY,
    request_hash text        NOT NULL,
    status       integer     NOT NULL,
    content_type text        NOT NULL DEFAULT '',
    body         bytea       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    expires_at   timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS idempotency_key_expires_at_idx ON idempotency_key (expires_at);
