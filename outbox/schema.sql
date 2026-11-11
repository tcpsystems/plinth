-- Transactional outbox. Rows are written in the caller's transaction and
-- published by a Relay; published_at IS NULL marks pending rows.
CREATE TABLE IF NOT EXISTS outbox (
    id           text        PRIMARY KEY,
    topic        text        NOT NULL,
    key          text        NOT NULL DEFAULT '',
    payload      jsonb       NOT NULL,
    headers      jsonb       NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    attempts     integer     NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (created_at) WHERE published_at IS NULL;
