-- Runtime settings. Writers should bump version and NOTIFY the channel with the key.
CREATE TABLE IF NOT EXISTS setting (
    key        text        PRIMARY KEY,
    value      jsonb       NOT NULL,
    version    bigint      NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by text        NOT NULL DEFAULT ''
);
