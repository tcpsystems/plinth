-- Shared event bus storage for PostgresBus. Apply with migrate: the whole
-- file runs in one transaction.
--
-- Seqs come from one sequence, so they are unique and dense for a single
-- publisher. Every row is published; there is no pending marker.
CREATE SCHEMA IF NOT EXISTS events;

CREATE SEQUENCE IF NOT EXISTS events.event_seq AS bigint;

CREATE TABLE IF NOT EXISTS events.event (
    seq        bigint      PRIMARY KEY,
    topic      text        NOT NULL,
    kind       text        NOT NULL,
    ref        text        NOT NULL DEFAULT '',
    payload    jsonb,
    emitted_at timestamptz NOT NULL
);

-- Topic prefixes: every read filters seq > cursor and topic = 'a/b' OR
-- starts_with(topic, 'a/b/'), ordered by seq, so an index on topic then seq
-- serves "this topic after the cursor" from an index scan.
CREATE INDEX IF NOT EXISTS event_topic_seq_idx ON events.event (topic text_pattern_ops, seq);

-- The notification trigger lives in PostgresSchema (events package), which
-- returns this DDL plus the trigger for the channel the bus listens on. A
-- migration that cannot call Go applies it as:
--
--   CREATE OR REPLACE FUNCTION events.notify() RETURNS trigger LANGUAGE plpgsql AS $$
--   BEGIN
--       PERFORM pg_notify('events_notify', max(seq)::text) FROM new_rows;
--       RETURN NULL;
--   END $$;
--   CREATE TRIGGER event_notify AFTER INSERT ON events.event
--       REFERENCING NEW TABLE AS new_rows
--       FOR EACH STATEMENT EXECUTE FUNCTION events.notify();
