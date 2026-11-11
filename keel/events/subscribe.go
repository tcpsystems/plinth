package events

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// PostgresSchema returns the DDL that backs PostgresBus, for a migrator to
// apply: the event table, its sequence, the topic index and the trigger that
// notifies listeners about rows inserted by anyone.
//
// The trigger is a safety net under Publish: rows written by another writer,
// or by a Publish whose NOTIFY did not arrive, still wake subscribers. Apply
// the whole string in one transaction, so no subscriber reads rows before
// the trigger that announces them exists.
func PostgresSchema(channel string) string {
	if !identRe.MatchString(channel) {
		channel = DefaultChannel
	}
	return `CREATE SCHEMA IF NOT EXISTS events;
CREATE SEQUENCE IF NOT EXISTS events.event_seq AS bigint;
CREATE TABLE IF NOT EXISTS events.event (
    seq        bigint      PRIMARY KEY,
    topic      text        NOT NULL,
    kind       text        NOT NULL,
    ref        text        NOT NULL DEFAULT '',
    payload    jsonb,
    emitted_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS event_topic_seq_idx ON events.event (topic text_pattern_ops, seq);
CREATE OR REPLACE FUNCTION events.notify() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('` + channel + `', max(seq)::text) FROM new_rows;
    RETURN NULL;
END $$;
DROP TRIGGER IF EXISTS event_notify ON events.event;
CREATE TRIGGER event_notify AFTER INSERT ON events.event
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION events.notify();`
}

// wait is how long an idle subscriber blocks on one notification before it
// re-checks, so a missed wake still resolves quickly.
const wait = 250 * time.Millisecond

// Subscribe implements Bus.
func (b *PostgresBus) Subscribe(ctx context.Context, topic string, cursor uint64) (*Subscription, error) {
	if err := ValidateTopic(topic); err != nil {
		return nil, err
	}
	s, err := b.init()
	if err != nil {
		return nil, err
	}
	// Listen before the first read: an event committed while the backlog is
	// read is then either in that read or announced afterwards, never
	// missed between the two.
	src := newNotifySource()
	removeLocal := s.source.Attach(src.announce)
	release, err := s.listen(ctx, src)
	if err != nil {
		removeLocal()
		return nil, err
	}
	evch := make(chan Event, 64)
	errch := make(chan error, 1)
	done := make(chan struct{})
	var once sync.Once
	sub := &Subscription{Events: evch, Errors: errch}
	sub.close = func() {
		once.Do(func() {
			close(done)
			src.Close()
			removeLocal()
			release()
		})
	}
	go s.produce(ctx, done, src, topic, cursor, evch, errch)
	return sub, nil
}

// listen makes sure a notification for anything published after this point
// reaches src: the bus source covers this process, and a dedicated LISTEN
// connection covers NOTIFY from every publisher on the database when the
// listener handle's driver carries a Source.
func (s *busState) listen(ctx context.Context, src *notifySource) (func(), error) {
	lctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	conn, err := s.listener.Conn(lctx)
	if err != nil {
		return nil, err
	}
	release := func() { _ = conn.Close() }
	if _, err := conn.ExecContext(lctx, `LISTEN `+s.channel); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// produce reads the backlog, then waits for announcements and polls, reading
// every event after its cursor in order.
func (s *busState) produce(ctx context.Context, done <-chan struct{}, src *notifySource, topic string, cursor uint64, evch chan<- Event, errch chan<- error) {
	defer close(evch)
	defer close(errch)
	var poll <-chan time.Time
	if s.poll > 0 {
		t := time.NewTicker(s.poll)
		defer t.Stop()
		poll = t.C
	}
	for {
		for {
			batch, err := s.read(ctx, topic, cursor, s.drain)
			if err != nil {
				select {
				case errch <- err:
				case <-done:
				}
				return
			}
			for _, e := range batch {
				select {
				case evch <- e:
					cursor = e.Seq
				case <-done:
					return
				}
			}
			if len(batch) < s.drain {
				break
			}
		}
		select {
		case <-done:
			return
		case <-poll:
		}
		// Wait for an announcement or a poll. The read that starts the next
		// round happens even without one: that is what makes a lost
		// notification harmless.
		src.sleepUntilEvent(done, wait)
	}
}

// read returns up to limit events of topic or below it with Seq > cursor, in
// Seq order.
func (s *busState) read(ctx context.Context, topic string, cursor uint64, limit int) ([]Event, error) {
	rctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	rows, err := s.db.QueryContext(rctx, `
SELECT seq, topic, kind, ref, payload, emitted_at
FROM events.event
WHERE seq > $1 AND (topic = $2 OR starts_with(topic, $2 || '/'))
ORDER BY seq
LIMIT $3`, int64(cursor), topic, limit)
	if err != nil {
		return nil, fmt.Errorf("events: read: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Event
	for rows.Next() {
		var (
			seq int64
			e   Event
			p   []byte
		)
		if err := rows.Scan(&seq, &e.Topic, &e.Kind, &e.Ref, &p, &e.EmittedAt); err != nil {
			return nil, fmt.Errorf("events: read: %w", err)
		}
		e.Seq = uint64(seq)
		if p != nil {
			e.Payload = p
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
