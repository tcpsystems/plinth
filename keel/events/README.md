# events

The shared event bus. Services publish events to a topic; any number of
consumers subscribe from a cursor and receive every event once the cursor has
passed, in order.

```go
type Bus interface {
	Publish(ctx context.Context, batches ...Batch) ([]Event, error)
	Subscribe(ctx context.Context, topic string, cursor uint64) (*Subscription, error)
}
```

`PostgresBus` is the implementation: event storage in Postgres, delivered by
reading the table and woken by `LISTEN`/`NOTIFY`.

## Contract

**Envelope.** `Event{Seq, Topic, Kind, Ref, Payload, EmittedAt}`. `Seq` is a
bus-wide `uint64` assigned when the event is stored; `Topic` a slash-separated
path (`orders/42/line/7`), `Kind` a short name of what happened, `Ref` an
optional subject (`line/7`), `Payload` JSON bytes (optional), `EmittedAt` UTC, defaulted
to publish time.

**Order.** Seq rises with publication order: a `Publish` call returns the
events it stored with their Seq, in batch order, and later calls receive later
Seqs. Two concurrent `Publish` calls are ordered by when they reach the bus,
each call wholly before or after the other. Every subscriber sees the events
of its topics in Seq order.

**Delivery.** At least once. A subscriber receives every event of its topics
with `Seq > cursor` exactly once in a healthy process. If the database errors
on a commit, `Publish` returns the error and the call's events are not stored
(its batch is one transaction); a retry of a call whose result was lost can
store duplicates, so consumers that care dedupe on `Seq`.

**Topics.** A subscription to `a/b` also receives `a/b/c` and `a/bd` is not
included: the rule is `topic = 'a/b' OR starts_with(topic, 'a/b/')`. Topics
are `[A-Za-z0-9_.:~@+-]` segments separated by `/` (`ValidateTopic`).

**Resume.** Pass the last Seq handled as `cursor` (0 for the start). The
subscription first replays the stored backlog after the cursor, then follows
live events. Subscribing before reading removes the gap between "read the
backlog" and "hear of new events", and a periodic re-read covers a lost
notification, so a subscriber that waits and then resumes always sees
everything.

**Ending.** `Subscription.Close` stops delivery and releases its `LISTEN`
connection. If the stream ends because reads fail unrecoverably, the error is
sent on `Errors` and then `Events` is closed.

**Batching.** `Publish` takes batches: every event in a batch is written by
one transaction, so a batch is the unit that consumers can rely on being all
present or all absent.

## PostgresBus

```go
bus := &events.PostgresBus{DB: db}
defer bus.Close()
evs, err := bus.Publish(ctx, events.Batch{Topic: "orders/42", Events: []events.Event{{Kind: "order.placed", Payload: p}}})
sub, err := bus.Subscribe(ctx, "orders/42", cursor)
defer sub.Close()
for e := range sub.Events { /* handle e; remember e.Seq as the next cursor */ }
```

| Field      | Default        | Meaning |
|------------|----------------|---------|
| `DB`       | required       | commits publishes, serves reads |
| `Listener` | `DB`           | handle for `LISTEN` connections; give it its own pool so busy reads cannot starve subscribers |
| `Source`   | none           | in-process notification channel, see below |
| `Channel`  | `events_notify`| `NOTIFY` channel; every publisher, trigger and schema must agree |
| `Window`   | 15 ms          | coalescing window, clamped to 10-20 ms |
| `BatchSize`| 1000           | rows per flush |
| `Drain`    | 500            | rows per subscriber read |
| `Poll`     | 1 s            | re-read interval without a notification; negative turns polling off |
| `Timeout`  | 30 s           | per-database-call limit |

### Coalescing

`Publish` does not write inline. Calls join a buffer, and one flusher goroutine
commits the buffer in call order, then sends a single `NOTIFY` naming the
highest Seq stored. Log under load produces timestamps inside one window and
every event of a ticker merge landing close together become one commit and one
notification, so listeners see batches, not a wakeup per row.

- The first buffered call starts a `Window`; each flush is followed by another
  at most `MaxWindow` (20 ms) later while work remains, so an event is stored
  between `MinWindow` (10 ms) and `MaxWindow` (20 ms) after `Publish` (plus
  database time).
- Each call gets its own transaction (a bad payload fails only its call);
  the flush commits them in a row.
- `Publish` returns when its call's transaction committed or rolled back, with
  the events and their Seq, or the error.
- `Close` flushes whatever is buffered before returning.

### Wakeups, and why they are only hints

The `NOTIFY` payload is the highest stored Seq, never the event, because
notification payloads are limited to 8000 bytes. Subscribers read
`events.event`:

```sql
SELECT seq, topic, kind, ref, payload, emitted_at FROM events.event
WHERE seq > $1 AND (topic = $2 OR starts_with(topic, $2 || '/'))
ORDER BY seq LIMIT $3
```

The index `events.event (topic text_pattern_ops, seq)` serves it. A
notification that is lost, duplicated or late costs a poll interval of
latency or a repeated read, never an event.

`database/sql` has no portable way to deliver `LISTEN` notifications to Go
code. The bus therefore combines:

- a `Source` that `Publish` announces on after commit, waking the bus's own
  subscribers in the same process at once;
- a dedicated `LISTEN <channel>` connection per subscription (`Listener`),
  released by `Close`, for a driver that does deliver notifications;
- the periodic `Poll` re-read for everything else, including events written
  by other processes.

To connect a driver's notifications, give the bus a `Source` and open the
`Listener` connections with that source in their context
(`db.Conn(events.WithSource(ctx, src))`); the driver calls
`src.Attach(f)` once the connection is listening.

### Schema and trigger

`PostgresSchema(channel)` returns the DDL: the `events` schema, the
`events.event` table, the `events.event_seq` sequence and that table's topic
index, plus a statement-level `AFTER INSERT` trigger that runs
`pg_notify(channel, max(seq))` for every insert made by anyone. It is a safety
net under `Publish`: rows written by another writer, or with a notification
that went missing, still wake every process that is listening. Apply it, or
the equivalent in `schema.sql`, in one transaction during migration.

## Testing

Tests run against `internal/fakesql` with a small in-memory model of the
table that executes the bus's own statements (transactions, the sequence,
inserts, reads with prefix matching, `pg_notify`), so no database or network
is involved. They cover the golden order across topics and publishers, resume
from arbitrary cursors under concurrent publishing (a seeded property test),
coalescing, rollback of failed calls, and delivery by polling alone.
