// Package outbox records events in the caller's transaction and relays them
// through the keel job queue.
//
// Write inserts the event row and a relay job in the same transaction, so the
// state change, the event and the job commit or roll back together. A worker
// then loads the row, publishes it and marks it published. Publishing happens
// outside any transaction, so a slow or down broker holds no row locks and
// blocks no writers; failed publishes are retried by the queue with bounded
// exponential backoff, and a job that exhausts its attempts goes to the queue's
// dead state while the row stays unpublished for inspection.
//
// Delivery is at least once and not ordered across events: Publisher must be
// idempotent on Event.ID, and consumers that need order should sort by the
// event's Key and creation time themselves.
package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/riverqueue/river"

	"github.com/tcpsystems/plinth/keel/queue"
)

// Event is one outbox row.
type Event struct {
	ID      string            // generated when empty
	Topic   string            // required
	Key     string            // ordering/partition key
	Payload json.RawMessage   // required, valid JSON
	Headers map[string]string // optional metadata
}

// NewID returns a random 128-bit hex id.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Publisher delivers one event to a broker or consumer. It must be idempotent
// on Event.ID: delivery is at least once.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Args is the relay job. It names the outbox row by id; the row holds the
// event. EventID is the job's idempotency key, so writing the same event twice
// enqueues one job.
type Args struct {
	EventID string `json:"event_id" river:"unique"`
}

// Kind implements river.JobArgs.
func (Args) Kind() string { return "keel.outbox.relay" }

// Writer writes events and enqueues their relay jobs.
type Writer struct {
	Queue *queue.Client
}

// Write inserts e and its relay job using tx and returns the event id. It
// never commits: if the caller rolls back, neither the row nor the job exists.
func (w Writer) Write(ctx context.Context, tx *sql.Tx, e Event) (string, error) {
	if tx == nil {
		return "", errors.New("outbox: nil transaction")
	}
	if w.Queue == nil {
		return "", errors.New("outbox: nil queue client")
	}
	if e.Topic == "" || !json.Valid(e.Payload) {
		return "", errors.New("outbox: topic and valid JSON payload are required")
	}
	if e.ID == "" {
		e.ID = NewID()
	}
	h := []byte("{}")
	if e.Headers != nil {
		var err error
		if h, err = json.Marshal(e.Headers); err != nil {
			return "", err
		}
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO outbox (id, topic, key, payload, headers) VALUES ($1, $2, $3, $4, $5)`,
		e.ID, e.Topic, e.Key, []byte(e.Payload), h); err != nil {
		return "", fmt.Errorf("outbox: insert: %w", err)
	}
	if _, err := w.Queue.InsertTx(ctx, tx, Args{EventID: e.ID}, queue.Idempotent(nil)); err != nil {
		return "", fmt.Errorf("outbox: enqueue relay: %w", err)
	}
	return e.ID, nil
}

// Register adds the relay worker to workers. The worker publishes through pub
// using db for the event row; it holds no transaction while publishing.
func Register(workers *river.Workers, db *sql.DB, pub Publisher) error {
	if workers == nil || db == nil || pub == nil {
		return errors.New("outbox: workers, database and publisher are required")
	}
	return river.AddWorkerSafely(workers, &relayWorker{db: db, pub: pub})
}

type relayWorker struct {
	river.WorkerDefaults[Args]
	db  *sql.DB
	pub Publisher
}

// Work publishes one event. A row that is already published or gone is not an
// error: the job is a duplicate or the row was removed on purpose.
func (w *relayWorker) Work(ctx context.Context, job *river.Job[Args]) error {
	var (
		e    Event
		p, h []byte
	)
	err := w.db.QueryRowContext(ctx,
		`SELECT id, topic, key, payload, headers FROM outbox WHERE id = $1 AND published_at IS NULL`,
		job.Args.EventID).Scan(&e.ID, &e.Topic, &e.Key, &p, &h)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("outbox: load %s: %w", job.Args.EventID, err)
	}
	e.Payload = json.RawMessage(p)
	if err := json.Unmarshal(h, &e.Headers); err != nil {
		return river.JobCancel(fmt.Errorf("outbox: bad headers on %s: %w", e.ID, err))
	}
	if err := w.pub.Publish(ctx, e); err != nil {
		if _, uerr := w.db.ExecContext(ctx, `UPDATE outbox SET attempts = attempts + 1 WHERE id = $1`, e.ID); uerr != nil {
			return errors.Join(err, uerr)
		}
		return err
	}
	if _, err := w.db.ExecContext(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, e.ID); err != nil {
		return fmt.Errorf("outbox: mark %s published: %w", e.ID, err)
	}
	return nil
}
