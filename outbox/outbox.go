// Package outbox writes events to the outbox table in the caller's transaction,
// so state change and event commit or roll back together, and defines the Relay
// that publishes them afterwards.
package outbox

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

// Write inserts e using tx and returns its id. It never commits.
func Write(ctx context.Context, tx *sql.Tx, e Event) (string, error) {
	if tx == nil {
		return "", errors.New("outbox: nil transaction")
	}
	if e.Topic == "" || !json.Valid(e.Payload) {
		return "", errors.New("outbox: topic and valid JSON payload are required")
	}
	if e.ID == "" {
		e.ID = NewID()
	}
	h, err := json.Marshal(e.Headers)
	if err != nil {
		return "", err
	}
	if e.Headers == nil {
		h = []byte("{}")
	}
	_, err = tx.ExecContext(ctx,
		`INSERT INTO outbox (id, topic, key, payload, headers) VALUES ($1, $2, $3, $4, $5)`,
		e.ID, e.Topic, e.Key, []byte(e.Payload), h)
	if err != nil {
		return "", fmt.Errorf("outbox: insert: %w", err)
	}
	return e.ID, nil
}

// Publisher delivers one event to a broker or consumer. It must be idempotent
// on Event.ID: delivery is at least once.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Relay moves pending outbox rows to a Publisher.
type Relay interface {
	// Poll publishes up to batch pending events oldest first and returns how
	// many were published. It stops at the first publish error.
	Poll(ctx context.Context, batch int) (int, error)
}

// SQLRelay is the database/sql Relay. Concurrent relays are safe: rows are
// claimed with FOR UPDATE SKIP LOCKED inside one transaction.
type SQLRelay struct {
	DB  *sql.DB
	Pub Publisher
}

// Poll implements Relay.
func (r SQLRelay) Poll(ctx context.Context, batch int) (n int, err error) {
	if batch < 1 {
		batch = 100
	}
	tx, err := r.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	rows, err := tx.QueryContext(ctx,
		`SELECT id, topic, key, payload, headers FROM outbox WHERE published_at IS NULL ORDER BY created_at, id LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return 0, err
	}
	var evs []Event
	for rows.Next() {
		var e Event
		var p, h []byte
		if err = rows.Scan(&e.ID, &e.Topic, &e.Key, &p, &h); err != nil {
			rows.Close()
			return 0, err
		}
		e.Payload = json.RawMessage(p)
		if err = json.Unmarshal(h, &e.Headers); err != nil {
			rows.Close()
			return 0, err
		}
		evs = append(evs, e)
	}
	if err = rows.Err(); err != nil {
		return 0, err
	}
	rows.Close()
	var pubErr error
	for _, e := range evs {
		if pubErr = r.Pub.Publish(ctx, e); pubErr != nil {
			if _, err = tx.ExecContext(ctx, `UPDATE outbox SET attempts = attempts + 1 WHERE id = $1`, e.ID); err != nil {
				return 0, err
			}
			break
		}
		if _, err = tx.ExecContext(ctx, `UPDATE outbox SET published_at = now() WHERE id = $1`, e.ID); err != nil {
			return 0, err
		}
		n++
	}
	if err = tx.Commit(); err != nil {
		return 0, err
	}
	return n, pubErr
}
