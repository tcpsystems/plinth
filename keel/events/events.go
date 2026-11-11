// Package events is the shared event bus: services publish events to a topic
// and any number of consumers subscribe from a cursor.
//
// Delivery semantics follow keel/events/README.md. Every event carries a
// bus-wide Seq assigned when it is stored; Seq rises with publication order,
// within a topic and across topics. Delivery is at least once and ordered by
// Seq, so consumers dedupe on Seq and resume by passing the last Seq they
// handled back to Subscribe.
//
// PostgresBus is the default implementation. It keeps every event in the
// events.event table and uses LISTEN/NOTIFY only as a "new events" hint: the
// notification carries the highest Seq written (ids, never payloads), Publish
// calls made close together are coalesced into one commit and one NOTIFY
// inside a 10-20 ms window, and subscribers read the table. A lost,
// duplicated or late notification therefore costs latency or a re-read, never
// an event. The notification is sent after commit, so every committed Seq is
// readable when a subscriber wakes; a subscriber that is slow or was asleep
// reads its whole backlog on waking.
package events

import (
	"context"
	"errors"
	"regexp"
	"time"
)

// Event is one published event.
type Event struct {
	Seq       uint64    `json:"seq"`
	Topic     string    `json:"topic"`
	Kind      string    `json:"kind"`
	Ref       string    `json:"ref,omitempty"`
	Payload   []byte    `json:"payload,omitempty"`
	EmittedAt time.Time `json:"emitted_at"`
}

// Batch is the unit Publish writes: every event in a batch is committed by
// one statement, so a batch is the transaction boundary for ordering.
type Batch struct {
	Topic  string
	Events []Event
}

// Bus publishes events and delivers them to subscribers.
//
// Publish commits its batches (at least once: a retry after a failed commit
// may leave duplicates) and returns the events with their assigned Seq, in
// batch order. Subscribe hands back a Subscription whose Events channel yields
// every event of the given topics with Seq > cursor — first those already
// stored, then live ones — without gaps or duplicates. Topics are
// slash-separated paths ("acct/42/run/7"); a subscription to "a/b" also
// receives "a/b/c".
type Bus interface {
	Publish(ctx context.Context, batches ...Batch) ([]Event, error)
	Subscribe(ctx context.Context, topic string, cursor uint64) (*Subscription, error)
}

// Subscription streams events from one Subscribe call.
type Subscription struct {
	// Events yields the events in Seq order. It is closed by Close and after
	// an unrecoverable read error (which has already gone to Errors).
	Events <-chan Event
	// Errors reports a failure that ended the stream (the database went
	// away and could not be reached again); Events is closed after a value
	// is sent. It is closed by Close. Buffered, so the bus never blocks on it.
	Errors <-chan error

	close func()
}

// Close ends the subscription and releases its LISTEN connection. Events may
// still hold buffered events; Close does not drain them. Safe to call more
// than once.
func (s *Subscription) Close() {
	if s != nil {
		s.close()
	}
}

var (
	// ErrEmptyTopic is returned when a topic is empty.
	ErrEmptyTopic = errors.New("events: empty topic")
	// ErrClosed reports use of a closed bus.
	ErrClosed = errors.New("events: bus closed")
)

var (
	topicRe = regexp.MustCompile(`^[A-Za-z0-9_.:~@+-]+(/[A-Za-z0-9_.:~@+-]+)*$`)
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// ValidateTopic reports whether topic is usable: a non-empty slash-separated
// path of printable identifiers.
func ValidateTopic(topic string) error {
	if topic == "" {
		return ErrEmptyTopic
	}
	if !topicRe.MatchString(topic) {
		return errors.New("events: topic " + quote(topic) + ` must be slash-separated identifiers ([A-Za-z0-9_.:~@+-])`)
	}
	return nil
}

func quote(s string) string {
	const max = 64
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}
