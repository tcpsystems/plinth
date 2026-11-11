package events

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DefaultChannel is the NOTIFY channel PostgresBus uses unless Channel is set.
const DefaultChannel = "events_notify"

// Coalescing and timing defaults for PostgresBus.
const (
	MinWindow      = 10 * time.Millisecond
	MaxWindow      = 20 * time.Millisecond
	DefaultWindow  = 15 * time.Millisecond
	DefaultBatch   = 1000
	DefaultDrain   = 500
	DefaultPoll    = time.Second
	DefaultTimeout = 30 * time.Second
)

// rowsPerInsert keeps one INSERT well under the protocol's bind parameter
// limit (six parameters per row).
const rowsPerInsert = 1000

// PostgresBus is the database/sql Bus: events live in events.event (see
// PostgresSchema and schema.sql) and LISTEN/NOTIFY wakes subscribers.
//
// Publish coalesces concurrent and near-simultaneous calls. Each call's events
// join a pending buffer; one flusher commits the buffer in call order, each
// call in its own transaction, so Seq order matches call order, then announces
// the highest Seq once. The flusher
// starts one Window after the first buffered event and runs at most every
// MaxWindow after that, so an event is committed and announced between
// MinWindow and MaxWindow after it reached Publish, and the writes in that
// window share one transaction.
//
// The announcement is a hint, never the data: subscribers read the table. A
// lost, duplicated or late notification costs latency or a re-read, never an
// event.
type PostgresBus struct {
	// DB commits publishes and reads stored events. Required.
	DB *sql.DB
	// Listener, when set, is a separate handle for LISTEN connections, so a
	// busy pool cannot starve subscribers. For notifications from other
	// processes its connections must be opened with a Source in their
	// context (WithSource); without one, subscribers learn of them on their
	// next Poll. Defaults to DB.
	Listener *sql.DB
	// Source is the in-process notification channel. Publish announces on
	// it, which wakes this bus's subscribers immediately; the NOTIFY
	// statement (and PostgresSchema's trigger) reaches them through the
	// LISTEN connection. Use one Source per process, open the Listener's
	// connections with WithSource in its context and set it here. When nil
	// the bus keeps its own, which still serves subscribers of this bus.
	// Optional.
	Source *Source
	// Channel is the NOTIFY channel; every publisher and every trigger or
	// function that raises notifications must use the same one. Defaults to
	// DefaultChannel.
	Channel string
	// Window is the coalescing window, clamped to [MinWindow, MaxWindow].
	// The zero value means DefaultWindow.
	Window time.Duration
	// BatchSize caps the rows in one flush. The zero value means
	// DefaultBatch.
	BatchSize int
	// Drain caps how many stored events a subscriber reads per query. The
	// zero value means DefaultDrain.
	Drain int
	// Poll re-reads the table on this interval even without a notification,
	// covering a publisher whose notification was lost. The zero value means
	// DefaultPoll; a negative value relies on notifications alone.
	Poll time.Duration
	// Timeout bounds each database call made on behalf of Publish and
	// Subscribe. The zero value means DefaultTimeout.
	Timeout time.Duration

	once  sync.Once
	state *busState
}

type settings struct {
	channel string
	window  time.Duration
	batch   int
	drain   int
	poll    time.Duration
	timeout time.Duration
}

// busState is PostgresBus resolved and running.
type busState struct {
	db       *sql.DB
	listener *sql.DB
	source   *Source
	settings

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	qmu     sync.Mutex
	pending []*inflight
}

// inflight is one Publish call waiting for, or inside, a flush.
type inflight struct {
	batches []Batch
	res     []Event // last event written per batch, in batch order
	full    []Event
	err     error
	done    chan struct{}
}

func (b *PostgresBus) init() (*busState, error) {
	b.once.Do(func() {
		s := &busState{db: b.DB, listener: b.Listener, source: b.Source, settings: settings{
			channel: b.Channel, window: b.Window, batch: b.BatchSize,
			drain: b.Drain, poll: b.Poll, timeout: b.Timeout,
		}}
		if s.listener == nil {
			s.listener = b.DB
		}
		if s.source == nil {
			s.source = NewSource()
		}
		s.applyDefaults()
		s.wake = make(chan struct{}, 1)
		s.stop = make(chan struct{})
		s.done = make(chan struct{})
		go s.run()
		b.state = s
	})
	s := b.state
	if s.db == nil {
		return nil, errors.New("events: PostgresBus.DB is required")
	}
	if !identRe.MatchString(s.channel) {
		return nil, errors.New("events: channel " + quote(s.channel) + " must be an identifier (letters, digits, underscore)")
	}
	return s, nil
}

func (s *settings) applyDefaults() {
	if s.channel == "" {
		s.channel = DefaultChannel
	}
	switch {
	case s.window == 0:
		s.window = DefaultWindow
	case s.window < MinWindow:
		s.window = MinWindow
	case s.window > MaxWindow:
		s.window = MaxWindow
	}
	if s.batch <= 0 {
		s.batch = DefaultBatch
	}
	if s.drain <= 0 {
		s.drain = DefaultDrain
	}
	if s.poll == 0 {
		s.poll = DefaultPoll
	}
	if s.timeout <= 0 {
		s.timeout = DefaultTimeout
	}
}

// Publish implements Bus.
func (b *PostgresBus) Publish(ctx context.Context, batches ...Batch) ([]Event, error) {
	if len(batches) == 0 {
		return nil, nil
	}
	s, err := b.init()
	if err != nil {
		return nil, err
	}
	for _, batch := range batches {
		if err := ValidateTopic(batch.Topic); err != nil {
			return nil, err
		}
		if len(batch.Events) == 0 {
			return nil, errors.New("events: batch for topic " + quote(batch.Topic) + " is empty")
		}
		for _, e := range batch.Events {
			if e.Kind == "" {
				return nil, errors.New("events: an event on topic " + quote(batch.Topic) + " has no kind")
			}
		}
	}
	p := &inflight{batches: batches, res: make([]Event, len(batches)), done: make(chan struct{})}
	s.qmu.Lock()
	s.pending = append(s.pending, p)
	s.qmu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
	select {
	case <-p.done:
		return p.full, p.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close flushes events still buffered by Publish, waits for subscribers to
// finish and ends the flusher. Safe to call more than once; does nothing when
// the bus was never used.
func (b *PostgresBus) Close() {
	s := b.state
	if s == nil {
		return
	}
	select {
	case <-s.stop:
	default:
		close(s.stop)
	}
	<-s.done
}

// maxCallsPerFlush bounds the Publish calls one flush chunk covers.
const maxCallsPerFlush = 256

// run is the flusher. The first buffered event starts a Window; a flush that
// found more work is followed by another at most MaxWindow later.
func (s *busState) run() {
	defer close(s.done)
	for {
		select {
		case <-s.wake:
		case <-s.stop:
			s.drainPending()
			return
		}
		t := time.NewTimer(s.window)
		select {
		case <-t.C:
		case <-s.stop:
			t.Stop()
			s.drainPending()
			return
		}
		for s.flushOnce() > 0 {
			t := time.NewTimer(MaxWindow)
			select {
			case <-t.C:
			case <-s.stop:
				t.Stop()
				s.drainPending()
				return
			}
		}
	}
}

// drainPending flushes everything still buffered at shutdown.
func (s *busState) drainPending() {
	for s.flushOnce() > 0 {
	}
}

// flushOnce takes the next chunk of pending calls, flushes it and reports how
// many calls it covered (0 when idle).
func (s *busState) flushOnce() int {
	s.qmu.Lock()
	n, rows := 0, 0
	for n < len(s.pending) && n < maxCallsPerFlush {
		r := 0
		for _, bt := range s.pending[n].batches {
			r += len(bt.Events)
		}
		if n > 0 && rows+r > s.batch {
			break
		}
		rows += r
		n++
	}
	if n == 0 {
		s.qmu.Unlock()
		return 0
	}
	ps := s.pending[:n:n]
	s.pending = s.pending[n:]
	s.qmu.Unlock()
	s.flush(ps)
	return n
}

// flush commits ps in call order, each call in its own transaction, then
// announces the highest Seq once. A call that fails reports its error; the
// later calls still run.
func (s *busState) flush(ps []*inflight) {
	var last uint64
	for _, p := range ps {
		ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
		l, ok := s.commit(ctx, p)
		cancel()
		if ok {
			last = max(last, l)
		}
	}
	if last > 0 {
		s.announce(last)
	}
}

// announce tells subscribers about committed Seq seq. Local subscribers hear
// it on the source; every LISTEN connection on the database hears the NOTIFY.
// A failed NOTIFY only costs subscribers latency until their next Poll.
func (s *busState) announce(seq uint64) {
	if s.source != nil {
		s.source.announce(seq)
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.timeout)
	defer cancel()
	_, _ = s.db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, s.channel, strconv.FormatUint(seq, 10))
}

// commit writes p's batches in one transaction and completes p.
func (s *busState) commit(ctx context.Context, p *inflight) (last uint64, ok bool) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		finish(p, nil, err)
		return 0, false
	}
	var full []Event
	for j, batch := range p.batches {
		evs, err := writeBatch(ctx, tx, batch)
		if err != nil {
			_ = tx.Rollback()
			finish(p, nil, err)
			return 0, false
		}
		full = append(full, evs...)
		p.res[j] = evs[len(evs)-1]
		last = max(last, evs[len(evs)-1].Seq)
	}
	if err := tx.Commit(); err != nil {
		finish(p, nil, err)
		return 0, false
	}
	finish(p, full, nil)
	return last, true
}

func finish(p *inflight, full []Event, err error) {
	p.full, p.err = full, err
	close(p.done)
}

// writeBatch inserts one batch and returns it with Seq and topic filled in.
// Seq is reserved with the rows inside this transaction, so concurrent
// publishers write distinct Seqs in commit order.
func writeBatch(ctx context.Context, tx *sql.Tx, batch Batch) ([]Event, error) {
	out := make([]Event, len(batch.Events))
	now := time.Now().UTC()
	for i, e := range batch.Events {
		e.Topic = batch.Topic
		if e.EmittedAt.IsZero() {
			e.EmittedAt = now
		}
		out[i] = e
	}
	for i := 0; i < len(out); i += rowsPerInsert {
		chunk := out[i:min(i+rowsPerInsert, len(out))]
		var first int64
		if err := tx.QueryRowContext(ctx, `SELECT nextval('events.event_seq', $1)`, len(chunk)).Scan(&first); err != nil {
			return nil, fmt.Errorf("events: reserve seq: %w", err)
		}
		if err := insertBlock(ctx, tx, chunk, first); err != nil {
			return nil, err
		}
		for j := range chunk {
			chunk[j].Seq = uint64(first + int64(j))
		}
	}
	return out, nil
}

func insertBlock(ctx context.Context, tx *sql.Tx, chunk []Event, first int64) error {
	var q strings.Builder
	q.WriteString(`INSERT INTO events.event (seq, topic, kind, ref, payload, emitted_at) VALUES `)
	args := make([]any, 0, 6*len(chunk))
	for i, e := range chunk {
		if i > 0 {
			q.WriteString(",")
		}
		fmt.Fprintf(&q, "($%d,$%d,$%d,$%d,$%d,$%d)", 6*i+1, 6*i+2, 6*i+3, 6*i+4, 6*i+5, 6*i+6)
		args = append(args, first+int64(i), e.Topic, e.Kind, e.Ref, payloadOf(e.Payload), e.EmittedAt)
	}
	if _, err := tx.ExecContext(ctx, q.String(), args...); err != nil {
		return fmt.Errorf("events: insert: %w", err)
	}
	return nil
}

func payloadOf(p []byte) any {
	if len(p) == 0 {
		return nil
	}
	return p
}
