// Package sse provides a server-sent events (SSE) broker and handler for
// Go's net/http. Events are backed by the outbox table so clients can
// resume from any point in history via Last-Event-ID (up to 15 minutes).
//
// The handler writes standard SSE with:
//
//   - id: the outbox row's event ID (acts as sequence).
//   - event: the event type from the AsyncAPI registry (e.g. "card.created").
//   - data: JSON payload per schema.
//
// A `: ping` heartbeat is sent every 15 seconds. Connections without an
// Event-Source client default to a 3-second retry interval; the handler
// emits `retry: 3000` for browsers.
//
// Slow consumers (write blocked beyond SlowWriteTimeout) are dropped with a
// `resync` event so the client can reconnect and catch up.
package sse

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// Defaults.
const (
	HeartbeatInterval  = 15 * time.Second // : ping interval
	RetryMs            = 3000             // retry: directive in ms
	ResumeRetention    = 15 * time.Minute // how far back Last-Event-ID can reach
	DefaultBufferSize  = 256              // per-subscriber bounded buffer
	DefaultSlowTimeout = 30 * time.Second // how long a write may block before drop
)

// sseFrame is one frame written to an SSE connection.
type sseFrame struct {
	id    string
	event string
	data  json.RawMessage
}

// Broker dispatches outbox events to SSE subscribers via Postgres LISTEN/NOTIFY.
//
// It polls the outbox table for new events and fans them to connected
// subscribers through bounded channels. When a subscriber's buffer is full,
// the subscriber is flagged as slow and a resync event is written to its
// stream.
//
// Call [NewBroker] then [Broker.Start] to begin listening. Call [Broker.Stop]
// to shut down cleanly.
type Broker struct {
	Db          *sql.DB
	Channel     string // LISTEN channel (default "sse")
	Logger      *slog.Logger
	BufferSize  int           // per-subscriber buffer (default DefaultBufferSize)
	PollPeriod  time.Duration // how often to poll outbox (default 500ms)
	mu          sync.Mutex
	subscribers []*subscriber
	stopped     chan struct{}
	once        sync.Once
}

type subscriber struct {
	buf    chan sseFrame
	slowCh chan struct{} // non-blocking signal when write would block
}

// NewBroker creates a broker backed by db. Call Start() to begin.
func NewBroker(db *sql.DB) *Broker {
	return &Broker{
		Db:         db,
		Channel:    "sse",
		Logger:     slog.Default(),
		BufferSize: DefaultBufferSize,
		PollPeriod: 500 * time.Millisecond,
		stopped:    make(chan struct{}),
	}
}

// Start begins polling the outbox and LISTEN on the channel.
func (b *Broker) Start(ctx context.Context) error {
	b.once.Do(func() {
		b.stopped = make(chan struct{})
		go b.listenLoop(ctx)
	})
	_, err := b.Db.ExecContext(ctx, fmt.Sprintf(`LISTEN %s`, b.Channel))
	return err
}

// Stop sends UNLISTEN and waits for the listener to exit.
func (b *Broker) Stop(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, err := b.Db.ExecContext(ctx, fmt.Sprintf(`UNLISTEN %s`, b.Channel))
	if err != nil {
		return err
	}
	close(b.stopped)
	return nil
}

func (b *Broker) listenLoop(ctx context.Context) {
	ticker := time.NewTicker(b.PollPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopped:
			return
		case <-ticker.C:
			b.dispatchPending(ctx)
		}
	}
}

// dispatchPending fetches up to 100 new outbox rows and fans them to subscribers.
func (b *Broker) dispatchPending(ctx context.Context) {
	b.mu.Lock()
	subs := make([]*subscriber, len(b.subscribers))
	copy(subs, b.subscribers)
	b.mu.Unlock()

	rows, err := b.Db.QueryContext(ctx,
		`SELECT id, topic, payload FROM outbox ORDER BY created_at, id LIMIT 100`)
	if err != nil {
		b.Logger.Debug("sse: query failed", "error", err)
		return
	}
	defer rows.Close()

	var frames []sseFrame
	for rows.Next() {
		var f sseFrame
		if err := rows.Scan(&f.id, &f.event, (*json.RawMessage)(&f.data)); err != nil {
			continue
		}
		frames = append(frames, f)
	}

	for _, frame := range frames {
		for _, sub := range subs {
			select {
			case sub.buf <- frame:
			default:
				// Buffer full — slow consumer. Signal for resync.
				select {
				case sub.slowCh <- struct{}{}:
				default:
				}
			}
		}
	}
}

// Subscribe registers a subscriber and returns it. Call Unregister when done.
func (b *Broker) Subscribe() *subscriber {
	b.mu.Lock()
	defer b.mu.Unlock()
	sub := &subscriber{
		buf:    make(chan sseFrame, b.BufferSize),
		slowCh: make(chan struct{}, 1),
	}
	b.subscribers = append(b.subscribers, sub)
	return sub
}

// Unregister removes a subscriber and closes its buffer channel.
func (b *Broker) Unregister(sub *subscriber) {
	b.mu.Lock()
	for i, s := range b.subscribers {
		if s == sub {
			b.subscribers = append(b.subscribers[:i], b.subscribers[i+1:]...)
			break
		}
	}
	b.mu.Unlock()
	close(sub.buf)
}

// Handler serves SSE connections on GET requests. It reads events from the
// Broker's subscribers and writes them to the HTTP response in SSE format.
//
// Usage:
//
//	mux.Handle("/events", &sse.Handler{Broker: broker})
type Handler struct {
	Broker      *Broker
	SlowTimeout time.Duration // write timeout before resync (default 30s)
	Logger      *slog.Logger
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("retry", strconv.Itoa(RetryMs))

	fmt.Fprintf(w, "retry: %d\n\n", RetryMs)
	flusher.Flush()

	// Last-Event-ID resume: replay events after the client's last seen id.
	lastID := r.Header.Get("Last-Event-ID")
	if lastID != "" {
		h.writeResume(w, r, lastID)
		flusher.Flush()
	}

	sub := h.Broker.Subscribe()
	defer h.Broker.Unregister(sub)

	slowTimeout := h.SlowTimeout
	if slowTimeout <= 0 {
		slowTimeout = DefaultSlowTimeout
	}

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	// A write that blocks longer than slowTimeout fails (the connection is
	// stuck) and the handler returns. Servers whose ResponseWriter does not
	// support deadlines fall back to the broker's slow-consumer drop.
	rc := http.NewResponseController(w)
	armDeadline := func() { _ = rc.SetWriteDeadline(time.Now().Add(slowTimeout)) }

	for {
		select {
		case <-r.Context().Done():
			return

		case frame, ok := <-sub.buf:
			if !ok {
				return
			}
			armDeadline()
			h.writeFrame(w, flusher, frame)

		case <-sub.slowCh:
			// Slow consumer — send resync and drop.
			armDeadline()
			h.writeFrame(w, flusher, sseFrame{
				event: "resync",
				data:  json.RawMessage(`{"reason":"slow-consumer"}`),
			})
			flusher.Flush()
			return

		case <-ticker.C:
			armDeadline()
			fmt.Fprint(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

// writeFrame writes one SSE frame to the response.
func (h *Handler) writeFrame(w http.ResponseWriter, flusher http.Flusher, frame sseFrame) {
	if frame.id != "" {
		fmt.Fprintf(w, "id: %s\n", frame.id)
	}
	if frame.event != "" {
		fmt.Fprintf(w, "event: %s\n", frame.event)
	}
	fmt.Fprintf(w, "data: %s\n\n", frame.data)
	flusher.Flush()
}

// writeResume replays events from the outbox after lastID, within retention.
func (h *Handler) writeResume(w http.ResponseWriter, r *http.Request, lastID string) {
	since := time.Now().Add(-ResumeRetention)
	rows, err := h.Broker.Db.QueryContext(r.Context(),
		`SELECT id, topic, payload FROM outbox WHERE id > $1 AND created_at >= $2 ORDER BY created_at, id LIMIT 50`,
		lastID, since)
	if err != nil {
		if h.Logger != nil {
			h.Logger.Debug("sse: resume query failed", "error", err)
		}
		return
	}
	defer rows.Close()

	for rows.Next() {
		var id, topic string
		var data json.RawMessage
		if err := rows.Scan(&id, &topic, &data); err != nil {
			continue
		}
		fmt.Fprintf(w, "id: %s\nevent: %s\ndata: %s\n\n", id, topic, data)
	}
}

// IsSlowConsumer reports whether err indicates a slow-consumer drop.
func IsSlowConsumer(err error) bool {
	return errors.Is(err, ErrSlowConsumer)
}

// ErrSlowConsumer is returned when a subscriber was dropped for being slow.
var ErrSlowConsumer = errors.New("sse: subscriber dropped (slow-consumer)")

// ConnIsClosed checks whether an SSE connection was dropped by the client.
func ConnIsClosed(err error) bool {
	if err == io.EOF {
		return true
	}
	if errors.Is(err, context.Canceled) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
