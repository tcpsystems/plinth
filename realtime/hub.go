package realtime

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// HubOptions tunes the hub's memory bounds.
type HubOptions struct {
	// Retain is how many recent events the ring keeps for replay
	// (Last-Event-ID resume). Default 8192.
	Retain int
	// QueueSize is each subscription's buffer; a subscriber that cannot keep
	// up is dropped (its Reset closes) rather than blocking the publisher.
	// Default 256.
	QueueSize int
}

// Hub fans events out to subscribers and keeps a ring of recent events so
// reconnecting clients can resume with their last seen seq. It is safe for
// concurrent use. The hub holds no durable state: everything that matters
// must come from the service's outbox on restart.
type Hub struct {
	mu      sync.Mutex
	retain  int
	queue   int
	seq     uint64
	ring    []Event
	r, w, n int
	subs    map[*Subscription]struct{}
	closed  bool
}

// ErrClosed is returned by Publish after Close.
var ErrClosed = errors.New("realtime: hub closed")

// NewHub builds a hub with the given bounds (zero fields take the defaults).
func NewHub(o HubOptions) *Hub {
	if o.Retain <= 0 {
		o.Retain = 8192
	}
	if o.QueueSize <= 0 {
		o.QueueSize = 256
	}
	return &Hub{retain: o.Retain, queue: o.QueueSize, ring: make([]Event, o.Retain), subs: map[*Subscription]struct{}{}}
}

// Head returns the last assigned seq.
func (h *Hub) Head() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

// Publish assigns a seq if the event has none, retains it and delivers it to
// every matching subscriber. Events must have a Topic and a Kind; topics are
// slash-separated paths like "acct/42/run/7" and must not contain "*".
// Out-of-order seqs (a replay racing live traffic) are rejected so the
// single cursor space stays monotonic.
func (h *Hub) Publish(ev Event) (Event, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ev, ErrClosed
	}
	if ev.Topic == "" || ev.Kind == "" || strings.ContainsRune(ev.Topic, '*') {
		return ev, errors.New("realtime: event needs topic and kind, and topic must not contain *")
	}
	if ev.Seq == 0 {
		ev.Seq = h.seq + 1
	} else if ev.Seq <= h.seq {
		return ev, fmt.Errorf("realtime: seq %d not above head %d", ev.Seq, h.seq)
	}
	h.seq = ev.Seq
	h.retain1(ev)
	for s := range h.subs {
		if s.dropped || !s.matchesLocked(ev.Topic) {
			continue
		}
		select {
		case s.evch <- ev:
		default:
			s.dropLocked()
		}
	}
	return ev, nil
}

func (h *Hub) retain1(ev Event) {
	if h.n == h.retain {
		h.ring[h.w] = ev
		h.w = (h.w + 1) % h.retain
		h.r = h.w
		return
	}
	h.ring[h.w] = ev
	h.w = (h.w + 1) % h.retain
	h.n++
}

// Close ends every subscription; transports see Events closed and finish the
// stream, so callers can rely on the browser's own reconnect. Call it while
// shutting the process down.
func (h *Hub) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	for s := range h.subs {
		s.cancelLocked()
	}
	return nil
}

// Subscription is one client's view of the hub.
type Subscription struct {
	// Events carries replayed (if in range) then live events. It is closed
	// by Cancel or Hub.Close; draining it fully is the transport's job.
	Events <-chan Event
	// Reset closes when the subscription was dropped for backing up, or
	// immediately when the requested resume cursor is no longer retained.
	// Transports emit a reset notification and end the stream: the client
	// reconnects and resumes or re-snapshots.
	Reset <-chan struct{}

	h       *Hub
	evch    chan Event
	rst     chan struct{}
	t       []string
	done    bool // cancelled: evch closed
	dropped bool // overflowed: no further sends
	rstOnce bool // Reset already closed (gap at subscribe, or drop)
}

// Subscribe returns the subscription for the listed topics starting after
// cursor (0 for a fresh stream). Patterns end with "*" (prefix) or are "*".
// Events with cursor < seq <= head that are still in the ring are replayed
// into the buffer before live delivery; when they are not (or overflow the
// buffer) Reset is closed already and the subscription delivers live events
// from the current head.
func (h *Hub) Subscribe(topics []string, cursor uint64) *Subscription {
	s := &Subscription{h: h, t: append([]string(nil), topics...), evch: make(chan Event, h.queue), rst: make(chan struct{})}
	s.Events, s.Reset = s.evch, s.rst
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		s.closeResetLocked()
		s.cancelLocked() // marks done, so a later Cancel cannot re-close evch
		return s
	}
	if cursor > 0 && cursor < h.seq && h.n > 0 {
		lo := h.ring[h.r].Seq
		if cursor+1 < lo {
			s.closeResetLocked() // the ring no longer holds events after the cursor
		} else {
			n := 0
			for i := 0; i < h.n; i++ {
				e := h.ring[(h.r+i)%h.retain]
				if e.Seq > cursor && s.matchesLocked(e.Topic) {
					n++
				}
			}
			if n > cap(s.evch) {
				s.closeResetLocked() // the replay cannot fit the buffer
			} else {
				for i := 0; i < h.n; i++ {
					e := h.ring[(h.r+i)%h.retain]
					if e.Seq > cursor && s.matchesLocked(e.Topic) {
						s.evch <- e
					}
				}
			}
		}
	}
	h.subs[s] = struct{}{}
	return s
}

// Cancel ends the subscription; Events closes. Safe to call more than once.
func (s *Subscription) Cancel() {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()
	s.cancelLocked()
}

// ErrCursorGone reports that a resume cursor is no longer backed by the
// ring: the client must take a fresh snapshot instead of replaying.
var ErrCursorGone = errors.New("realtime: resume cursor no longer retained")

// Rewind replays ring events with seq > cursor that match the
// subscription's topics into its buffer, for a WebSocket client resuming
// mid-session. It acquires the hub lock itself — callers must not hold it.
// When the events are not retained anymore, or do not fit the buffer, it
// replays nothing and returns [ErrCursorGone]; the subscription stays alive
// and live delivery continues, so the transport announces a reset in place
// rather than dropping the session. A cursor at or above the head is a
// no-op success.
func (s *Subscription) Rewind(cursor uint64) error {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()
	if s.done || s.dropped {
		return nil // the transport is ending this session anyway
	}
	if cursor >= s.h.seq || s.h.n == 0 {
		return nil
	}
	lo := s.h.ring[s.h.r].Seq
	if cursor+1 < lo {
		return ErrCursorGone
	}
	n := 0
	for i := 0; i < s.h.n; i++ {
		e := s.h.ring[(s.h.r+i)%s.h.retain]
		if e.Seq > cursor && s.matchesLocked(e.Topic) {
			n++
		}
	}
	if n > cap(s.evch) {
		return ErrCursorGone
	}
	for i := 0; i < s.h.n; i++ {
		e := s.h.ring[(s.h.r+i)%s.h.retain]
		if e.Seq > cursor && s.matchesLocked(e.Topic) {
			s.evch <- e
		}
	}
	return nil
}

func (s *Subscription) cancelLocked() {
	if !s.done {
		s.done = true
		delete(s.h.subs, s)
		close(s.evch)
	}
}

func (s *Subscription) dropLocked() {
	if !s.dropped {
		s.dropped = true
		s.closeResetLocked()
	}
}

// closeResetLocked closes Reset at most once: a gap announced at Subscribe
// time pre-closes it, and a later overflow must not close it again.
func (s *Subscription) closeResetLocked() {
	if !s.rstOnce {
		s.rstOnce = true
		close(s.rst)
	}
}

func (s *Subscription) matchesLocked(topic string) bool {
	for _, p := range s.t {
		switch {
		case p == "*":
			return true
		case strings.HasSuffix(p, "*"):
			if strings.HasPrefix(topic, p[:len(p)-1]) {
				return true
			}
		case p == topic:
			return true
		}
	}
	return false
}
