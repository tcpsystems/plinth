package events

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type sourceKey struct{}

// Source is the in-process notification channel between a publisher and the
// subscribers of the same process.
//
// database/sql has no portable notification API, so a driver that can deliver
// NOTIFY out of band meets the bus here: it opens the bus Listener's
// connections with the context returned by WithSource(ctx, src), calls
// src.Attach(f) once that connection is LISTENing (and Remove when it
// closes), and src is then woken by every Publish of the same process. The
// NOTIFY statement and PostgresSchema's trigger cover other processes and
// other connections. A subscriber's Poll covers the rest: correctness never
// depends on a notification arriving.
type Source struct {
	mu   sync.Mutex
	last uint64
	subs map[int]func(uint64)
	next int
}

// NewSource returns an empty Source.
func NewSource() *Source { return &Source{subs: map[int]func(uint64){}} }

// WithSource returns a context that opens connections against src.
func WithSource(ctx context.Context, src *Source) context.Context {
	return context.WithValue(ctx, sourceKey{}, src)
}

// SourceFrom returns the source carried by ctx, or nil.
func SourceFrom(ctx context.Context) *Source {
	s, _ := ctx.Value(sourceKey{}).(*Source)
	return s
}

// Attach registers f and returns the function that removes it. f is called
// with every Seq announced after the call.
func (s *Source) Attach(f func(seq uint64)) (remove func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	id := s.next
	s.subs[id] = f
	return func() {
		s.mu.Lock()
		delete(s.subs, id)
		s.mu.Unlock()
	}
}

// announce wakes every attached callback with seq. It runs on the publisher's
// flusher goroutine; callbacks must not block.
func (s *Source) announce(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq <= s.last {
		return
	}
	s.last = seq
	for _, f := range s.subs {
		f(seq)
	}
}

// notifySource is one subscription's notification state: the highest Seq
// announced since it was created, and the wake channel the producer blocks on.
type notifySource struct {
	wake chan struct{}
	high atomic.Uint64
	stop chan struct{}
	once sync.Once
}

func newNotifySource() *notifySource {
	return &notifySource{wake: make(chan struct{}, 1), stop: make(chan struct{})}
}

// announce records seq and wakes the producer once.
func (n *notifySource) announce(seq uint64) {
	for {
		cur := n.high.Load()
		if seq <= cur || n.high.CompareAndSwap(cur, seq) {
			break
		}
	}
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// hintAfter reports whether an announced Seq is above cursor, that is,
// whether an event is waiting to be read.
func (n *notifySource) hintAfter(cursor uint64) bool { return n.high.Load() > cursor }

// Close ends the waits. Safe to call more than once.
func (n *notifySource) Close() { n.once.Do(func() { close(n.stop) }) }

// sleepUntilEvent blocks until an announcement, Close, done or the timeout.
func (n *notifySource) sleepUntilEvent(done <-chan struct{}, timeout time.Duration) {
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-n.wake:
	case <-n.stop:
	case <-done:
	case <-t.C:
	}
}
