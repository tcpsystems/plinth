package events

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"database/sql/driver"
	"github.com/tcpsystems/plinth/keel/internal/fakesql"
)

// window is the coalescing window tests use; the minimum keeps them quick
// while still letting calls arrive together.
const window = MinWindow

func newTestBus(t *testing.T, pg *fakePG) *PostgresBus {
	t.Helper()
	b := &PostgresBus{DB: pg.open(), Window: window}
	t.Cleanup(b.Close)
	return b
}

func mustPublish(t *testing.T, b *PostgresBus, batches ...Batch) []Event {
	t.Helper()
	evs, err := b.Publish(context.Background(), batches...)
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	return evs
}

func kinds(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind
	}
	return out
}

// TestBusOrder is the golden order test: the order events are published in is
// the order Seq assigns and the order subscribers see, across topics, across
// batches, across multiple publishers, and on resume from a cursor.
func TestBusOrder(t *testing.T) {
	pg := newFakePG(t)
	b := newTestBus(t, pg)
	ctx := context.Background()

	// Two topics interleaved, one call carrying two topics at once.
	mustPublish(t, b, Batch{Topic: "acct/a", Events: []Event{{Kind: "A1"}}})
	mustPublish(t, b, Batch{Topic: "acct/b", Events: []Event{{Kind: "B1"}}})
	pub := mustPublish(t, b,
		Batch{Topic: "acct/a", Events: []Event{{Kind: "A2"}, {Kind: "A3"}}},
		Batch{Topic: "acct/b", Events: []Event{{Kind: "B2"}}},
	)
	if got, want := kinds(pub), []string{"A2", "A3", "B2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Publish returned %v, want %v", got, want)
	}
	for i, e := range pub {
		if e.Seq != uint64(3+i) {
			t.Fatalf("event %d has seq %d, want %d", i, e.Seq, 3+i)
		}
		if e.EmittedAt.IsZero() || e.EmittedAt.Location() != time.UTC {
			t.Fatalf("event %d emitted_at = %v, want UTC", i, e.EmittedAt)
		}
	}

	// A subscription to the parent sees both topics in Seq order.
	sub, err := b.Subscribe(ctx, "acct", 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	if got, want := kinds(collect(t, sub, 5, 5*time.Second)), []string{"A1", "B1", "A2", "A3", "B2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acct delivered %v, want %v", got, want)
	}

	// A topic subscription sees only its own topic.
	subA, err := b.Subscribe(ctx, "acct/a", 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subA.Close()
	if got, want := kinds(collect(t, subA, 3, 5*time.Second)), []string{"A1", "A2", "A3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("acct/a delivered %v, want %v", got, want)
	}

	// Resume: a cursor into "acct" cuts across topics; a topic cursor only
	// counts its own events.
	resumed, err := b.Subscribe(ctx, "acct", 2)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer resumed.Close()
	if got, want := kinds(collect(t, resumed, 3, 5*time.Second)), []string{"A2", "A3", "B2"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("resume from 2 delivered %v, want %v", got, want)
	}
	lastA := uint64(4) // A3
	resumedA, err := b.Subscribe(ctx, "acct/a", lastA)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer resumedA.Close()
	// Resuming at the head delivers nothing: the cursor was not live yet.
	quiet(t, pg, resumedA)

	// Live events follow the backlog without a gap.
	mustPublish(t, b, Batch{Topic: "acct/a", Events: []Event{{Kind: "A4"}}})
	if got, want := kinds(collect(t, resumedA, 1, 5*time.Second)), []string{"A4"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("live delivery %v, want %v", got, want)
	}
}

// TestBusCoalesces checks that calls arriving inside one window share one
// transaction and one notification, and that the notification names the
// highest Seq committed, so a subscriber that wakes can read everything.
func TestBusCoalesces(t *testing.T) {
	pg := newFakePG(t)
	b := newTestBus(t, pg)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			mustPublish(t, b, Batch{Topic: "t", Events: []Event{{Kind: "k"}}})
		}(i)
	}
	wg.Wait()
	if got := len(pg.committed()); got != 20 {
		t.Fatalf("committed %d events, want 20", got)
	}
	if n := pg.noticeCount(); n > 20 || n == 0 {
		t.Fatalf("sent %d notifications for 20 events, want at least one and coalesced", n)
	}
	pg.mu.Lock()
	defer pg.mu.Unlock()
	last := pg.notices[len(pg.notices)-1]
	if last != 20 {
		t.Fatalf("last notification names seq %d, want 20", last)
	}
}

// TestBusFailedPublish checks that a failed commit reports its error, rolls
// back its rows, and does not stop later publishes.
func TestBusFailedPublish(t *testing.T) {
	pg := newFakePG(t)
	b := newTestBus(t, pg)
	ctx := context.Background()

	pg.setFail("INSERT INTO events.event", errors.New("disk full"))
	_, err := b.Publish(ctx, Batch{Topic: "t", Events: []Event{{Kind: "lost"}}})
	if err == nil || !contains(err.Error(), "disk full") {
		t.Fatalf("Publish with failing insert = %v, want disk full", err)
	}
	if got := pg.committed(); len(got) != 0 {
		t.Fatalf("failed publish left rows: %+v", got)
	}

	pg.setFail("INSERT INTO events.event", nil)
	// The failed reservation must not be reused: later Seqs are above it, so
	// a cursor never needs to see the rolled-back range.
	evs := mustPublish(t, b, Batch{Topic: "t", Events: []Event{{Kind: "ok"}}})
	if evs[0].Seq <= 1 || pg.seqReuse.Load() != 1 {
		t.Fatalf("seq %d after a failed publish of seq 1 (reused: %d)", evs[0].Seq, pg.seqReuse.Load())
	}
	sub, err := b.Subscribe(ctx, "t", 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	if got := kinds(collect(t, sub, 1, 5*time.Second)); got[0] != "ok" {
		t.Fatalf("delivered %v", got)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestBusReplayResume is the replay/resume property test. Random batches
// across related topics are published by several publishers; subscribers with
// random cursors must receive exactly the events after that cursor, in Seq
// order, whether they start before, during or after the publishing.
func TestBusReplayResume(t *testing.T) {
	topics := []string{"a", "a/b", "a/b/c", "a/x", "b"}
	for seed := int64(1); seed <= 20; seed++ {
		r := rand.New(rand.NewSource(seed))
		pg := newFakePG(t)
		b := newTestBus(t, pg)
		ctx := context.Background()
		gen := func(seed int64) *rand.Rand { return rand.New(rand.NewSource(seed)) }

		const publishers, perPublisher = 3, 8
		var wg sync.WaitGroup
		for p := 0; p < publishers; p++ {
			wg.Add(1)
			go func(p int) {
				defer wg.Done()
				r := gen(seed*100 + int64(p))
				for i := 0; i < perPublisher; i++ {
					topic := topics[r.Intn(len(topics))]
					evs := make([]Event, 1+r.Intn(3))
					for j := range evs {
						evs[j] = Event{Kind: topic, Payload: []byte{byte(p), byte(i), byte(j)}}
					}
					mustPublish(t, b, Batch{Topic: topic, Events: evs})
				}
			}(p)
		}
		// A subscriber started mid-publish, and one after everything.
		mid, err := b.Subscribe(ctx, "a", 0)
		if err != nil {
			t.Fatalf("seed %d: Subscribe: %v", seed, err)
		}
		wg.Wait()
		all := pg.committed()
		for _, topic := range topics {
			var want []Event
			cursor := r.Int63n(int64(len(all)) + 1)
			for _, row := range all {
				e := Event{Seq: uint64(row.seq), Topic: row.topic, Kind: row.kind, Payload: row.payload}
				if e.Seq <= uint64(cursor) || !matchTopic(topic, e.Topic) {
					continue
				}
				want = append(want, e)
			}
			sub, err := b.Subscribe(ctx, topic, uint64(cursor))
			if err != nil {
				t.Fatalf("seed %d: Subscribe: %v", seed, err)
			}
			got := collect(t, sub, len(want), 5*time.Second)
			sub.Close()
			if !sameEvents(got, want) {
				t.Fatalf("seed %d topic %q cursor %d: got %v, want %v", seed, topic, cursor, kindSeqs(got), kindSeqs(want))
			}
		}
		midWant := 0
		for _, row := range all {
			if matchTopic("a", row.topic) {
				midWant++
			}
		}
		got := collect(t, mid, midWant, 5*time.Second)
		mid.Close()
		var want []Event
		for _, row := range all {
			if matchTopic("a", row.topic) {
				want = append(want, Event{Seq: uint64(row.seq), Payload: row.payload})
			}
		}
		if !sameSeqPayload(got, want) {
			t.Fatalf("seed %d: mid subscriber got %v, want %v", seed, kindSeqs(got), kindSeqs(want))
		}
	}
}

// matchTopic mirrors the WHERE clause read uses.
func matchTopic(topic, got string) bool {
	return got == topic || len(got) > len(topic) && got[:len(topic)+1] == topic+"/"
}

func sameEvents(a, b []Event) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Seq != b[i].Seq || a[i].Topic != b[i].Topic || a[i].Kind != b[i].Kind || !reflect.DeepEqual(a[i].Payload, b[i].Payload) {
			return false
		}
	}
	return true
}

func sameSeqPayload(a, b []Event) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Seq != b[i].Seq || !reflect.DeepEqual(a[i].Payload, b[i].Payload) {
			return false
		}
	}
	return true
}

func kindSeqs(evs []Event) []string {
	out := make([]string, len(evs))
	for i, e := range evs {
		out[i] = e.Kind + "#" + itoa(e.Seq)
	}
	return out
}

func itoa(u uint64) string { return string(appendUint(nil, u)) }

func appendUint(b []byte, u uint64) []byte {
	if u == 0 {
		return append(b, '0')
	}
	var tmp [20]byte
	i := len(tmp)
	for u > 0 {
		i--
		tmp[i] = byte('0' + u%10)
		u /= 10
	}
	return append(b, tmp[i:]...)
}

func TestBusValidation(t *testing.T) {
	b := &PostgresBus{DB: fakesql.Open(func(string, []driver.Value) ([]string, [][]driver.Value, int64, error) { return nil, nil, 0, nil })}
	defer b.Close()
	if _, err := b.Publish(context.Background(), Batch{Topic: "", Events: []Event{{Kind: "k"}}}); !errors.Is(err, ErrEmptyTopic) {
		t.Fatalf("empty topic: %v", err)
	}
	if _, err := b.Publish(context.Background(), Batch{Topic: "a b", Events: []Event{{Kind: "k"}}}); err == nil {
		t.Fatal("topic with a space accepted")
	}
	if _, err := b.Publish(context.Background(), Batch{Topic: "t"}); err == nil {
		t.Fatal("empty batch accepted")
	}
	if _, err := b.Publish(context.Background(), Batch{Topic: "t", Events: []Event{{}}}); err == nil {
		t.Fatal("event without kind accepted")
	}
	if evs, err := b.Publish(context.Background()); err != nil || evs != nil {
		t.Fatalf("no-op publish = %v, %v", evs, err)
	}
	(&PostgresBus{}).Close() // never used: no-op
	if _, err := (&PostgresBus{}).Publish(context.Background(), Batch{Topic: "t", Events: []Event{{Kind: "k"}}}); err == nil || !contains(err.Error(), "DB is required") {
		t.Fatalf("Publish without DB = %v", err)
	}
	if _, err := (&PostgresBus{DB: b.DB, Channel: "no good"}).Subscribe(context.Background(), "t", 0); err == nil || !contains(err.Error(), "identifier") {
		t.Fatalf("bad channel = %v", err)
	}

	for _, ok := range []string{"a", "a/b", "acct/42/run/7", "a_b.c-d:e"} {
		if err := ValidateTopic(ok); err != nil {
			t.Fatalf("ValidateTopic(%q) = %v", ok, err)
		}
	}
	for _, bad := range []string{"", "/a", "a/", "a//b", "a b", "a?b"} {
		if err := ValidateTopic(bad); err == nil {
			t.Fatalf("ValidateTopic(%q) accepted", bad)
		}
	}
}

// TestSourceAnnounce checks that a Source wakes attached callbacks once per
// rising Seq and that removed callbacks are not called.
func TestSourceAnnounce(t *testing.T) {
	s := NewSource()
	var mu sync.Mutex
	var got []uint64
	a := s.Attach(func(seq uint64) { mu.Lock(); got = append(got, seq); mu.Unlock() })
	b := s.Attach(func(uint64) { t.Error("removed callback called") })
	b()
	if SourceFrom(context.Background()) != nil || SourceFrom(WithSource(context.Background(), s)) != s {
		t.Fatal("WithSource/SourceFrom round trip")
	}
	s.announce(5)
	s.announce(3)
	s.announce(7)
	mu.Lock()
	defer mu.Unlock()
	if want := []uint64{5, 7}; !reflect.DeepEqual(got, want) {
		t.Fatalf("callbacks got %v, want %v", got, want)
	}
	a()
	s.announce(9)
	if len(got) != 2 {
		t.Fatalf("callback after remove: %v", got)
	}
}

func TestNotifySource(t *testing.T) {
	n := newNotifySource()
	if n.hintAfter(0) {
		t.Fatal("fresh source already hints")
	}
	n.announce(4)
	if !n.hintAfter(3) || n.hintAfter(4) {
		t.Fatal("hintAfter not compared to announcements")
	}
	done := make(chan struct{})
	n.sleepUntilEvent(done, time.Second) // consumes the pending wake
	start := time.Now()
	go func() { time.Sleep(time.Millisecond); n.announce(5) }()
	n.sleepUntilEvent(done, 5*time.Second)
	if time.Since(start) > time.Second {
		t.Fatal("sleepUntilEvent did not wake on announce")
	}
	n.Close()
	n.sleepUntilEvent(done, 5*time.Second) // returns at once via stop
}

// TestBusPollsWithoutNotify checks that Poll alone, without the notification
// reaching the subscriber, still delivers: a row stored by another process
// wakes nobody, yet the subscriber finds it on its next poll.
func TestBusPollsWithoutNotify(t *testing.T) {
	pg := newFakePG(t)
	b := &PostgresBus{DB: pg.open(), Window: window, Poll: 5 * time.Millisecond}
	t.Cleanup(b.Close)
	ctx := context.Background()

	// A separate bus whose notifications this one never hears: nothing
	// carries them from its Source or its LISTEN connection (the test driver).
	other := &PostgresBus{DB: pg.open(), Window: window}
	t.Cleanup(other.Close)
	if _, err := other.Publish(ctx, Batch{Topic: "t", Events: []Event{{Kind: "elsewhere"}}}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	sub, err := b.Subscribe(ctx, "t", 0)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	if got := kinds(collect(t, sub, 1, 5*time.Second)); got[0] != "elsewhere" {
		t.Fatalf("delivered %v", got)
	}
}
