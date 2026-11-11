package realtime

import (
	"errors"
	"testing"
	"time"
)

func pub(t *testing.T, h *Hub, topic, kind string) Event {
	t.Helper()
	ev, err := h.Publish(Event{Topic: topic, Kind: kind})
	if err != nil {
		t.Fatalf("Publish(%s,%s): %v", topic, kind, err)
	}
	return ev
}

func TestPublishAssignsSeqAndRejectsRegress(t *testing.T) {
	h := NewHub(HubOptions{})
	a := pub(t, h, "acct/1", "updated")
	b := pub(t, h, "acct/1", "updated")
	if a.Seq != 1 || b.Seq != 2 || h.Head() != 2 {
		t.Fatalf("seqs %d %d, head %d, want 1 2 2", a.Seq, b.Seq, h.Head())
	}
	if _, err := h.Publish(Event{Seq: 2, Topic: "acct/1", Kind: "x"}); err == nil {
		t.Error("duplicate seq accepted")
	}
	if _, err := h.Publish(Event{Topic: "", Kind: "x"}); err == nil {
		t.Error("empty topic accepted")
	}
	if _, err := h.Publish(Event{Topic: "a/*", Kind: "x"}); err == nil {
		t.Error("wildcard publish topic accepted")
	}
}

func TestSubscribeReplaysFromRing(t *testing.T) {
	h := NewHub(HubOptions{Retain: 8})
	pub(t, h, "acct/1", "one")
	pub(t, h, "other/9", "skip-me")
	pub(t, h, "acct/1", "three")
	pub(t, h, "acct/1", "four")

	s := h.Subscribe([]string{"acct/1"}, 1)
	// cursor 1: events 2..4 retained, matching topics are 3 and 4.
	for _, want := range []uint64{3, 4} {
		select {
		case ev := <-s.Events:
			if ev.Seq != want {
				t.Fatalf("replayed seq %d, want %d", ev.Seq, want)
			}
		default:
			t.Fatalf("replay of seq %d missing", want)
		}
	}
	select {
	case <-s.Reset:
		t.Fatal("in-range resume must not reset")
	default:
	}
	ev := pub(t, h, "acct/1", "live")
	if got := <-s.Events; got.Seq != ev.Seq {
		t.Fatalf("live seq %d, want %d", got.Seq, ev.Seq)
	}
}

func TestSubscribeOutOfRingRangeResets(t *testing.T) {
	h := NewHub(HubOptions{Retain: 2})
	for _, k := range []string{"x", "y", "z", "w"} {
		pub(t, h, "a/1", k) // seqs 1..4, ring holds 3..4
	}
	s := h.Subscribe([]string{"a/1"}, 1)
	select {
	case <-s.Reset:
	default:
		t.Fatal("stale cursor must reset")
	}
	// live delivery from the head still works after the reset.
	ev := pub(t, h, "a/1", "live") // seq 5
	if got := <-s.Events; got.Seq != ev.Seq {
		t.Fatalf("seq %d, want %d", got.Seq, ev.Seq)
	}
}

func TestGapSubscriptionOverflowDoesNotDoubleCloseReset(t *testing.T) {
	h := NewHub(HubOptions{Retain: 2, QueueSize: 1})
	for _, k := range []string{"a", "b", "c", "d"} {
		pub(t, h, "a/1", k) // ring holds 3..4
	}
	s := h.Subscribe([]string{"a/1"}, 1) // stale cursor: Reset pre-closed as a gap
	if !s.rstOnce {
		t.Fatal("gap must mark Reset closed")
	}
	// Nobody drains this subscription; overflow must go through the guarded
	// drop path instead of closing the already-closed Reset again.
	pub(t, h, "a/1", "y")
	pub(t, h, "a/1", "z")
	if !s.dropped {
		t.Fatal("overflow must still mark the subscription dropped")
	}
}

func TestSlowSubscriberDroppedToReset(t *testing.T) {
	h := NewHub(HubOptions{QueueSize: 1})
	s := h.Subscribe([]string{"a/1"}, 0)
	pub(t, h, "a/1", "queued")
	pub(t, h, "a/1", "overflow") // buffer full: drop to reset
	select {
	case <-s.Reset:
	default:
		t.Fatal("overflow must close Reset")
	}
	if got := (<-s.Events).Kind; got != "queued" {
		t.Fatalf("drained kind %q, want queued", got)
	}
	// A dropped subscription receives nothing more, but the hub keeps working.
	pub(t, h, "a/1", "ignored")
	select {
	case ev := <-s.Events:
		t.Fatalf("dropped sub received %v", ev)
	default:
	}
	s2 := h.Subscribe([]string{"a/1"}, 0)
	pub(t, h, "a/1", "fresh")
	if got := <-s2.Events; got.Kind != "fresh" {
		t.Fatal("other subscribers must keep receiving")
	}
}

func TestTopicPatterns(t *testing.T) {
	h := NewHub(HubOptions{})
	exact := h.Subscribe([]string{"acct/1"}, 0)
	prefix := h.Subscribe([]string{"acct/1/*"}, 0)
	all := h.Subscribe([]string{"*"}, 0)
	other := h.Subscribe([]string{"other"}, 0)
	pub(t, h, "acct/1", "hit")
	pub(t, h, "acct/1/run/7", "run")
	for _, s := range []*Subscription{exact, prefix, all} {
		<-s.Events
	}
	select {
	case ev := <-other.Events:
		t.Fatalf("unmatched topic received %v", ev)
	default:
	}
	// exact does not match the nested topic:
	_ = pub(t, h, "acct/1/only-nested", "n")
	select {
	case <-exact.Events:
		t.Fatal("exact pattern matched a nested topic")
	case <-prefix.Events:
	case <-time.After(time.Second):
		t.Fatal("prefix pattern missed its topic")
	}
	<-all.Events
}

func TestCancelAndClose(t *testing.T) {
	h := NewHub(HubOptions{})
	s := h.Subscribe([]string{"a/1"}, 0)
	s.Cancel()
	s.Cancel() // idempotent
	select {
	case _, ok := <-s.Events:
		if ok {
			t.Fatal("events open after cancel")
		}
	default:
		t.Fatal("events must close on cancel")
	}
	// Close ends live subscriptions...
	live := h.Subscribe([]string{"a/1"}, 0)
	h.Close()
	select {
	case _, ok := <-live.Events:
		if ok {
			t.Fatal("live subscription still open after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not end the subscription")
	}
	// ...and new ones come back already finished.
	if _, err := h.Publish(Event{Topic: "a/1", Kind: "x"}); !errors.Is(err, ErrClosed) {
		t.Fatalf("publish after close: %v", err)
	}
	s2 := h.Subscribe([]string{"a/1"}, 0)
	select {
	case _, ok := <-s2.Events:
		if ok {
			t.Fatal("subscribe after close delivered an event")
		}
	case <-time.After(time.Second):
		t.Fatal("subscribe after close must deliver a finished stream")
	}
	select {
	case <-s2.Reset:
	case <-time.After(time.Second):
		t.Fatal("subscribe after close must signal reset")
	}
}
