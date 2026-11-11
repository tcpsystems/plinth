package realtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/problem"
)

func sseServer(t *testing.T, h *Hub, o Options) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /stream", h.SSE(o))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL + "/stream"
}

// stream reads one SSE response body line by line in the background.
type stream struct {
	lines chan string
}

// openStream GETs url (with optional headers), asserts a 200 event-stream
// response and returns a line reader. The body is closed at cleanup so the
// server handler returns and httptest.Server.Close cannot hang.
func openStream(t *testing.T, url string, header http.Header) *stream {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
		t.Fatalf("GET %s: status %d body %s", url, resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type %q, want text/event-stream", ct)
	}
	s := &stream{lines: make(chan string, 256)}
	go func() {
		defer close(s.lines)
		br := bufio.NewReader(resp.Body)
		for {
			line, err := br.ReadString('\n')
			if line != "" {
				s.lines <- strings.TrimSuffix(line, "\n")
			}
			if err != nil {
				return
			}
		}
	}()
	return s
}

// until blocks until the accumulated stream text contains want.
func (s *stream) until(t *testing.T, want string) string {
	t.Helper()
	var sb strings.Builder
	deadline := time.After(5 * time.Second)
	for {
		select {
		case l, ok := <-s.lines:
			if !ok {
				t.Fatalf("stream ended before %q appeared:\n%s", want, sb.String())
			}
			sb.WriteString(l)
			sb.WriteString("\n")
			if strings.Contains(sb.String(), want) {
				return sb.String()
			}
		case <-deadline:
			t.Fatalf("timeout waiting for %q:\n%s", want, sb.String())
		}
	}
}

// untilClosed asserts the stream reached EOF (the server ended it).
func (s *stream) untilClosed(t *testing.T) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-s.lines:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("stream did not close")
		}
	}
}

// getProblem issues a GET expected to fail fast and returns status + body.
func getProblem(t *testing.T, url string, header http.Header) (int, string) {
	t.Helper()
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, string(b)
}

func TestSSELiveStream(t *testing.T) {
	h := NewHub(HubOptions{})
	url := sseServer(t, h, Options{Retry: 50 * time.Millisecond, Heartbeat: time.Hour})
	s := openStream(t, url+"?topics=a/1", nil)
	s.until(t, "retry: 50\n\n")
	ev := pub(t, h, "a/1", "created") // seq 1
	got := s.until(t, "id: 1\nevent: created\ndata: ")
	if !strings.Contains(got, `"topic":"a/1"`) || !strings.Contains(got, `"seq":`+strconv.FormatUint(ev.Seq, 10)) {
		t.Fatalf("event data missing fields:\n%s", got)
	}
}

func TestWriteSSEFieldFraming(t *testing.T) {
	rr := httptest.NewRecorder()
	writeSSE(rr, rr, "7", "pretty", []byte("{\n  \"x\": 1\n}"))
	if got, want := rr.Body.String(), "id: 7\nevent: pretty\ndata: {\ndata:   \"x\": 1\ndata: }\n\n"; got != want {
		t.Fatalf("data newlines must split over data: lines:\ngot  %q\nwant %q", got, want)
	}
	rr2 := httptest.NewRecorder()
	writeSSE(rr2, rr2, "", "reset", []byte(`{"reason":"backpressure"}`))
	if got, want := rr2.Body.String(), "event: reset\ndata: {\"reason\":\"backpressure\"}\n\n"; got != want {
		t.Fatalf("empty id must omit the id: field:\ngot  %q\nwant %q", got, want)
	}
}

func TestSSEResumeFromLastEventID(t *testing.T) {
	h := NewHub(HubOptions{Retain: 8})
	pub(t, h, "a/1", "one")
	pub(t, h, "a/1", "two")
	pub(t, h, "a/1", "three")
	url := sseServer(t, h, Options{Retry: time.Millisecond, Heartbeat: time.Hour})
	s := openStream(t, url+"?topics=a/1", http.Header{"Last-Event-Id": []string{"1"}})
	got := s.until(t, "id: 3\nevent: three\ndata: ")
	if strings.Contains(got, "id: 1\n") {
		t.Fatalf("resume must start after the cursor:\n%s", got)
	}
}

func TestSSEStaleCursorResetsAndReconnects(t *testing.T) {
	h := NewHub(HubOptions{Retain: 2})
	for _, k := range []string{"a", "b", "c", "d"} {
		pub(t, h, "a/1", k) // ring holds seqs 3..4
	}
	url := sseServer(t, h, Options{Retry: time.Millisecond, Heartbeat: time.Hour})
	s := openStream(t, url+"?topics=a/1", http.Header{"Last-Event-Id": []string{"1"}})
	// the reset carries id = head so the reconnect cannot re-trigger the gap
	s.until(t, "id: 4\nevent: reset\ndata: ")
	s.untilClosed(t)
	pub(t, h, "a/1", "live") // seq 5 while the client was away
	// browser reconnects with the announced cursor; resume is in range
	s2 := openStream(t, url+"?topics=a/1", http.Header{"Last-Event-Id": []string{"4"}})
	s2.until(t, "id: 5\nevent: live\ndata: ")
}

func TestSSESendsSnapshot(t *testing.T) {
	h := NewHub(HubOptions{})
	pub(t, h, "a/1", "old1")
	pub(t, h, "a/1", "old2")
	o := Options{
		Retry:     time.Millisecond,
		Heartbeat: time.Hour,
		Snapshot: func(context.Context, any, []string) (json.RawMessage, uint64, error) {
			return json.RawMessage(`{"items":[1,2]}`), 2, nil
		},
	}
	url := sseServer(t, h, o)
	s := openStream(t, url+"?topics=a/1", nil)
	got := s.until(t, "id: 2\nevent: snapshot\ndata: ")
	if !strings.Contains(got, `"as_of_seq":2`) || !strings.Contains(got, `"items":[1,2]`) {
		t.Fatalf("snapshot body wrong:\n%s", got)
	}
	// live deltas follow the snapshot
	pub(t, h, "a/1", "fresh")
	s.until(t, "id: 3\nevent: fresh\ndata: ")
}

func TestSSEResumeWithSnapshotAfterGap(t *testing.T) {
	h := NewHub(HubOptions{Retain: 2})
	for _, k := range []string{"a", "b", "c", "d"} {
		pub(t, h, "a/1", k)
	}
	o := Options{
		Retry:     time.Millisecond,
		Heartbeat: time.Hour,
		Snapshot: func(context.Context, any, []string) (json.RawMessage, uint64, error) {
			return json.RawMessage(`{"v":1}`), 4, nil
		},
	}
	url := sseServer(t, h, o)
	s := openStream(t, url+"?topics=a/1", http.Header{"Last-Event-Id": []string{"1"}})
	s.until(t, "id: 4\nevent: reset\ndata: ")
	got := s.until(t, "id: 4\nevent: snapshot\ndata: ")
	if !strings.Contains(got, `"as_of_seq":4`) {
		t.Fatalf("snapshot must carry the as_of_seq boundary:\n%s", got)
	}
	s.untilClosed(t)
}

func TestSSESnapshotFailureIsProblem(t *testing.T) {
	h := NewHub(HubOptions{})
	o := Options{Snapshot: func(context.Context, any, []string) (json.RawMessage, uint64, error) {
		return nil, 0, errors.New("db down")
	}}
	mux := http.NewServeMux()
	mux.Handle("GET /stream", h.SSE(o))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	code, body := getProblem(t, srv.URL+"/stream?topics=a/1", nil)
	if code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500", code)
	}
	if !strings.Contains(body, `"status":500`) {
		t.Fatalf("not a problem response: %s", body)
	}
	if strings.Contains(body, "db down") {
		t.Fatalf("problem response leaked the internal error: %s", body)
	}
}

func TestSSEHeartbeat(t *testing.T) {
	h := NewHub(HubOptions{})
	url := sseServer(t, h, Options{Retry: time.Millisecond, Heartbeat: 20 * time.Millisecond})
	s := openStream(t, url+"?topics=a/1", nil)
	s.until(t, ":hb")
}

func TestSSEBackpressureClosesStream(t *testing.T) {
	h := NewHub(HubOptions{QueueSize: 1})
	url := sseServer(t, h, Options{Retry: time.Millisecond, Heartbeat: time.Hour})
	s := openStream(t, url+"?topics=a/1", nil)
	s.until(t, "retry:")
	go func() { // outrun the writer: with QueueSize 1 the hub drops us to reset
		for range 500 {
			if _, err := h.Publish(Event{Topic: "a/1", Kind: "flood"}); err != nil {
				return
			}
		}
	}()
	s.until(t, `"reason":"backpressure"`)
	// the handler returns, closing the stream so the browser reconnects
	s.untilClosed(t)
}

func TestSSEProblemPaths(t *testing.T) {
	h := NewHub(HubOptions{})
	pub(t, h, "a/1", "x")
	base := sseServer(t, h, Options{
		AllowOrigins: []string{"https://ui.tcp.systems"},
		Auth: func(r *http.Request) (any, error) {
			if r.Header.Get("Authorization") != "Bearer good" {
				return nil, problem.New(problem.ErrUnauthorized, "no_creds", "missing bearer token")
			}
			return "user", nil
		},
		Authorize: func(_ any, topic string) bool { return topic != "secret/1" },
	})
	if code, body := getProblem(t, base+"?topics=a/1", nil); code != http.StatusUnauthorized || !strings.Contains(body, "no_creds") {
		t.Fatalf("no auth: %d %s", code, body)
	}
	// a good credential opens the stream (openStream asserts 200 + content type)
	openStream(t, base+"?topics=a/1", http.Header{"Authorization": []string{"Bearer good"}})
	if code, _ := getProblem(t, base+"?topics=a/1", http.Header{"Origin": []string{"https://evil.example"}}); code != http.StatusForbidden {
		t.Fatalf("bad origin: %d", code)
	}
	if code, _ := getProblem(t, base+"?topics=a/1", http.Header{"Origin": []string{"https://ui.tcp.systems"}}); code != http.StatusUnauthorized {
		t.Fatalf("allowed origin must pass the origin gate, then fail auth: %d", code)
	}
	// authentication runs before input validation: anonymous clients get 401
	// whether or not their query is well formed.
	if code, body := getProblem(t, base, http.Header{"Authorization": []string{"Bearer good"}}); code != http.StatusBadRequest || !strings.Contains(body, "topics_required") {
		t.Fatalf("missing topics: %d %s", code, body)
	}
	if code, body := getProblem(t, base+"?topics=secret/1", http.Header{"Authorization": []string{"Bearer good"}}); code != http.StatusForbidden || !strings.Contains(body, "topic_forbidden") {
		t.Fatalf("forbidden topic: %d %s", code, body)
	}
}
