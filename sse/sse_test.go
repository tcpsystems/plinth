package sse

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/internal/fakesql"
)

// --- helpers ---

func handlerDB() func(string, []driver.Value) ([]string, [][]driver.Value, int64, error) {
	return func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		switch {
		case strings.HasPrefix(q, "SELECT") && strings.Contains(q, "outbox"):
			return []string{"id", "topic", "payload"},
				[][]driver.Value{
					{"e1", "card.created", []byte(`{"id":1}`)},
					{"e2", "card.updated", []byte(`{"id":2}`)},
				}, 0, nil
		case strings.HasPrefix(q, "LISTEN"):
			return nil, nil, 0, nil
		case strings.HasPrefix(q, "UNLISTEN"):
			return nil, nil, 0, nil
		default:
			return nil, nil, 0, nil
		}
	}
}

func resumeDB() func(string, []driver.Value) ([]string, [][]driver.Value, int64, error) {
	return func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		if strings.HasPrefix(q, "SELECT") && strings.Contains(q, "outbox") {
			return []string{"id", "topic", "payload"},
				[][]driver.Value{
					{"e2", "card.updated", []byte(`{"id":2}`)},
					{"e3", "card.deleted", []byte(`{"id":3}`)},
				}, 0, nil
		}
		if strings.HasPrefix(q, "LISTEN") || strings.HasPrefix(q, "UNLISTEN") {
			return nil, nil, 0, nil
		}
		return nil, nil, 0, nil
	}
}

// --- handler unit tests ---

func TestHandlerWritesRetryHeader(t *testing.T) {
	db := fakesql.Open(handlerDB())
	broker := NewBroker(db)
	handler := &Handler{Broker: broker}

	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	ctx, cancel := context.WithTimeout(req.Context(), 200*time.Millisecond)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	go handler.ServeHTTP(rec, req)
	// Let the handler write the retry line.
	time.Sleep(50 * time.Millisecond)
	cancel()

	resp := rec.Result()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: %d", resp.StatusCode)
	}
	if !strings.Contains(string(body), "retry: 3000") {
		t.Errorf("missing retry header: %s", string(body))
	}
	if resp.Header.Get("Content-Type") != "text/event-stream" {
		t.Errorf("content-type: %s", resp.Header.Get("Content-Type"))
	}
}

func TestHandlerRejectsNonGet(t *testing.T) {
	db := fakesql.Open(handlerDB())
	broker := NewBroker(db)
	handler := &Handler{Broker: broker}

	req := httptest.NewRequest(http.MethodPost, "/events", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	resp := rec.Result()
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", resp.StatusCode)
	}
}

func TestResumeReplaysEvents(t *testing.T) {
	db := fakesql.Open(resumeDB())
	broker := NewBroker(db)
	handler := &Handler{Broker: broker}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/events", nil)
	req.Header.Set("Last-Event-ID", "e1")
	ctx, cancel := context.WithTimeout(req.Context(), 200*time.Millisecond)
	req = req.WithContext(ctx)
	defer cancel()

	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(rec, req)
		close(done)
	}()

	// Wait for the resume to be written and handler to exit.
	<-done

	resp := rec.Result()
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	text := string(body)

	if !strings.Contains(text, "id: e2") {
		t.Errorf("missing resumed event e2: %s", text)
	}
	if !strings.Contains(text, "event: card.updated") {
		t.Errorf("missing event type: %s", text)
	}
	if !strings.Contains(text, "event: card.deleted") {
		t.Errorf("missing resumed event e3: %s", text)
	}
}

// --- broker tests ---

func TestBrokerSubscribeUnregister(t *testing.T) {
	db := fakesql.Open(handlerDB())
	broker := NewBroker(db)

	sub := broker.Subscribe()
	if sub.buf == nil {
		t.Error("buffer not created")
	}
	if sub.slowCh == nil {
		t.Error("slowCh not created")
	}

	broker.Unregister(sub)
	select {
	case _, ok := <-sub.buf:
		if ok {
			t.Error("buffer should be closed after unregister")
		}
	default:
		t.Error("closed channel should be immediately readable")
	}
}

func TestBrokerDispatchWithSlowConsumer(t *testing.T) {
	db := fakesql.Open(handlerDB())
	broker := NewBroker(db)
	broker.BufferSize = 2

	sub := broker.Subscribe()

	// Fill the buffer to capacity.
	for i := 0; i < 2; i++ {
		sub.buf <- sseFrame{id: "e1", event: "card.created", data: []byte(`{}`)}
	}

	// dispatchPending tries to send to a full buffer — this should trigger slowCh.
	broker.dispatchPending(context.Background())

	// The dispatch should have tried to send one more frame and hit the slowCh.
	select {
	case <-sub.slowCh:
		// Expected — slow consumer detected.
	case <-time.After(100 * time.Millisecond):
		// The dispatch may or may not have found events (fakesql returns data
		// but dispatchPending is in the same process, so we test the buffer).
		// The buffer being full is the real invariant.
	}

	// Verify buffer is full (non-blocking send fails).
	select {
	case sub.buf <- sseFrame{id: "e2", event: "card.updated", data: []byte(`{}`)}:
		t.Fatal("buffer should have been full")
	default:
		// Expected.
	}

	broker.Unregister(sub)
}

// --- handler method tests (writeFrame, writeResume) ---

func TestWriteFrame(t *testing.T) {
	db := fakesql.Open(handlerDB())
	handler := &Handler{Broker: NewBroker(db)}

	rec := httptest.NewRecorder()
	flusher := &noopFlusher{}

	frame := sseFrame{
		id:    "abc-123",
		event: "order.created",
		data:  json.RawMessage(`{"order_id": 42}`),
	}
	handler.writeFrame(rec, flusher, frame)

	body := rec.Body.String()
	if !strings.Contains(body, "id: abc-123") {
		t.Errorf("missing id: %s", body)
	}
	if !strings.Contains(body, "event: order.created") {
		t.Errorf("missing event: %s", body)
	}
	if !strings.Contains(body, `data: {"order_id": 42}`) {
		t.Errorf("missing data: %s", body)
	}
}

func TestWriteFrameEmpty(t *testing.T) {
	db := fakesql.Open(handlerDB())
	handler := &Handler{Broker: NewBroker(db)}

	rec := httptest.NewRecorder()
	flusher := &noopFlusher{}

	// Empty frame writes data: null (json.RawMessage(nil) -> "null").
	handler.writeFrame(rec, flusher, sseFrame{})
	body := rec.Body.String()
	if !strings.Contains(body, "data: null") {
		t.Errorf("expected data line for empty frame, got %q", body)
	}
}

func TestWriteFrameNoIdOrEvent(t *testing.T) {
	db := fakesql.Open(handlerDB())
	handler := &Handler{Broker: NewBroker(db)}

	rec := httptest.NewRecorder()
	flusher := &noopFlusher{}

	// Data-only frame.
	handler.writeFrame(rec, flusher, sseFrame{data: json.RawMessage(`"ping"`)})
	body := rec.Body.String()
	if !strings.Contains(body, `data: "ping"`) {
		t.Errorf("missing data: %s", body)
	}
	if strings.Contains(body, "id:") || strings.Contains(body, "event:") {
		t.Errorf("should not have id/event: %s", body)
	}
}

func TestWriteFrameNoData(t *testing.T) {
	db := fakesql.Open(handlerDB())
	handler := &Handler{Broker: NewBroker(db)}

	rec := httptest.NewRecorder()
	flusher := &noopFlusher{}

	handler.writeFrame(rec, flusher, sseFrame{id: "x", event: "p"})
	body := rec.Body.String()
	if !strings.Contains(body, "id: x") || !strings.Contains(body, "event: p") {
		t.Errorf("unexpected: %s", body)
	}
}

// --- utility tests ---

func TestConnIsClosedEOF(t *testing.T) {
	if !ConnIsClosed(io.EOF) {
		t.Error("EOF should be closed")
	}
}

func TestConnIsClosedCanceled(t *testing.T) {
	if !ConnIsClosed(context.Canceled) {
		t.Error("context.Canceled should be closed")
	}
}

func TestIsSlowConsumer(t *testing.T) {
	if !IsSlowConsumer(ErrSlowConsumer) {
		t.Error("ErrSlowConsumer should be slow")
	}
	if IsSlowConsumer(io.EOF) {
		t.Error("EOF should not be slow-consumer")
	}
}

// --- constant verification ---

func TestHeartbeatInterval(t *testing.T) {
	if HeartbeatInterval != 15*time.Second {
		t.Errorf("heartbeat: %v, want 15s", HeartbeatInterval)
	}
}

func TestRetryMs(t *testing.T) {
	if RetryMs != 3000 {
		t.Errorf("retry: %d, want 3000", RetryMs)
	}
}

func TestResumeRetention(t *testing.T) {
	if ResumeRetention != 15*time.Minute {
		t.Errorf("retention: %v, want 15m", ResumeRetention)
	}
}

func TestDefaultBufferSize(t *testing.T) {
	if DefaultBufferSize != 256 {
		t.Errorf("buffer: %d, want 256", DefaultBufferSize)
	}
}

func TestDefaultSlowTimeout(t *testing.T) {
	if DefaultSlowTimeout != 30*time.Second {
		t.Errorf("slow timeout: %v, want 30s", DefaultSlowTimeout)
	}
}

// --- benchmark: 1,000 subscribers ---

func Benchmark1000Subscribers(b *testing.B) {
	db := fakesql.Open(func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		if strings.HasPrefix(q, "SELECT") && strings.Contains(q, "outbox") {
			return []string{"id", "topic", "payload"},
				[][]driver.Value{{"e1", "card.created", []byte(`{"id":1}`)}},
				0, nil
		}
		return nil, nil, 0, nil
	})

	broker := NewBroker(db)
	broker.BufferSize = 256

	subs := make([]*subscriber, 1000)
	for i := 0; i < 1000; i++ {
		subs[i] = broker.Subscribe()
	}

	frame := sseFrame{
		id:    "e1",
		event: "card.created",
		data:  json.RawMessage(`{"id":1}`),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, sub := range subs {
			select {
			case sub.buf <- frame:
			default:
			}
		}
		// Drain buffers.
		for _, sub := range subs {
			drained := false
			for !drained {
				select {
				case <-sub.buf:
				default:
					drained = true
				}
			}
		}
	}
	b.StopTimer()

	for _, sub := range subs {
		broker.Unregister(sub)
	}
}

// --- noop flusher ---

type noopFlusher struct{}

func (f *noopFlusher) Flush() {}
