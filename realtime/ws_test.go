package realtime

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// wsServer mounts the WS handler and returns its URL. Dial sends the RFC
// 6455 handshake headers itself, so client-side tests only pass the headers
// under test (Origin, Authorization, Sec-WebSocket-Protocol).
func wsServer(t *testing.T, h *Hub, o Options) string {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /ws", h.WS(o))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv.URL
}

// dialWS connects a client, failing the test on a refused handshake.
func dialWS(t *testing.T, url string, header http.Header) *Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := Dial(ctx, url, header, 0)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { _ = conn.CloseNow() })
	return conn
}

// readFrame reads one inbound message with a deadline and decodes the JSON
// frame envelope.
func readFrame(t *testing.T, conn *Conn) wsFrame {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	op, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if op != OpText {
		t.Fatalf("opcode %d, want text", op)
	}
	var f wsFrame
	if err := json.Unmarshal(payload, &f); err != nil {
		t.Fatalf("frame %s: %v", payload, err)
	}
	return f
}

// readRaw reads one raw inbound frame (for close/binary/protocol tests).
func readRaw(t *testing.T, conn *Conn) (Opcode, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	op, payload, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return op, payload
}

func writeFrame(t *testing.T, conn *Conn, obj map[string]any) {
	t.Helper()
	b, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	writeFrameRaw(t, conn, string(b))
}

func writeFrameRaw(t *testing.T, conn *Conn, s string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.WriteMessage(ctx, OpText, []byte(s)); err != nil {
		t.Fatalf("write frame: %v", err)
	}
}

func wantClose(t *testing.T, conn *Conn, code int) {
	t.Helper()
	op, payload := readRaw(t, conn)
	if op != OpClose {
		t.Fatalf("opcode %d, want close", op)
	}
	if got, _ := ParseClose(payload); got != code {
		t.Fatalf("close code %d, want %d", got, code)
	}
}

// testSnap is a snapshot returning fixed data as of the hub's current head.
func testSnap(h *Hub, data string) SnapshotFunc {
	return func(ctx context.Context, principal any, topics []string) (json.RawMessage, uint64, error) {
		return json.RawMessage(data), h.Head(), nil
	}
}

func decodeReason(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var m struct {
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("reset data %s: %v", raw, err)
	}
	return m.Reason
}

func TestWSSnapshotThenLiveEvents(t *testing.T) {
	h := NewHub(HubOptions{})
	url := wsServer(t, h, Options{Snapshot: testSnap(h, `{"items":[]}`)})
	conn := dialWS(t, url+"/ws?topics=acct/1", nil)
	if f := readFrame(t, conn); f.Type != "snapshot" || string(f.Data) != `{"items":[]}` {
		t.Fatalf("first frame %+v, want snapshot", f)
	}
	ev, err := h.Publish(Event{Topic: "acct/1", Kind: "run.started", Data: json.RawMessage(`{"ok":true}`)})
	if err != nil {
		t.Fatal(err)
	}
	f := readFrame(t, conn)
	if f.Type != "event" || f.Seq != ev.Seq || f.Topic != "acct/1" || f.Kind != "run.started" || string(f.Data) != `{"ok":true}` {
		t.Fatalf("event frame %+v", f)
	}
}

func TestWSLastSeqReplays(t *testing.T) {
	h := NewHub(HubOptions{})
	e1 := pub(t, h, "acct/1", "run.started")
	e2 := pub(t, h, "acct/1", "run.finished")
	pub(t, h, "acct/2", "run.started") // other topic, must not arrive
	conn := dialWS(t, wsServer(t, h, Options{})+"/ws?topics=acct/1&last_seq="+strconv.FormatUint(e1.Seq, 10), nil)
	f := readFrame(t, conn)
	if f.Type != "event" || f.Seq != e2.Seq {
		t.Fatalf("resume frame %+v, want replay of seq %d", f, e2.Seq)
	}
	e3 := pub(t, h, "acct/1", "run.started")
	f = readFrame(t, conn)
	if f.Type != "event" || f.Seq != e3.Seq {
		t.Fatalf("live frame %+v, want seq %d", f, e3.Seq)
	}
}

func TestWSUpgradeValidation(t *testing.T) {
	h := NewHub(HubOptions{})
	srv := httptest.NewServer(h.WS(Options{}))
	t.Cleanup(srv.Close)
	for _, tc := range []struct {
		name string
		hdrs map[string]string
		want string // problem code in the refusal body
	}{
		{"plain get", nil, "websocket_upgrade_required"},
		{"no upgrade token", map[string]string{"Connection": "keep-alive", "Upgrade": "websocket", "Sec-WebSocket-Version": "13", "Sec-WebSocket-Key": "abc"}, "websocket_upgrade_required"},
		{"wrong version", map[string]string{"Connection": "Upgrade", "Upgrade": "websocket", "Sec-WebSocket-Version": "8", "Sec-WebSocket-Key": "abc"}, "websocket_version_unsupported"},
		{"no key", map[string]string{"Connection": "Upgrade", "Upgrade": "websocket", "Sec-WebSocket-Version": "13"}, "websocket_key_missing"},
	} {
		req, err := http.NewRequest("GET", srv.URL+"/ws?topics=*", nil)
		if err != nil {
			t.Fatal(err)
		}
		for k, v := range tc.hdrs {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("%s: status %d, want 400 (%s)", tc.name, resp.StatusCode, body)
		}
		var p struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(body, &p); err != nil {
			t.Fatalf("%s: body %s: %v", tc.name, body, err)
		}
		if p.Code != tc.want {
			t.Fatalf("%s: problem code %q, want %q", tc.name, p.Code, tc.want)
		}
	}
}

func TestWSOriginAuthRejectsBeforeUpgrade(t *testing.T) {
	h := NewHub(HubOptions{})
	// Wrong origin: refused as ordinary HTTP (the browser sees a failed
	// WebSocket connection); the right origin passes.
	srv := httptest.NewServer(h.WS(Options{AllowOrigins: []string{"https://ui.tcp.systems"}}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, respH, err := Dial(ctx, srv.URL+"/ws?topics=*", http.Header{"Origin": []string{"https://evil.example"}}, 0)
	if err == nil {
		t.Fatal("evil origin accepted")
	}
	if ct := respH.Get("Content-Type"); !strings.Contains(ct, "problem") {
		t.Fatalf("refusal content-type %q, want problem+json", ct)
	}
	conn := dialWS(t, srv.URL+"/ws?topics=*", http.Header{"Origin": []string{"https://ui.tcp.systems"}})
	pub(t, h, "x", "system.up")
	if f := readFrame(t, conn); f.Type != "event" || f.Kind != "system.up" {
		t.Fatalf("good-origin session frame %+v", f)
	}

	// Auth error: refused, no upgrade; a good token streams.
	srv2 := httptest.NewServer(h.WS(Options{Auth: func(r *http.Request) (any, error) {
		if r.Header.Get("Authorization") != "Bearer ok" {
			return nil, http.ErrNoCookie
		}
		return "user", nil
	}}))
	t.Cleanup(srv2.Close)
	if _, _, err := Dial(ctx, srv2.URL+"/ws?topics=*", nil, 0); err == nil {
		t.Fatal("anonymous upgrade accepted")
	}
	conn2 := dialWS(t, srv2.URL+"/ws?topics=*", http.Header{"Authorization": []string{"Bearer ok"}})
	pub(t, h, "x", "system.up")
	if f := readFrame(t, conn2); f.Type != "event" || f.Kind != "system.up" {
		t.Fatalf("authenticated session frame %+v", f)
	}
}

func TestWSSubprotocolNegotiation(t *testing.T) {
	h := NewHub(HubOptions{})
	srv := httptest.NewServer(h.WS(Options{Subprotocols: []string{"rt.v1"}}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// No overlap: refused.
	if _, _, err := Dial(ctx, srv.URL+"/ws?topics=*", http.Header{"Sec-WebSocket-Protocol": []string{"rt.v9"}}, 0); err == nil {
		t.Fatal("mismatched subprotocol accepted")
	}
	// Overlap: echoed in the 101.
	conn, respH, err := Dial(ctx, srv.URL+"/ws?topics=*", http.Header{"Sec-WebSocket-Protocol": []string{"other, rt.v1"}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if got := respH.Get("Sec-WebSocket-Protocol"); got != "rt.v1" {
		t.Fatalf("negotiated %q, want rt.v1", got)
	}
}

func TestWSInitialGapAnnouncesAndCloses(t *testing.T) {
	h := NewHub(HubOptions{Retain: 2})
	pub(t, h, "acct/1", "run.started") // seq 1, evicted from the ring below
	pub(t, h, "acct/1", "run.started") // seq 2, evicted
	pub(t, h, "acct/1", "run.started") // seq 3 (ring holds 3..4)
	pub(t, h, "acct/1", "run.started") // seq 4
	srv := wsServer(t, h, Options{Snapshot: testSnap(h, `{"v":1}`)})
	// cursor 1 needs seq 2 onward, but the ring starts at 3: gap.
	conn := dialWS(t, srv+"/ws?topics=acct/1&last_seq=1", nil)
	f := readFrame(t, conn)
	if f.Type != "reset" || f.Seq != h.Head() {
		t.Fatalf("gap frame %+v, want reset at head %d", f, h.Head())
	}
	if reason := decodeReason(t, f.Data); reason != "cursor_out_of_range" {
		t.Fatalf("gap reason %q", reason)
	}
	if f := readFrame(t, conn); f.Type != "snapshot" || string(f.Data) != `{"v":1}` {
		t.Fatalf("gap snapshot frame %+v", f)
	}
	wantClose(t, conn, CloseTryLater)
}

func TestWSBackpressureDropResetsAndCloses(t *testing.T) {
	h := NewHub(HubOptions{QueueSize: 1})
	srv := wsServer(t, h, Options{})
	conn := dialWS(t, srv+"/ws?topics=acct/1", nil)
	// The client does not read. The hub outruns the session's per-event
	// marshal+write by an order of magnitude, fills the 1-slot queue and
	// drops the subscriber; the session then announces reset+1013 with
	// whatever it had buffered.
	for i := 0; i < 64; i++ {
		pub(t, h, "acct/1", "run.started")
	}
	sawReset := false
	for {
		op, payload := readRaw(t, conn)
		if op == OpClose {
			if got, _ := ParseClose(payload); got != CloseTryLater {
				t.Fatalf("close code %d, want 1013", got)
			}
			break
		}
		var f wsFrame
		if err := json.Unmarshal(payload, &f); err != nil {
			t.Fatalf("frame %s: %v", payload, err)
		}
		if f.Type == "reset" && decodeReason(t, f.Data) == "backpressure" {
			sawReset = true
		}
	}
	if !sawReset {
		t.Fatal("no backpressure reset frame before close")
	}
}

func TestWSResumeRewindReplaysDuplicates(t *testing.T) {
	h := NewHub(HubOptions{})
	srv := wsServer(t, h, Options{})
	conn := dialWS(t, srv+"/ws?topics=acct/1", nil)
	e1 := pub(t, h, "acct/1", "run.started")
	e2 := pub(t, h, "acct/1", "run.finished")
	// Drain the two live frames, then rewind from before both: the ring
	// replays them (at-least-once — consumers dedupe by seq).
	if f := readFrame(t, conn); f.Seq != e1.Seq {
		t.Fatalf("frame %+v want %d", f, e1.Seq)
	}
	if f := readFrame(t, conn); f.Seq != e2.Seq {
		t.Fatalf("frame %+v want %d", f, e2.Seq)
	}
	writeFrame(t, conn, map[string]any{"type": "resume", "seq": e1.Seq - 1})
	if f := readFrame(t, conn); f.Type != "event" || f.Seq != e1.Seq {
		t.Fatalf("rewind frame %+v", f)
	}
	if f := readFrame(t, conn); f.Type != "event" || f.Seq != e2.Seq {
		t.Fatalf("rewind frame %+v", f)
	}
	// And the session is still live afterwards.
	e3 := pub(t, h, "acct/1", "run.started")
	if f := readFrame(t, conn); f.Type != "event" || f.Seq != e3.Seq {
		t.Fatalf("live after rewind %+v", f)
	}
}

func TestWSResumeGoneResetsInPlace(t *testing.T) {
	h := NewHub(HubOptions{Retain: 2})
	srv := wsServer(t, h, Options{Snapshot: testSnap(h, `{"v":1}`)})
	conn := dialWS(t, srv+"/ws?topics=acct/1", nil)
	// A fresh session at cursor 0 gets its snapshot first.
	if f := readFrame(t, conn); f.Type != "snapshot" || string(f.Data) != `{"v":1}` {
		t.Fatalf("initial frame %+v, want snapshot", f)
	}
	pub(t, h, "acct/1", "run.started")       // seq 1, evicted from the ring below
	pub(t, h, "acct/1", "run.started")       // seq 2
	e3 := pub(t, h, "acct/1", "run.started") // ring holds seq 2..3
	// Drain everything already in flight before asking for the lost
	// cursor: all session writes are serialized, so once e3 is read, the
	// frames after the resume can only be the resume's own output.
	for {
		f := readFrame(t, conn)
		if f.Type == "event" && f.Seq == e3.Seq {
			break
		}
	}
	// Cursor 0 is no longer backed by the ring: resume becomes an in-place
	// reset + snapshot (no reconnect, unlike SSE) and the session keeps
	// streaming.
	writeFrame(t, conn, map[string]any{"type": "resume", "seq": 0})
	f := readFrame(t, conn)
	if f.Type != "reset" {
		t.Fatalf("post-resume frame %+v, want reset", f)
	}
	if decodeReason(t, f.Data) != "cursor_out_of_range" {
		t.Fatalf("reset reason %+v", f.Data)
	}
	if f := readFrame(t, conn); f.Type != "snapshot" || string(f.Data) != `{"v":1}` {
		t.Fatalf("reset follow-up %+v, want snapshot", f)
	}
	// Still live: a new event arrives without any reconnect.
	e4 := pub(t, h, "acct/1", "run.started")
	if f := readFrame(t, conn); f.Type != "event" || f.Seq != e4.Seq {
		t.Fatalf("post-reset frame %+v, want seq %d", f, e4.Seq)
	}
}

func TestWSInboundMsg(t *testing.T) {
	h := NewHub(HubOptions{})
	saw := make(chan InboundMsg, 4)
	srv := wsServer(t, h, Options{Inbound: func(ctx context.Context, principal any, msg InboundMsg) error {
		saw <- msg
		_, err := h.Publish(Event{Topic: "acct/1", Kind: "run.reply", Data: msg.Data})
		return err
	}})
	conn := dialWS(t, srv+"/ws?topics=acct/1", nil)
	writeFrame(t, conn, map[string]any{"type": "msg", "seq": 7, "topic": "acct/1", "data": map[string]any{"hello": true}})
	select {
	case msg := <-saw:
		if msg.Seq != 7 || msg.Topic != "acct/1" || string(msg.Data) != `{"hello":true}` {
			t.Fatalf("inbound %+v", msg)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("inbound handler never ran")
	}
	if f := readFrame(t, conn); f.Type != "event" || f.Kind != "run.reply" {
		t.Fatalf("reply frame %+v", f)
	}
}

func TestWSMsgWithoutInboundCloses1008(t *testing.T) {
	h := NewHub(HubOptions{})
	conn := dialWS(t, wsServer(t, h, Options{})+"/ws?topics=*", nil)
	writeFrame(t, conn, map[string]any{"type": "msg", "data": 1})
	wantClose(t, conn, ClosePolicy)
}

func TestWSInboundErrorCloses1011(t *testing.T) {
	h := NewHub(HubOptions{})
	srv := wsServer(t, h, Options{Inbound: func(ctx context.Context, principal any, msg InboundMsg) error {
		return http.ErrAbortHandler // any non-nil error
	}})
	conn := dialWS(t, srv+"/ws?topics=*", nil)
	writeFrame(t, conn, map[string]any{"type": "msg"})
	wantClose(t, conn, CloseInternal)
}

func TestWSAppPingPong(t *testing.T) {
	h := NewHub(HubOptions{})
	conn := dialWS(t, wsServer(t, h, Options{})+"/ws?topics=*", nil)
	writeFrame(t, conn, map[string]any{"type": "ping", "seq": 3})
	f := readFrame(t, conn)
	if f.Type != "pong" || f.Seq != 3 {
		t.Fatalf("app ping reply %+v", f)
	}
}

func TestWSProtocolPingPong(t *testing.T) {
	h := NewHub(HubOptions{})
	conn := dialWS(t, wsServer(t, h, Options{})+"/ws?topics=*", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.WritePing(ctx, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	op, payload := readRaw(t, conn)
	if op != OpPong || string(payload) != "hi" {
		t.Fatalf("op %d payload %q, want pong hi", op, payload)
	}
}

func TestWSBadFrames(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(t *testing.T, conn *Conn)
		code  int
	}{
		{"binary", func(t *testing.T, c *Conn) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.WriteMessage(ctx, OpBinary, []byte{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
		}, CloseUnsupportedData},
		{"invalid json", func(t *testing.T, c *Conn) { writeFrameRaw(t, c, `{"type":`) }, CloseInvalidPayload},
		{"unknown type", func(t *testing.T, c *Conn) { writeFrameRaw(t, c, `{"type":"dance"}`) }, CloseProtocol},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHub(HubOptions{})
			conn := dialWS(t, wsServer(t, h, Options{})+"/ws?topics=*", nil)
			tc.write(t, conn)
			wantClose(t, conn, tc.code)
		})
	}
}

func TestWSClientCloseEchoed(t *testing.T) {
	h := NewHub(HubOptions{})
	conn := dialWS(t, wsServer(t, h, Options{})+"/ws?topics=*", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.WriteClose(ctx, CloseGoingAway, "bye"); err != nil {
		t.Fatal(err)
	}
	wantClose(t, conn, CloseGoingAway)
}

func TestWSTooLargeFrame(t *testing.T) {
	h := NewHub(HubOptions{})
	srv := wsServer(t, h, Options{MaxMessageBytes: 64})
	conn := dialWS(t, srv+"/ws?topics=*", nil)
	writeFrameRaw(t, conn, `{"type":"msg","padding":"`+strings.Repeat("x", 200)+`"}`)
	wantClose(t, conn, CloseTooLarge)
}

func TestWSTopicPatternFiltering(t *testing.T) {
	h := NewHub(HubOptions{})
	conn := dialWS(t, wsServer(t, h, Options{})+"/ws?topics=acct/1/*", nil)
	pub(t, h, "acct/2/run/1", "run.started")
	e := pub(t, h, "acct/1/run/1", "run.started")
	f := readFrame(t, conn)
	if f.Type != "event" || f.Seq != e.Seq {
		t.Fatalf("frame %+v, want only the matching topic", f)
	}
}
