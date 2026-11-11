package conformance

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/realtime"
)

// Realtime describes a running service's realtime surface to prove: the SSE
// stream, the WebSocket session, the served schema index and the Connect-JSON
// RPC endpoint. Empty paths skip that group.
//
// The contract a service must meet for [RunRealtime] to pass:
//
//   - StreamPath is the SSE endpoint and WSPath the WebSocket endpoint of
//     one hub; both take ?topics= with the joined Topics patterns and share
//     one seq cursor space. The SSE endpoint answers fresh subscribers with
//     a retry: hint and honors Last-Event-ID.
//   - Topic is a concrete topic matching Topics, and Publish injects
//     {tick: N} events on it — N counts up from 1 over the suite's run, so
//     every event the suite publishes is distinguishable even against a
//     hub that already holds earlier traffic.
//   - SchemaPath serves the index document (service, realtime_prefix,
//     events with grammar-valid kinds and JSON Schema objects).
//   - RPCPath is a method that returns its request body unchanged (the
//     echo convention) and is NOT registered for the unary GET form;
//     RPCIdempotentPath is such a method that is.
//   - When Header is empty, the suite also checks that a stream request
//     without topics is a clean problem rejection.
//   - Heartbeat, when non-zero, must be the SSE heartbeat interval the
//     service runs with; the suite waits for one comment frame.
type Realtime struct {
	// Header is sent on every request (e.g. an Authorization bearer for a
	// service with Options.Auth). Nil for an open test service.
	Header http.Header
	// StreamPath is the SSE endpoint, e.g. "/api/rt/stream".
	StreamPath string
	// WSPath is the WebSocket endpoint, e.g. "/api/rt/ws".
	WSPath string
	// SchemaPath is the served-schema index, e.g. "/api/rt/schema".
	SchemaPath string
	// RPCPath is the fully qualified POST-able echo method, e.g.
	// "/api/rt/rpc/conformance.RPC/Echo".
	RPCPath string
	// RPCIdempotentPath is the echo method registered for the unary GET
	// form; empty skips the GET checks.
	RPCIdempotentPath string
	// Topics are the subscription patterns sent as ?topics= (comma joined).
	Topics []string
	// Topic is the concrete topic Publish uses; it must match Topics.
	Topic string
	// Publish injects one event (kind conformance.tick, JSON data) into the
	// service's hub.
	Publish func(topic, kind string, data any) error
	// Heartbeat is the service's SSE heartbeat interval; zero skips the
	// heartbeat check.
	Heartbeat time.Duration
}

// rtKind is the event kind the suite publishes. It is grammar-valid per the
// standard's kind rules so it can appear in a real event index.
const rtKind = "conformance.tick"

// rtKindRE mirrors the standard's event-kind grammar (the regex chapter 2
// pins); kept as a literal here so conformance tests any implementation,
// not only plinth's.
var rtKindRE = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+$`)

// RunRealtime executes every applicable realtime check as subtests of t.
// It complements [Run]: that one proves the HTTP contract, this proves the
// three transports and served schema of the realtime standard. Subtests run
// sequentially and share one tick counter, so a service must not wrap this
// suite in parallel subtests.
func RunRealtime(t *testing.T, g Target, rt Realtime) {
	t.Helper()
	if rt.Publish == nil {
		t.Fatal("conformance: Realtime.Publish is required")
	}
	if len(rt.Topics) == 0 || rt.Topic == "" {
		t.Fatal("conformance: Realtime needs Topics and a matching Topic")
	}
	s := &rtSuite{g: g, rt: rt}
	if rt.StreamPath != "" {
		t.Run("sse_envelope", s.envelope)
		t.Run("sse_resume", s.resume)
		if rt.Heartbeat > 0 {
			t.Run("sse_heartbeat", s.heartbeat)
		}
		if len(rt.Header) == 0 {
			t.Run("sse_missing_topics", s.missingTopics)
		}
	}
	if rt.WSPath != "" {
		t.Run("ws_session", s.session)
	}
	if rt.SchemaPath != "" {
		t.Run("schema_index", s.index)
	}
	if rt.RPCPath != "" {
		t.Run("rpc_unary", s.rpc)
	}
}

// rtSuite carries the shared tick counter. Subtests never run in parallel
// (the framework serializes non-parallel subtests), so it needs no lock.
type rtSuite struct {
	g  Target
	rt Realtime
	n  float64 // last tick published by the suite
}

// publish injects the next tick event and returns its number.
func (s *rtSuite) publish(t *testing.T) float64 {
	t.Helper()
	s.n++
	if err := s.rt.Publish(s.rt.Topic, rtKind, map[string]any{"tick": s.n}); err != nil {
		t.Fatalf("publish tick %v: %v", s.n, err)
	}
	return s.n
}

// rtEvent is the JSON payload of one SSE data: line (the hub's Event).
type rtEvent struct {
	Seq   uint64 `json:"seq"`
	Topic string `json:"topic"`
	Kind  string `json:"kind"`
	Data  struct {
		Tick float64 `json:"tick"`
	} `json:"data"`
}

func (rt Realtime) topicsQuery() string {
	return "topics=" + url.QueryEscape(strings.Join(rt.Topics, ","))
}

// headerMap converts an http.Header to [Target.do]'s header form.
func headerMap(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	m := make(map[string]string, len(h))
	for k, vs := range h {
		if len(vs) > 0 {
			m[k] = vs[0]
		}
	}
	return m
}

// sseConn opens the stream with the topics query and any extra headers. The
// whole stream is bounded by timeout so a wedged service fails the test
// instead of hanging the suite.
func (s *rtSuite) sseConn(t *testing.T, extra http.Header, timeout time.Duration) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, "GET",
		strings.TrimRight(s.g.BaseURL, "/")+s.rt.StreamPath+"?"+s.rt.topicsQuery(), nil)
	if err != nil {
		t.Fatal(err)
	}
	addHeaders(req, s.rt.Header)
	addHeaders(req, extra)
	c := s.g.Client
	if c == nil {
		c = &http.Client{}
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		t.Fatalf("stream status %d body %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		t.Fatalf("stream content-type %q, want text/event-stream", ct)
	}
	return resp
}

func addHeaders(req *http.Request, h http.Header) {
	for k, vs := range h {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
}

// nextFrame reads one SSE frame (up to the blank line). It returns the
// field map and any comment payload (":hb" → "hb").
func nextFrame(t *testing.T, br *bufio.Reader) (map[string]string, string) {
	t.Helper()
	fields := map[string]string{}
	comment := ""
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			t.Fatalf("sse read: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if len(fields) == 0 && comment == "" {
				continue // stray blank line between frames
			}
			return fields, comment
		}
		if strings.HasPrefix(line, ":") {
			comment = line[1:]
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			t.Fatalf("malformed sse line %q", line)
		}
		fields[k] = strings.TrimPrefix(v, " ")
	}
}

// tick reads frames until a tick event with this N arrives and returns its
// id and decoded event. Frames of other kinds are ignored (a service may
// multiplex its own traffic onto the topics we subscribed to), and comment
// or retry frames never match.
func (s *rtSuite) tick(t *testing.T, br *bufio.Reader, want float64) (string, rtEvent) {
	t.Helper()
	for {
		f, _ := nextFrame(t, br)
		if f["event"] != rtKind {
			continue
		}
		var ev rtEvent
		if err := json.Unmarshal([]byte(f["data"]), &ev); err != nil {
			t.Fatalf("tick data %q: %v", f["data"], err)
		}
		if ev.Data.Tick != want {
			continue
		}
		if ev.Kind != rtKind || ev.Topic != s.rt.Topic {
			t.Fatalf("tick event %+v, want kind %q on topic %q", ev, rtKind, s.rt.Topic)
		}
		return f["id"], ev
	}
}

func (s *rtSuite) envelope(t *testing.T) {
	resp := s.sseConn(t, nil, 20*time.Second)
	br := bufio.NewReader(resp.Body)

	// WHATWG resume contract: a fresh subscriber gets the reconnection
	// hint first, before any event.
	fields, _ := nextFrame(t, br)
	if _, ok := fields["retry"]; !ok {
		t.Fatalf("first frame %+v, want a retry: hint", fields)
	}
	if v, err := strconv.Atoi(fields["retry"]); err != nil || v <= 0 {
		t.Fatalf("retry %q, want a positive number of milliseconds", fields["retry"])
	}

	n := s.publish(t)
	id, ev := s.tick(t, br, n)
	if id == "" {
		t.Fatal("event frame has no id")
	}
	if id != strconv.FormatUint(ev.Seq, 10) {
		t.Fatalf("id %q, want the hub seq %d (one cursor space across transports)", id, ev.Seq)
	}
}

func (s *rtSuite) resume(t *testing.T) {
	resp := s.sseConn(t, nil, 20*time.Second)
	br := bufio.NewReader(resp.Body)
	n1 := s.publish(t)
	id1, _ := s.tick(t, br, n1)
	resp.Body.Close() // disconnect with the service still running

	ackSeq, err := strconv.ParseUint(id1, 10, 64)
	if err != nil {
		t.Fatalf("event id %q is not a number", id1)
	}
	n2 := s.publish(t) // arrives while we are away

	resp2 := s.sseConn(t, http.Header{"Last-Event-ID": []string{id1}}, 20*time.Second)
	br2 := bufio.NewReader(resp2.Body)
	for {
		f, _ := nextFrame(t, br2)
		if f["event"] != rtKind {
			continue
		}
		var ev rtEvent
		if err := json.Unmarshal([]byte(f["data"]), &ev); err != nil {
			t.Fatalf("resumed data %q: %v", f["data"], err)
		}
		if ev.Seq <= ackSeq {
			t.Fatalf("Last-Event-ID %s did not skip: re-delivered seq %d", id1, ev.Seq)
		}
		if ev.Data.Tick == n2 {
			return // the missed event arrived — resume works
		}
	}
}

func (s *rtSuite) heartbeat(t *testing.T) {
	resp := s.sseConn(t, nil, 2*s.rt.Heartbeat+15*time.Second)
	br := bufio.NewReader(resp.Body)
	// The heartbeat must fire on an idle stream: publish nothing and wait
	// for a comment frame (the stream's retry hint carries no comment, so
	// the first comment we see is a heartbeat).
	for {
		_, comment := nextFrame(t, br)
		if comment != "" {
			return
		}
	}
}

func (s *rtSuite) missingTopics(t *testing.T) {
	r := s.g.do(t, "GET", s.rt.StreamPath, "", headerMap(s.rt.Header))
	// A service behind Auth rejects earlier with 401; both are clean
	// problem responses, anything else is not conformance.
	switch r.code {
	case http.StatusBadRequest, http.StatusUnauthorized:
	default:
		t.Fatalf("stream without topics: status %d body %s, want 400 (or 401 if auth is required)", r.code, r.body)
	}
	s.g.problem(t, r, r.code)
}

// rtFrame is the standard's WebSocket frame envelope (chapter 3): JSON text
// frames with a type discriminator and the hub seq.
type rtFrame struct {
	Type  string          `json:"type"`
	Seq   uint64          `json:"seq"`
	Topic string          `json:"topic"`
	Kind  string          `json:"kind"`
	Data  json.RawMessage `json:"data"`
}

// tickOf extracts the tick number from an event frame's data.
func (f rtFrame) tickOf(t *testing.T) float64 {
	t.Helper()
	var d struct {
		Tick float64 `json:"tick"`
	}
	if err := json.Unmarshal(f.Data, &d); err != nil {
		t.Fatalf("event data %s: %v", f.Data, err)
	}
	return d.Tick
}

func (s *rtSuite) session(t *testing.T) {
	u, err := url.Parse(s.g.BaseURL)
	if err != nil {
		t.Fatal(err)
	}
	switch u.Scheme {
	case "https":
		u.Scheme = "wss"
	default:
		u.Scheme = "ws"
	}
	u.Path = strings.TrimRight(u.Path, "/") + s.rt.WSPath
	u.RawQuery = s.rt.topicsQuery()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, _, err := realtime.Dial(ctx, u.String(), s.rt.Header, 0)
	if err != nil {
		t.Fatalf("dial %s: %v", u, err)
	}
	defer conn.CloseNow()

	// readUntil consumes frames until pred accepts a text frame, answering
	// server pings and failing on close (events older than ours — replayed
	// by a fresh dial at cursor 0 — simply do not match pred).
	readUntil := func(what string, pred func(rtFrame) bool) rtFrame {
		for {
			op, payload, err := conn.Read(ctx)
			if err != nil {
				t.Fatalf("ws read (waiting for %s): %v", what, err)
			}
			switch op {
			case realtime.OpPing:
				if err := conn.WritePong(ctx, payload); err != nil {
					t.Fatalf("ws pong: %v", err)
				}
			case realtime.OpPong:
				continue
			case realtime.OpClose:
				code, reason := realtime.ParseClose(payload)
				t.Fatalf("ws closed by server while waiting for %s: code %d reason %q", what, code, reason)
			case realtime.OpText:
				var f rtFrame
				if err := json.Unmarshal(payload, &f); err != nil {
					t.Fatalf("ws frame %s: %v", payload, err)
				}
				if f.Type == "reset" {
					t.Fatalf("server sent reset frame: %s", payload)
				}
				if pred(f) {
					return f
				}
			default:
				t.Fatalf("unexpected opcode %d while waiting for %s", op, what)
			}
		}
	}
	send := func(what string, obj any) {
		b, err := json.Marshal(obj)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.WriteMessage(ctx, realtime.OpText, b); err != nil {
			t.Fatalf("ws write %s: %v", what, err)
		}
	}

	// Delivery: a published event arrives as an "event" frame carrying the
	// hub seq, kind and topic.
	n1 := s.publish(t)
	f := readUntil("tick event", func(f rtFrame) bool {
		return f.Type == "event" && f.Kind == rtKind && f.tickOf(t) == n1
	})
	if f.Seq == 0 || f.Topic != s.rt.Topic {
		t.Fatalf("event frame %+v, want kind %q on topic %q with a seq", f, rtKind, s.rt.Topic)
	}

	// App-level ping/pong (chapter 3): a {"type":"ping"} frame is answered
	// with a pong echoing the same seq.
	send("app ping", map[string]any{"type": "ping", "seq": 123456789})
	if p := readUntil("app pong", func(f rtFrame) bool { return f.Type == "pong" }); p.Seq != 123456789 {
		t.Fatalf("pong %+v, want seq echoed", p)
	}

	// Mid-session resume at the last seen seq asks for nothing older than
	// the head: a no-op that must leave the session live.
	send("resume", map[string]any{"type": "resume", "seq": f.Seq})
	n2 := s.publish(t)
	g2 := readUntil("post-resume tick", func(f rtFrame) bool {
		return f.Type == "event" && f.Kind == rtKind && f.tickOf(t) == n2
	})
	if g2.Seq <= f.Seq {
		t.Fatalf("post-resume event seq %d, want after %d", g2.Seq, f.Seq)
	}

	// RFC 6455 ping/pong liveness.
	if err := conn.WritePing(ctx, []byte("rt")); err != nil {
		t.Fatalf("ws ping: %v", err)
	}
	for {
		op, payload, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("ws read (waiting for pong): %v", err)
		}
		switch op {
		case realtime.OpPong:
			if string(payload) != "rt" {
				t.Fatalf("pong payload %q, want rt", payload)
			}
			return
		case realtime.OpPing:
			if err := conn.WritePong(ctx, payload); err != nil {
				t.Fatalf("ws pong: %v", err)
			}
		case realtime.OpClose:
			t.Fatal("server closed the session during ping/pong")
		}
	}
}

func (s *rtSuite) index(t *testing.T) {
	r := s.g.do(t, "GET", s.rt.SchemaPath, "", headerMap(s.rt.Header))
	if r.code != http.StatusOK {
		t.Fatalf("schema: status %d body %s", r.code, r.body)
	}
	if ct := r.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("schema content-type %q", ct)
	}
	var idx map[string]any
	if err := json.Unmarshal(r.body, &idx); err != nil {
		t.Fatalf("schema index %s: %v", r.body, err)
	}
	if svc, _ := idx["service"].(string); svc == "" {
		t.Error("index lacks a service name")
	}
	if p, _ := idx["realtime_prefix"].(string); !strings.HasPrefix(p, "/") {
		t.Errorf("realtime_prefix %v, want a path", idx["realtime_prefix"])
	}
	events, ok := idx["events"].([]any)
	if !ok || len(events) == 0 {
		t.Fatalf("events %v, want a non-empty list of event schemas", idx["events"])
	}
	for _, e := range events {
		m, _ := e.(map[string]any)
		kind, _ := m["kind"].(string)
		if !rtKindRE.MatchString(kind) {
			t.Errorf("event kind %q violates the family.kind grammar", kind)
		}
		if tp, _ := m["topic_pattern"].(string); tp == "" {
			t.Errorf("event %q has no topic_pattern", kind)
		}
		if _, ok := m["schema"].(map[string]any); !ok {
			t.Errorf("event %q schema is not a JSON object", kind)
		}
	}
	// If the index advertises a Connect surface and we were given an RPC
	// path, the RPC method must be listed — generated clients discover it
	// there.
	if c, ok := idx["connect"].(map[string]any); ok && s.rt.RPCPath != "" {
		spec := path.Base(path.Dir(s.rt.RPCPath)) + "/" + path.Base(s.rt.RPCPath)
		methods, _ := c["rpc_methods"].([]any)
		found := false
		for _, m := range methods {
			if str, _ := m.(string); str == spec {
				found = true
			}
		}
		if !found {
			t.Errorf("rpc_methods %v lack %q", c["rpc_methods"], spec)
		}
	}
	var typedIdx realtime.SchemaIndex
	if err := json.Unmarshal(r.body, &typedIdx); err != nil {
		t.Fatalf("schema index decode as SchemaIndex: %v", err)
	}
	if ts, err := realtime.GenerateTypeScript(typedIdx); err != nil || len(ts) == 0 {
		t.Errorf("GenerateTypeScript from served schema index: err=%v len=%d", err, len(ts))
	}
}

// jsonEquiv compares two JSON documents as values (key order and spacing
// are not part of the message).
func jsonEquiv(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	return reflect.DeepEqual(x, y)
}

// wantConnectErr asserts r is a Connect wire error: JSON (never a problem
// document — the RPC surface speaks its own dialect), with this HTTP status
// and code.
func wantConnectErr(t *testing.T, r resp, status int, code string) {
	t.Helper()
	if r.code != status {
		t.Fatalf("status %d, want %d: %s", r.code, status, r.body)
	}
	if ct := r.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("error content-type %q, want application/json (a connect error is never problem+json)", ct)
	}
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(r.body, &e); err != nil {
		t.Fatalf("error body %s: %v", r.body, err)
	}
	if e.Code != code {
		t.Fatalf("code %q, want %q (message %q)", e.Code, code, e.Message)
	}
}

func (s *rtSuite) rpc(t *testing.T) {
	const reqBody = `{"conformance":true}`

	// Echo: POST JSON in, the same message out (the contract RPCPath
	// promises).
	r := s.g.do(t, "POST", s.rt.RPCPath, reqBody, headerMap(s.rt.Header))
	if r.code != http.StatusOK {
		t.Fatalf("echo: status %d body %s", r.code, r.body)
	}
	if !json.Valid(r.body) || !jsonEquiv(r.body, []byte(reqBody)) {
		t.Fatalf("echo body %s, want %s", r.body, reqBody)
	}

	// An unsupported Message-Codec is unimplemented at HTTP 415 — the
	// protocol spec pins that status, and a generated client's typed
	// errors depend on it.
	r = s.g.do(t, "POST", s.rt.RPCPath, `{}`, mergeHdrs(s.rt.Header, map[string]string{"Content-Type": "application/proto"}))
	wantConnectErr(t, r, http.StatusUnsupportedMediaType, "unimplemented")

	// A malformed Connect-Timeout-Ms is rejected as invalid_argument,
	// before any handler runs.
	r = s.g.do(t, "POST", s.rt.RPCPath, `{}`, mergeHdrs(s.rt.Header, map[string]string{"Connect-Timeout-Ms": "not-a-number"}))
	wantConnectErr(t, r, http.StatusBadRequest, "invalid_argument")

	// An unknown method under the same mount is 501 unimplemented.
	unknown := path.Dir(s.rt.RPCPath) + "/NoSuchMethod"
	r = s.g.do(t, "POST", unknown, `{}`, headerMap(s.rt.Header))
	wantConnectErr(t, r, http.StatusNotImplemented, "unimplemented")

	// A non-idempotent method must refuse the unary GET form.
	r = s.g.do(t, "GET", s.rt.RPCPath+"?connect=v1&encoding=json&message="+url.QueryEscape(`{}`), "", headerMap(s.rt.Header))
	wantConnectErr(t, r, http.StatusBadRequest, "invalid_argument")

	if s.rt.RPCIdempotentPath == "" {
		return
	}
	// Unary GET with the message percent-encoded in the query.
	msg := `{"conformance":1}`
	r = s.g.do(t, "GET", s.rt.RPCIdempotentPath+"?connect=v1&encoding=json&message="+url.QueryEscape(msg), "", headerMap(s.rt.Header))
	if r.code != http.StatusOK || !jsonEquiv(r.body, []byte(msg)) {
		t.Fatalf("GET echo: status %d body %s, want 200 %s", r.code, r.body, msg)
	}
	// The same form with base64url (unpadded, per RFC 4648 §5) message.
	r = s.g.do(t, "GET", s.rt.RPCIdempotentPath+"?connect=v1&encoding=json&base64=1&message="+
		base64.RawURLEncoding.EncodeToString([]byte(msg)), "", headerMap(s.rt.Header))
	if r.code != http.StatusOK || !jsonEquiv(r.body, []byte(msg)) {
		t.Fatalf("GET base64 echo: status %d body %s, want 200 %s", r.code, r.body, msg)
	}
}

// mergeHdrs returns rt.Header as do()'s map form with extra applied on top.
func mergeHdrs(h http.Header, extra map[string]string) map[string]string {
	m := headerMap(h)
	if m == nil {
		m = map[string]string{}
	}
	for k, v := range extra {
		m[k] = v
	}
	return m
}
