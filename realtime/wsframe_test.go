package realtime

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func wctx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// connOn wraps one end of a net.Pipe as a WebSocket Conn.
func connOn(nc net.Conn, isClient bool, maxMsg int64) *Conn {
	return newConn(nc, bufio.NewReader(nc), isClient, maxMsg)
}

func wsPair(maxMsg int64) (*Conn, *Conn) {
	c, s := net.Pipe()
	return connOn(c, true, maxMsg), connOn(s, false, maxMsg)
}

// rawFrame builds an on-the-wire frame; client→server frames are masked with
// a fixed key (masking is checked, but the key value is protocol-irrelevant).
func rawFrame(fin bool, op byte, masked bool, payload []byte) []byte {
	var b []byte
	h0 := op
	if fin {
		h0 |= 0x80
	}
	b = append(b, h0)
	switch n := len(payload); {
	case n < 126:
		b = append(b, byte(n))
	case n < 1<<16:
		var e [2]byte
		binary.BigEndian.PutUint16(e[:], uint16(n))
		b = append(b, 126)
		b = append(b, e[:]...)
	default:
		var e [8]byte
		binary.BigEndian.PutUint64(e[:], uint64(n))
		b = append(b, 127)
		b = append(b, e[:]...)
	}
	if masked {
		b[1] |= 0x80
		key := [4]byte{0x37, 0xfa, 0x21, 0x1d}
		b = append(b, key[:]...)
		for i, v := range payload {
			b = append(b, v^key[i%4])
		}
	} else {
		b = append(b, payload...)
	}
	return b
}

// send runs fn against the client end of a pipe concurrently: net.Pipe is
// synchronous, so a frame written before the peer reads would deadlock.
func send(t *testing.T, fn func(ctx context.Context) error) {
	t.Helper()
	errs := make(chan error, 1)
	ctx := wctx(t)
	go func() { errs <- fn(ctx) }()
	t.Cleanup(func() {
		if err := <-errs; err != nil {
			t.Errorf("websocket writer goroutine: %v", err)
		}
	})
}

func wantProto(t *testing.T, err error, code int) {
	t.Helper()
	var pe *ProtoError
	if !errors.As(err, &pe) || pe.CloseCode != code {
		t.Fatalf("err = %v, want ProtoError with close code %d", err, code)
	}
}

func TestAcceptKeyVector(t *testing.T) {
	// RFC 6455 §1.3 worked example.
	if got := AcceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Fatalf("AcceptKey = %q", got)
	}
}

func TestConnRoundTrip(t *testing.T) {
	client, server := wsPair(0)
	ctx := wctx(t)
	send(t, func(ctx context.Context) error {
		if err := client.WriteMessage(ctx, OpText, []byte("hello")); err != nil {
			return err
		}
		if err := server.WriteMessage(ctx, OpBinary, []byte{42}); err != nil {
			return err
		}
		return server.WriteClose(ctx, CloseNormal, "bye")
	})
	op, msg, err := server.Read(ctx)
	if err != nil || op != OpText || string(msg) != "hello" {
		t.Fatalf("server read %v %q %v", op, msg, err)
	}
	op, msg, err = client.Read(ctx)
	if err != nil || op != OpBinary || len(msg) != 1 || msg[0] != 42 {
		t.Fatalf("client read %v %q %v", op, msg, err)
	}
	op, msg, err = client.Read(ctx)
	if err != nil || op != OpClose {
		t.Fatalf("client read close: %v %v", op, err)
	}
	if code, reason := ParseClose(msg); code != 1000 || reason != "bye" {
		t.Fatalf("ParseClose = %d %q", code, reason)
	}
}

func TestFragmentationSurvivesInterleavedControl(t *testing.T) {
	client, server := wsPair(0)
	ctx := wctx(t)
	// A ping between fragments is legal (RFC 6455 §5.4): the reader must
	// surface it and keep reassembling the message across Read calls.
	send(t, func(ctx context.Context) error {
		if err := client.write(ctx, false, OpText, []byte("ab")); err != nil {
			return err
		}
		if err := client.write(ctx, true, OpPing, []byte("x")); err != nil {
			return err
		}
		return client.write(ctx, true, OpContinuation, []byte("cd"))
	})
	op, msg, err := server.Read(ctx)
	if err != nil || op != OpPing || string(msg) != "x" {
		t.Fatalf("ping read %v %q %v", op, msg, err)
	}
	op, msg, err = server.Read(ctx)
	if err != nil || op != OpText || string(msg) != "abcd" {
		t.Fatalf("reassembled %v %q %v", op, msg, err)
	}
}

func TestFrameViolations(t *testing.T) {
	cases := []struct {
		name  string
		frame []byte
		want  int
	}{
		{"client frame not masked", rawFrame(true, 0x1, false, []byte("x")), CloseProtocol},
		{"RSV bits set", func() []byte { f := rawFrame(true, 0x1, true, []byte("x")); f[0] |= 0x40; return f }(), CloseProtocol},
		{"continuation without start", rawFrame(true, 0x0, true, []byte("x")), CloseProtocol},
		{"ping oversized", rawFrame(true, 0x9, true, make([]byte, 126)), CloseProtocol},
		{"ping fragmented", rawFrame(false, 0x9, true, []byte("x")), CloseProtocol},
		{"reserved data opcode", rawFrame(true, 0x3, true, nil), CloseUnsupportedData},
		{"unknown control opcode", rawFrame(true, 0xC, true, nil), CloseProtocol},
		{"close with one payload byte", rawFrame(true, 0x8, true, []byte{3}), CloseProtocol},
		{"close with reserved code", rawFrame(true, 0x8, true, []byte{0x03, 0xE5}), CloseProtocol}, // 1005 must not appear on the wire
		{"64-bit length high bit", []byte{0x82, 127 | 0x80, 0x80, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, 4}, CloseProtocol},
	}
	ctx := wctx(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cnc, snc := net.Pipe()
			srv := connOn(snc, false, 0)
			go func() { _, _ = cnc.Write(tc.frame) }()
			_, _, err := srv.Read(ctx)
			wantProto(t, err, tc.want)
			cnc.Close()
			snc.Close() // release the writer goroutine
		})
	}
}

func TestDataOpcodeDuringFragmentRejected(t *testing.T) {
	client, server := wsPair(0)
	ctx := wctx(t)
	send(t, func(ctx context.Context) error {
		if err := client.write(ctx, false, OpText, []byte("a")); err != nil {
			return err
		}
		return client.write(ctx, true, OpText, []byte("b"))
	})
	_, _, err := server.Read(ctx)
	wantProto(t, err, CloseProtocol)
}

func TestMaskedServerFrameRejectedByClient(t *testing.T) {
	snc, cnc := net.Pipe()
	cli := connOn(cnc, true, 0)
	go func() { _, _ = snc.Write(rawFrame(true, 0x1, true, []byte("x"))) }()
	_, _, err := cli.Read(wctx(t))
	wantProto(t, err, CloseProtocol)
	snc.Close()
	cnc.Close()
}

func TestInvalidUTF8RejectedBinaryAllowed(t *testing.T) {
	client, server := wsPair(0)
	ctx := wctx(t)
	send(t, func(ctx context.Context) error {
		if err := client.WriteMessage(ctx, OpText, []byte("\xff\xfe")); err != nil {
			return err
		}
		return client.WriteMessage(ctx, OpBinary, []byte("\xff\xfe"))
	})
	_, _, err := server.Read(ctx)
	wantProto(t, err, CloseInvalidPayload)
	// The error path abandoned the (never-started) fragment sequence, so the
	// connection must still read the next message cleanly.
	op, _, err := server.Read(ctx)
	if err != nil || op != OpBinary {
		t.Fatalf("binary after bad text: %v %v", op, err)
	}
}

func TestMessageTooLarge(t *testing.T) {
	client, server := wsPair(8) // server caps inbound messages at 8 bytes
	ctx := wctx(t)
	send(t, func(ctx context.Context) error {
		if err := client.WriteMessage(ctx, OpText, []byte(strings.Repeat("x", 20))); err != nil {
			return err
		}
		if err := client.write(ctx, false, OpText, []byte("12345")); err != nil {
			return err
		}
		return client.write(ctx, true, OpContinuation, []byte("67890"))
	})
	_, _, err := server.Read(ctx)
	wantProto(t, err, CloseTooLarge)
	_, _, err = server.Read(ctx)
	wantProto(t, err, CloseTooLarge) // fragmented total is capped too
}

func TestCloseCodeRoundTrip(t *testing.T) {
	client, server := wsPair(0)
	ctx := wctx(t)
	send(t, func(ctx context.Context) error {
		return client.WriteClose(ctx, 3000, "app")
	})
	op, msg, err := server.Read(ctx)
	if err != nil || op != OpClose {
		t.Fatalf("close read: %v %v", op, err)
	}
	if code, reason := ParseClose(msg); code != 3000 || reason != "app" {
		t.Fatalf("ParseClose = %d %q", code, reason)
	}
	if code, _ := ParseClose(nil); code != 1005 {
		t.Fatalf("empty close should map to 1005, got %d", code)
	}
}

func TestDialRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"status":400}`, http.StatusBadRequest)
	}))
	t.Cleanup(srv.Close)
	ctx := wctx(t)
	_, _, err := Dial(ctx, srv.URL+"/ws", nil, 0)
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("Dial refused-handshake err = %v, want status 400", err)
	}
}

func TestDialHandshakeAndEarlyFrames(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	res := make(chan error, 1)
	go func() {
		defer close(res)
		nc, err := ln.Accept()
		if err != nil {
			res <- err
			return
		}
		defer nc.Close()
		_ = nc.SetDeadline(time.Now().Add(10 * time.Second)) // fail fast if the test aborted early
		br := bufio.NewReader(nc)
		req, err := http.ReadRequest(br)
		if err != nil {
			res <- err
			return
		}
		if req.Header.Get("Authorization") != "Bearer t" ||
			!tokenIn(req.Header.Get("Sec-WebSocket-Protocol"), "rt.v1") {
			res <- fmt.Errorf("bad upgrade headers: %v", req.Header)
			return
		}
		accept := AcceptKey(req.Header.Get("Sec-WebSocket-Key"))
		// The 101 and one frame are written back-to-back: whatever bufio
		// swallowed past the headers must survive in the Conn, not the Body.
		_, err = fmt.Fprintf(nc, "HTTP/1.1 101 Switching Protocols\r\n"+
			"Upgrade: websocket\r\nConnection: Upgrade\r\n"+
			"Sec-WebSocket-Accept: %s\r\nSec-WebSocket-Protocol: rt.v1\r\n\r\n", accept)
		if err != nil {
			res <- err
			return
		}
		srvConn := newConn(nc, br, false, 0) // reuse br: leftover bytes are frame data
		if err := srvConn.WriteMessage(context.Background(), OpText, []byte("early")); err != nil {
			res <- err
			return
		}
		op, msg, err := srvConn.Read(context.Background())
		if err != nil || op != OpText || string(msg) != "question" {
			res <- fmt.Errorf("server read %v %q %v", op, msg, err)
			return
		}
		if err := srvConn.WriteMessage(context.Background(), OpText, []byte("answer")); err != nil {
			res <- err
			return
		}
		op, _, err = srvConn.Read(context.Background())
		if err != nil || op != OpClose {
			res <- fmt.Errorf("server read close: %v %v", op, err)
			return
		}
		res <- nil
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		for range res {
		}
	})

	ctx := wctx(t)
	cli, respHeader, err := Dial(ctx, "ws://"+ln.Addr().String()+"/ws",
		http.Header{"Authorization": {"Bearer t"}, "Sec-WebSocket-Protocol": {"rt.v1"}}, 0)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer cli.CloseNow()
	if got := respHeader.Get("Sec-WebSocket-Protocol"); got != "rt.v1" {
		t.Fatalf("negotiated subprotocol %q", got)
	}
	op, msg, err := cli.Read(ctx) // the frame buffered during the handshake
	if err != nil || op != OpText || string(msg) != "early" {
		t.Fatalf("early frame: %v %q %v", op, msg, err)
	}
	if err := cli.WriteMessage(ctx, OpText, []byte("question")); err != nil {
		t.Fatal(err)
	}
	op, msg, err = cli.Read(ctx)
	if err != nil || op != OpText || string(msg) != "answer" {
		t.Fatalf("answer: %v %q %v", op, msg, err)
	}
	if err := cli.WriteClose(ctx, CloseNormal, "done"); err != nil {
		t.Fatal(err)
	}
	if err := <-res; err != nil {
		t.Fatalf("server side: %v", err)
	}
}
