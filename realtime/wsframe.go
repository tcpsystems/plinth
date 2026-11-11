package realtime

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Opcode is an RFC 6455 frame opcode.
type Opcode byte

// RFC 6455 §5.2 opcodes. Only these are legal in this profile: there are no
// negotiated extensions, so RSV bits must be zero.
const (
	OpContinuation Opcode = 0x0
	OpText         Opcode = 0x1
	OpBinary       Opcode = 0x2
	OpClose        Opcode = 0x8
	OpPing         Opcode = 0x9
	OpPong         Opcode = 0xA
)

// WebSocket close codes this package uses (RFC 6455 §7.4.1).
const (
	CloseNormal          = 1000
	CloseGoingAway       = 1001
	CloseProtocol        = 1002
	CloseUnsupportedData = 1003
	CloseInvalidPayload  = 1007
	ClosePolicy          = 1008
	CloseTooLarge        = 1009
	CloseInternal        = 1011
	CloseTryLater        = 1013
)

// wsGUID is the RFC 6455 handshake GUID.
const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// AcceptKey computes the Sec-WebSocket-Accept value for a Sec-WebSocket-Key.
func AcceptKey(key string) string {
	h := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

// ProtoError reports an RFC 6455 protocol violation. The session answers
// with a close frame carrying CloseCode.
type ProtoError struct {
	CloseCode int
	msg       string
}

// Error formats the WebSocket protocol violation message.
func (e *ProtoError) Error() string { return "realtime: websocket: " + e.msg }

func protoErr(code int, format string, args ...any) error {
	return &ProtoError{CloseCode: code, msg: fmt.Sprintf(format, args...)}
}

// Conn is one WebSocket connection carrying raw RFC 6455 frames. Exactly one
// goroutine should read and one write; concurrent writes are serialized but
// readers never see a message split across writers.
type Conn struct {
	nc       net.Conn
	br       *bufio.Reader
	isClient bool  // clients mask outgoing frames; servers must not mask
	maxMsg   int64 // cap on one reassembled inbound message
	writeMu  sync.Mutex

	// Inbound fragment reassembly state. Only the reader goroutine touches
	// it, so it survives across Read calls: a Ping or Pong may legally
	// interleave the fragments of one message (RFC 6455 §5.4).
	frag    []byte
	fragOp  Opcode
	hasFrag bool
}

func newConn(nc net.Conn, br *bufio.Reader, isClient bool, maxMsg int64) *Conn {
	if maxMsg <= 0 {
		maxMsg = 1 << 20
	}
	return &Conn{nc: nc, br: br, isClient: isClient, maxMsg: maxMsg}
}

// CloseNow tears the underlying TCP connection down without a close
// handshake. Callers normally use WriteClose first, then CloseNow.
func (c *Conn) CloseNow() error { return c.nc.Close() }

// Read returns the next data message (fragmented frames are reassembled) or
// the next control frame. Control frames surface for the session to answer:
// nothing replies automatically, so every write stays on one code path.
// Malformed input returns a *ProtoError naming the close code to send.
func (c *Conn) Read(ctx context.Context) (Opcode, []byte, error) {
	fail := func(code int, format string, args ...any) (Opcode, []byte, error) {
		c.hasFrag, c.frag = false, nil // a broken sequence is abandoned
		return 0, nil, protoErr(code, format, args...)
	}
	for {
		if d, ok := ctx.Deadline(); ok {
			_ = c.nc.SetReadDeadline(d)
		} else {
			_ = c.nc.SetReadDeadline(time.Time{})
		}
		fin, op, payload, err := c.readFrame()
		if err != nil {
			c.hasFrag, c.frag = false, nil
			return 0, nil, err
		}
		switch op {
		case OpClose, OpPing, OpPong:
			if op == OpClose {
				if len(payload) == 1 {
					return fail(CloseProtocol, "close frame with one-byte payload")
				}
				if len(payload) >= 2 {
					code := int(binary.BigEndian.Uint16(payload))
					if !validWireCloseCode(code) || !utf8.Valid(payload[2:]) {
						return fail(CloseProtocol, "invalid close code or reason")
					}
				}
			}
			return op, payload, nil
		case OpContinuation:
			if !c.hasFrag {
				return fail(CloseProtocol, "continuation without a started message")
			}
			if int64(len(c.frag))+int64(len(payload)) > c.maxMsg {
				return fail(CloseTooLarge, "message over %d bytes", c.maxMsg)
			}
			c.frag = append(c.frag, payload...)
			if !fin {
				continue
			}
			op, msg := c.fragOp, c.frag
			c.hasFrag, c.frag = false, nil
			if op == OpText && !utf8.Valid(msg) {
				return 0, nil, protoErr(CloseInvalidPayload, "text message is not valid UTF-8")
			}
			return op, msg, nil
		case OpText, OpBinary:
			if c.hasFrag {
				return fail(CloseProtocol, "new data frame during a fragmented message")
			}
			if int64(len(payload)) > c.maxMsg {
				return fail(CloseTooLarge, "message over %d bytes", c.maxMsg)
			}
			if !fin {
				c.fragOp = op
				c.hasFrag = true
				c.frag = append([]byte(nil), payload...)
				continue
			}
			if op == OpText && !utf8.Valid(payload) {
				return fail(CloseInvalidPayload, "text message is not valid UTF-8")
			}
			return op, payload, nil
		default:
			if op >= 0x8 && op <= 0xF {
				return fail(CloseProtocol, "unknown control opcode 0x%x", op)
			}
			return fail(CloseUnsupportedData, "unexpected data opcode 0x%x", op)
		}
	}
}

func validWireCloseCode(code int) bool {
	switch {
	case code >= 3000 && code < 5000:
		return true
	case code >= 1000 && code <= 1004:
		return true
	case code >= 1007 && code <= 1014:
		return true
	default:
		return false // 1005/1006/1015 are reserved for the client layer
	}
}

// ParseClose reads a close frame payload into code and reason. An empty
// payload yields 1005 (no status received), per RFC 6455 §7.4.
func ParseClose(payload []byte) (code int, reason string) {
	if len(payload) == 0 {
		return 1005, ""
	}
	if len(payload) < 2 {
		return CloseProtocol, ""
	}
	return int(binary.BigEndian.Uint16(payload)), string(payload[2:])
}

func (c *Conn) readFrame() (bool, Opcode, []byte, error) {
	var head [2]byte
	if _, err := io.ReadFull(c.br, head[:]); err != nil {
		return false, 0, nil, err
	}
	fin := head[0]&0x80 != 0
	if head[0]&0x70 != 0 {
		return false, 0, nil, protoErr(CloseProtocol, "RSV bits set without a negotiated extension")
	}
	op := Opcode(head[0] & 0x0F)
	masked := head[1]&0x80 != 0
	if c.isClient && masked {
		return false, 0, nil, protoErr(CloseProtocol, "server frame must not be masked")
	}
	if !c.isClient && !masked {
		return false, 0, nil, protoErr(CloseProtocol, "client frame must be masked")
	}
	n := int64(head[1] & 0x7F)
	switch n {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		n = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		if ext[0]&0x80 != 0 {
			return false, 0, nil, protoErr(CloseProtocol, "frame length uses the high bit")
		}
		n = int64(binary.BigEndian.Uint64(ext[:]))
	}
	if op >= 0x8 { // control frames: small, unfragmented
		if !fin || n > 125 {
			return false, 0, nil, protoErr(CloseProtocol, "control frame too big or fragmented")
		}
	}
	var key [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, key[:]); err != nil {
			return false, 0, nil, err
		}
	}
	if n > c.maxMsg+1024 { // absurd frame: stop reading rather than buffer it
		return false, 0, nil, protoErr(CloseTooLarge, "frame over %d bytes", c.maxMsg)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(c.br, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	return fin, op, payload, nil
}

// WriteMessage writes one whole data message (text or binary) in a single
// FIN frame.
func (c *Conn) WriteMessage(ctx context.Context, op Opcode, data []byte) error {
	if op != OpText && op != OpBinary {
		return fmt.Errorf("realtime: websocket: WriteMessage needs a data opcode, got 0x%x", op)
	}
	return c.write(ctx, true, op, data)
}

// WriteClose writes a close frame with the given code and reason.
func (c *Conn) WriteClose(ctx context.Context, code int, reason string) error {
	payload := make([]byte, 2+len(reason))
	binary.BigEndian.PutUint16(payload, uint16(code))
	copy(payload[2:], reason)
	return c.write(ctx, true, OpClose, payload)
}

// WritePing writes a WebSocket ping control frame.
func (c *Conn) WritePing(ctx context.Context, data []byte) error {
	return c.write(ctx, true, OpPing, data)
}

// WritePong writes a WebSocket pong control frame.
func (c *Conn) WritePong(ctx context.Context, data []byte) error {
	return c.write(ctx, true, OpPong, data)
}

func (c *Conn) write(ctx context.Context, fin bool, op Opcode, data []byte) error {
	if op >= 0x8 && len(data) > 125 {
		return fmt.Errorf("realtime: websocket: control payload must fit 125 bytes, got %d", len(data))
	}
	if d, ok := ctx.Deadline(); ok {
		_ = c.nc.SetWriteDeadline(d)
	} else {
		_ = c.nc.SetWriteDeadline(time.Time{})
	}
	buf := make([]byte, 0, 14+len(data))
	var head [2]byte
	if fin {
		head[0] |= 0x80
	}
	head[0] |= byte(op)
	switch n := len(data); {
	case n < 126:
		head[1] = byte(n)
		buf = append(buf, head[:]...)
	case n < 1<<16:
		head[1] = 126
		buf = append(buf, head[:]...)
		var ext [2]byte
		binary.BigEndian.PutUint16(ext[:], uint16(n))
		buf = append(buf, ext[:]...)
	default:
		head[1] = 127
		buf = append(buf, head[:]...)
		var ext [8]byte
		binary.BigEndian.PutUint64(ext[:], uint64(n))
		buf = append(buf, ext[:]...)
	}
	if c.isClient {
		buf[1] |= 0x80 // mask bit rides on the byte already copied into buf
		var key [4]byte
		if _, err := rand.Read(key[:]); err != nil {
			return err
		}
		buf = append(buf, key[:]...)
		for i, b := range data {
			buf = append(buf, b^key[i%4])
		}
	} else {
		buf = append(buf, data...)
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_, err := c.nc.Write(buf)
	return err
}

// Dial connects a WebSocket client, performs the RFC 6455 handshake and
// returns the raw connection plus the server's response headers (for the
// negotiated Sec-WebSocket-Protocol). ws/wss accept http/https as aliases.
// maxMessageBytes caps inbound messages; zero means 1 MiB. A refused
// handshake returns an error naming the HTTP status.
func Dial(ctx context.Context, rawURL string, header http.Header, maxMessageBytes int64) (*Conn, http.Header, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, err
	}
	secure := false
	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme, secure = "https", true
	case "http", "https":
		secure = u.Scheme == "https"
	default:
		return nil, nil, fmt.Errorf("realtime: websocket: unsupported scheme %q", u.Scheme)
	}
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		return nil, nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", key)
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	host, port := u.Hostname(), u.Port()
	if port == "" {
		if secure {
			port = "443"
		} else {
			port = "80"
		}
	}
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, nil, err
	}
	if secure {
		tc := tls.Client(nc, &tls.Config{ServerName: host})
		if err := tc.HandshakeContext(ctx); err != nil {
			nc.Close()
			return nil, nil, err
		}
		nc = tc
	}
	if err := req.Write(nc); err != nil {
		nc.Close()
		return nil, nil, err
	}
	br := bufio.NewReader(nc)
	resp, err := http.ReadResponse(br, req)
	if err != nil {
		nc.Close()
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		// A refusal is an ordinary HTTP response: drain the problem body
		// (never past the cap) so the close is clean, then report the status.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		nc.Close()
		return nil, resp.Header, fmt.Errorf("realtime: websocket: handshake refused with status %d", resp.StatusCode)
	}
	// 101 has no body to close; anything already buffered in br belongs to
	// the frame stream, so hand br to the Conn untouched.
	if !tokenIn(resp.Header.Get("Upgrade"), "websocket") || !tokenIn(resp.Header.Get("Connection"), "upgrade") {
		nc.Close()
		return nil, resp.Header, protoErr(CloseProtocol, "response does not upgrade to websocket")
	}
	if resp.Header.Get("Sec-WebSocket-Accept") != AcceptKey(key) {
		nc.Close()
		return nil, resp.Header, protoErr(CloseProtocol, "wrong Sec-WebSocket-Accept")
	}
	return newConn(nc, br, true, maxMessageBytes), resp.Header, nil
}

func tokenIn(header, token string) bool {
	for _, part := range strings.Split(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
