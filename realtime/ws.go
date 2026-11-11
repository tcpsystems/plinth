package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tcpsystems/plinth/jsonx"
	"github.com/tcpsystems/plinth/problem"
)

// WebSocket transport.
//
// WS returns an http.Handler that upgrades to RFC 6455 and runs a
// bidirectional session over this hub. Before the 101 response the upgrade
// request gets the same checks as SSE — Origin allowlist, Options.Auth,
// topics, per-topic authorization — and the RFC 6455 handshake itself is
// validated (Connection/Upgrade tokens, version 13, Sec-WebSocket-Key). A
// refused upgrade is an ordinary problem+json HTTP response; the browser
// sees it as a failed WebSocket connection.
//
// The client may send ?last_seq=N to resume directly after a reconnect
// (the WS analogue of Last-Event-ID); it may also resume mid-session with
// a {"type":"resume","seq":N} frame.
//
// Outbound frames are JSON objects with a "type" discriminator:
//
//	{"type":"event","seq":N,"topic":"…","kind":"…","data":…}   one hub delta
//	{"type":"snapshot","seq":as_of_seq,"data":…}               full state
//	{"type":"reset","seq":head,"data":{"reason":"…"}}          cursor lost
//	{"type":"pong","seq":N}                                    app-level ping reply
//
// seq is always the hub sequence number, the same cursor space as SSE
// event ids, so one client-side dedupe works across both transports.
//
// Inbound frames: {"type":"resume","seq":N} rewinds from the ring;
// {"type":"ping","seq":N} is answered with a pong frame;
// {"type":"msg","seq":N,"topic":"…","data":…} goes to Options.Inbound.
// Binary frames close 1003, malformed JSON closes 1007, an unknown type
// closes 1002. A "msg" frame with no Options.Inbound closes 1008: the
// session is one-way and the client should use SSE.
//
// Liveness: the server sends an RFC 6455 ping every Options.PingInterval
// and closes 1001 if the peer has not ponged before the next one.
//
// Backpressure and reset semantics: when the hub drops this subscriber
// (queue full) the remaining buffered events are delivered, a reset frame
// announces the head, and the session closes 1013 TryLater — the client
// reconnects with last_seq = that head, which is in range, so resume
// cannot loop. A resume (initial or in-frame) whose cursor is no longer
// retained gets a reset frame plus a fresh snapshot and the session keeps
// streaming: unlike SSE, a WebSocket client does not need to reconnect to
// recover.
func (h *Hub) WS(o Options) http.Handler {
	o = o.withDefaults()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := wsUpgradeErr(r); err != nil {
			problem.WriteError(w, r, err)
			return
		}
		if err := originErr(o, r); err != nil {
			problem.WriteError(w, r, err)
			return
		}
		p, err := authenticate(o, r)
		if err != nil {
			problem.WriteError(w, r, err)
			return
		}
		topics, err := parseTopics(r)
		if err != nil {
			problem.WriteError(w, r, err)
			return
		}
		if err := authorizeErr(o, p, topics); err != nil {
			problem.WriteError(w, r, err)
			return
		}
		chosen, err := negotiateSubprotocol(o.Subprotocols, r.Header.Get("Sec-WebSocket-Protocol"))
		if err != nil {
			problem.WriteError(w, r, err)
			return
		}
		key := r.Header.Get("Sec-WebSocket-Key")
		cursor, _ := strconv.ParseUint(r.URL.Query().Get("last_seq"), 10, 64)

		// Subscribe and build the snapshot before hijacking, so a rejection
		// is still a clean problem response (same ordering rationale as SSE).
		sub := h.Subscribe(topics, cursor)
		defer sub.Cancel()
		gap := closedNow(sub.Reset)
		needSnap := o.Snapshot != nil && (cursor == 0 || gap)
		var snapData json.RawMessage
		var snapSeq uint64
		if needSnap {
			b, asOf, err := o.Snapshot(r.Context(), p, topics)
			if err != nil {
				problem.WriteError(w, r, err)
				return
			}
			snapData, snapSeq = b, asOf
		}

		hj, ok := w.(http.Hijacker)
		if !ok {
			problem.WriteError(w, r, problem.NewStatus(http.StatusInternalServerError, nil, "hijack_unsupported", "server cannot upgrade to websocket"))
			return
		}
		nc, rw, err := hj.Hijack()
		if err != nil {
			return // the server gives up on this connection
		}
		defer nc.Close()

		var hdrs strings.Builder
		hdrs.WriteString("HTTP/1.1 101 Switching Protocols\r\n")
		hdrs.WriteString("Upgrade: websocket\r\n")
		hdrs.WriteString("Connection: Upgrade\r\n")
		hdrs.WriteString("Sec-WebSocket-Accept: " + AcceptKey(key) + "\r\n")
		if chosen != "" {
			hdrs.WriteString("Sec-WebSocket-Protocol: " + chosen + "\r\n")
		}
		hdrs.WriteString("\r\n")
		// Write through the hijacked ReadWriter (and flush anything it still
		// holds) so the 101 is the first thing on the wire after the request.
		if _, err := io.WriteString(rw, hdrs.String()); err != nil {
			return
		}
		if err := rw.Flush(); err != nil {
			return
		}
		// Any bytes the scanner buffered past the handshake belong to the
		// frame stream; hand the same *bufio.Reader to the Conn.
		conn := newConn(nc, rw.Reader, false, o.MaxMessageBytes)
		runWSSession(r.Context(), conn, h, sub, o, p, topics, gap, snapData, snapSeq)
	})
}

// wsUpgradeErr validates the RFC 6455 client handshake headers (§4.2.1)
// before anything is hijacked.
func wsUpgradeErr(r *http.Request) error {
	if !tokenIn(r.Header.Get("Connection"), "upgrade") || !tokenIn(r.Header.Get("Upgrade"), "websocket") {
		return problem.New(problem.ErrInvalid, "websocket_upgrade_required", "request is not a WebSocket upgrade")
	}
	if r.Header.Get("Sec-WebSocket-Version") != "13" {
		return problem.New(problem.ErrInvalid, "websocket_version_unsupported", "only RFC 6455 version 13 is supported")
	}
	if r.Header.Get("Sec-WebSocket-Key") == "" {
		return problem.New(problem.ErrInvalid, "websocket_key_missing", "Sec-WebSocket-Key is required")
	}
	return nil
}

// negotiateSubprotocol returns the first server offer the client also
// offered (server order wins, newest first). When the server has a profile
// list and nothing overlaps the upgrade is refused — silently ignoring a
// version mismatch would let clients speak a dialect the server then
// misparses. With no server list the client's offer is ignored.
func negotiateSubprotocol(offer []string, request string) (string, error) {
	if len(offer) == 0 {
		return "", nil
	}
	for _, v := range offer {
		if tokenIn(request, v) {
			return v, nil
		}
	}
	return "", problem.New(problem.ErrInvalid, "subprotocol_mismatch", "client and server share no WebSocket subprotocol")
}

// wsInbound is one reader-goroutine result: a raw frame or the error that
// ended reading.
type wsInbound struct {
	op      Opcode
	payload []byte
	err     error
}

func runWSSession(ctx context.Context, conn *Conn, h *Hub, sub *Subscription, o Options, principal any, topics []string, gap bool, snapData json.RawMessage, snapSeq uint64) {
	// Every write gets its own timeout window from now; a session that lives
	// longer than Options.WriteTimeout must not find its deadlines spent.
	write := func(f func(ctx context.Context) error) error {
		wctx, cancel := context.WithTimeout(ctx, o.WriteTimeout)
		defer cancel()
		return f(wctx)
	}
	send := func(typ string, seq uint64, topic, kind string, data json.RawMessage) error {
		b, err := jsonx.Marshal(wsFrame{Type: typ, Seq: seq, Topic: topic, Kind: kind, Data: data})
		if err != nil {
			return err
		}
		return write(func(c context.Context) error { return conn.WriteMessage(c, OpText, b) })
	}
	sendReset := func(reason string) error {
		b, _ := jsonx.Marshal(map[string]string{"reason": reason})
		return send("reset", h.Head(), "", "", b)
	}
	closeSess := func(code int, reason string) {
		_ = write(func(c context.Context) error { return conn.WriteClose(c, code, reason) })
	}
	defer conn.CloseNow()

	drain := func() {
		for {
			select {
			case ev, open := <-sub.Events:
				if !open {
					return
				}
				if err := send("event", ev.Seq, ev.Topic, ev.Kind, ev.Data); err != nil {
					return
				}
			default:
				return
			}
		}
	}

	if gap {
		// Like SSE: announce with the head so the client's next reconnect
		// (or resume) lands in range, hand it whatever still made the
		// buffer, then try-later close.
		if err := sendReset("cursor_out_of_range"); err != nil {
			return
		}
		if snapData != nil {
			if err := send("snapshot", snapSeq, "", "", snapData); err != nil {
				return
			}
		}
		drain()
		closeSess(CloseTryLater, "reconnect from the announced head")
		return
	}
	if snapData != nil {
		if err := send("snapshot", snapSeq, "", "", snapData); err != nil {
			return
		}
	}

	frames := make(chan wsInbound, 8)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			op, payload, err := conn.Read(ctx)
			select {
			case frames <- wsInbound{op, payload, err}:
			case <-done:
				return
			}
			if err != nil {
				return
			}
		}
	}()

	ping := time.NewTicker(o.PingInterval)
	defer ping.Stop()
	awaitingPong := false

	for {
		select {
		case <-ctx.Done():
			return // server shutting down: CloseNow above ends the session

		case ev, open := <-sub.Events:
			if !open {
				closeSess(CloseGoingAway, "hub closed")
				return
			}
			if err := send("event", ev.Seq, ev.Topic, ev.Kind, ev.Data); err != nil {
				return // stuck or gone peer
			}

		case <-sub.Reset:
			// The hub dropped this subscriber: deliver what is buffered,
			// announce, and try-later close so the client resumes from a
			// fresh (live) subscription instead of a silently dead one.
			drain()
			_ = sendReset("backpressure")
			closeSess(CloseTryLater, "retry from the last seen seq")
			return

		case <-ping.C:
			if awaitingPong {
				closeSess(CloseGoingAway, "no pong")
				return
			}
			if err := write(func(c context.Context) error { return conn.WritePing(c, nil) }); err != nil {
				return
			}
			awaitingPong = true

		case in := <-frames:
			if in.err != nil {
				var pe *ProtoError
				if errors.As(in.err, &pe) {
					closeSess(pe.CloseCode, pe.Error())
				}
				return // io.EOF, closed conn, bad frame: all terminal here
			}
			switch in.op {
			case OpPing:
				payload := in.payload
				if err := write(func(c context.Context) error { return conn.WritePong(c, payload) }); err != nil {
					return
				}
			case OpPong:
				awaitingPong = false
			case OpClose:
				code, reason := ParseClose(in.payload)
				if code == 1005 || code == 1006 { // no status / abnormal: echo normal
					code, reason = CloseNormal, ""
				}
				closeSess(code, reason)
				return
			case OpBinary:
				closeSess(CloseUnsupportedData, "this profile carries JSON text frames only")
				return
			case OpText:
				closeCode, msg, quit := handleWSFrame(ctx, conn, h, sub, o, principal, topics, in.payload)
				if quit {
					if closeCode != 0 {
						closeSess(closeCode, msg)
					}
					return
				}
			default:
				closeSess(CloseProtocol, "unexpected data opcode")
				return
			}
		}
	}
}

// handleWSFrame processes one inbound JSON frame. A close code of 0 means
// keep the session; quit ends it (after the close frame is written).
func handleWSFrame(ctx context.Context, conn *Conn, h *Hub, sub *Subscription, o Options, principal any, topics []string, payload []byte) (closeCode int, msg string, quit bool) {
	var f wsFrame
	if err := json.Unmarshal(payload, &f); err != nil {
		return CloseInvalidPayload, "frame is not a JSON object", true
	}
	switch f.Type {
	case "resume":
		err := sub.Rewind(f.Seq)
		if errors.Is(err, ErrCursorGone) {
			// In place, no reconnect: reset announces the head, the
			// snapshot rebuilds state, live delivery already continues.
			send := func(typ string, seq uint64, data json.RawMessage) {
				b, err := jsonx.Marshal(wsFrame{Type: typ, Seq: seq, Data: data})
				if err != nil {
					return
				}
				wctx, cancel := context.WithTimeout(ctx, o.WriteTimeout)
				defer cancel()
				_ = conn.WriteMessage(wctx, OpText, b)
			}
			b, _ := jsonx.Marshal(map[string]string{"reason": "cursor_out_of_range"})
			send("reset", h.Head(), b)
			if o.Snapshot != nil {
				data, asOf, err := o.Snapshot(ctx, principal, topics)
				if err != nil {
					return CloseInternal, "snapshot failed", true
				}
				send("snapshot", asOf, data)
			}
		} else if err != nil {
			return CloseInternal, "rewind failed", true
		}
		return 0, "", false
	case "ping":
		b, _ := jsonx.Marshal(wsFrame{Type: "pong", Seq: f.Seq})
		wctx, cancel := context.WithTimeout(ctx, o.WriteTimeout)
		defer cancel()
		_ = conn.WriteMessage(wctx, OpText, b)
		return 0, "", false
	case "msg":
		if o.Inbound == nil {
			return ClosePolicy, "this service does not accept inbound messages; use SSE for one-way streams", true
		}
		if err := o.Inbound(ctx, principal, InboundMsg{Seq: f.Seq, Topic: f.Topic, Data: f.Data}); err != nil {
			return CloseInternal, "inbound handler failed", true
		}
		return 0, "", false
	default:
		return CloseProtocol, "unknown frame type " + strconv.Quote(f.Type), true
	}
}

// wsFrame is the wire shape of every outbound frame and of inbound
// resume/ping/msg frames (fields the frame does not use marshal away under
// omitempty).
type wsFrame struct {
	Type  string          `json:"type"`
	Seq   uint64          `json:"seq,omitempty"`
	Topic string          `json:"topic,omitempty"`
	Kind  string          `json:"kind,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}
