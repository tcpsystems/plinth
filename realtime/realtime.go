// Package realtime serves three realtime transports from a single in-process
// hub: Server-Sent Events for one-way streams, RFC 6455 WebSockets for
// bidirectional sessions and Connect-JSON unary RPC. Register mounts all of
// them on a [net/http.ServeMux] in one call.
//
// The hub is the delivery fabric of those transports: events are
// seq-ordered, at-least-once, buffered per subscriber and a slow subscriber is
// dropped to a reset so it can resume with a fresh snapshot instead of stalling
// the fan-out. State stays in the service's Postgres (outbox rt_event); the hub
// only carries deltas, so restarting it loses nothing durable.
//
// Standard library only: the WebSocket codec and the Connect envelope are
// hand-rolled here. Services that need full protobuf codecs, gRPC or server
// reflection can adopt connect-go and grpc-go in a nested module and must
// follow the same wire conventions; [conformance.RunRealtime] checks both.
package realtime

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

// Event is one factual change on one topic. Seq is the hub-wide sequence
// number: it is the SSE event id, the WebSocket event frame's seq and the
// resume cursor in Last-Event-ID, so all transports share one cursor space.
// Services that replay from the outbox keep the rt_event seq as-is; Publish
// assigns a seq when Seq is zero.
type Event struct {
	Seq   uint64          `json:"seq"`
	Topic string          `json:"topic"`
	Kind  string          `json:"kind"`
	Data  json.RawMessage `json:"data,omitempty"`
}

// SnapshotFunc builds the full state for a set of topics. The returned
// as_of_seq marks the boundary: events with seq <= as_of_seq are already in
// the snapshot and consumers must drop older deltas on replay. Build the
// snapshot after the subscription is registered (both transports do) so
// deltas that raced ahead of it are still delivered; consumers dedupe by seq.
type SnapshotFunc func(ctx context.Context, principal any, topics []string) (data json.RawMessage, asOfSeq uint64, err error)

// Options configures the SSE and WebSocket handlers. The zero value is
// valid: every field falls back to the standard's default.
type Options struct {
	// Auth authenticates the request before any stream starts: SSE reads
	// cookie or bearer (EventSource cannot set headers, so cross-origin UIs
	// use a SameSite cookie or a short ticket in the query), WebSockets
	// authenticate on the upgrade request. A non-nil error becomes a problem
	// response and the stream never starts. nil means anonymous.
	Auth func(r *http.Request) (any, error)
	// Authorize decides per requested topic whether the principal may
	// subscribe; nil allows everything. Checks run at connection time only;
	// long-lived sessions must be bounded by the service's token lifetime.
	Authorize func(principal any, topic string) bool
	// AllowOrigins, when non-empty, rejects requests whose Origin header is
	// present but not listed (403). Same-origin and non-browser clients send
	// no Origin and pass.
	AllowOrigins []string
	// Subprotocols offered for negotiation on WebSocket upgrades, newest
	// first, for example "app.example.v1". The first overlap with the client's
	// offer wins; with no overlap the upgrade is refused. Empty means no
	// negotiation (the client's offer, if any, is ignored).
	Subprotocols []string
	// Heartbeat is the SSE comment keep-alive interval (the WHATWG authoring
	// note suggests ~15s to survive legacy proxies). Default 15s.
	Heartbeat time.Duration
	// Retry is the SSE retry: reconnection hint. Default 3s.
	Retry time.Duration
	// PingInterval is the WebSocket protocol ping period; the peer must pong
	// before the next ping or the session is closed. Default 20s.
	PingInterval time.Duration
	// WriteTimeout bounds one WebSocket frame write; a frame that cannot be
	// written in time means a stuck peer and the session is closed.
	// Default 10s.
	WriteTimeout time.Duration
	// MaxMessageBytes caps one inbound WebSocket message (fragments
	// included). Over the cap the session closes 1009. Default 64 KiB.
	MaxMessageBytes int64
	// Snapshot, when set, is emitted as an SSE "snapshot" event or a WS
	// "snapshot" frame for fresh or out-of-range subscribers, before the
	// first delta.
	Snapshot SnapshotFunc
	// Inbound, when set, makes WebSocket sessions bidirectional: every
	// {"type":"msg",…} text frame is decoded as an [InboundMsg] and the
	// handler runs on the session's goroutine. The handler replies by
	// publishing to the hub (every subscriber on the matching topics, this
	// session included) or by writing its own frames if it holds the
	// [Conn]. A returned error closes the session 1011 — inbound handling
	// is not retried. Without it, "msg" frames close the session 1008.
	// SSE has no inbound path by definition: one-way streams stay SSE,
	// bidirectional sessions are WebSockets.
	Inbound func(ctx context.Context, principal any, msg InboundMsg) error
	// MaxRPCTimeout clamps a client-supplied Connect-Timeout-Ms deadline so
	// a caller cannot pin a server worker forever. Default 60s.
	MaxRPCTimeout time.Duration
}

// InboundMsg is one application frame from a WebSocket client:
// {"type":"msg","seq":N,"topic":"…","data":…}. Seq is the client's own
// frame counter (for its ordering/dedupe, the server does not interpret
// it); Topic is optional, for clients that multiplex over one connection.
// The handler publishes results as hub events; consumers deduplicate by
// hub seq, not by client seq.
type InboundMsg struct {
	Seq   uint64          `json:"seq,omitempty"`
	Topic string          `json:"topic,omitempty"`
	Data  json.RawMessage `json:"data,omitempty"`
}

func (o Options) withDefaults() Options {
	if o.Heartbeat <= 0 {
		o.Heartbeat = 15 * time.Second
	}
	if o.Retry <= 0 {
		o.Retry = 3 * time.Second
	}
	if o.PingInterval <= 0 {
		o.PingInterval = 20 * time.Second
	}
	if o.WriteTimeout <= 0 {
		o.WriteTimeout = 10 * time.Second
	}
	if o.MaxMessageBytes <= 0 {
		o.MaxMessageBytes = 64 << 10
	}
	if o.MaxRPCTimeout <= 0 {
		o.MaxRPCTimeout = 60 * time.Second
	}
	if o.Authorize == nil {
		o.Authorize = func(any, string) bool { return true }
	}
	return o
}
