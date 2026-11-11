package realtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/tcpsystems/plinth/jsonx"
)

// Connect-JSON unary RPC: the keel subset of the Connect protocol. POST (and
// GET for idempotent methods) to {prefix}/rpc/{pkg}.{Service}/{Method} with
// application/json bodies, the Connect code→HTTP status mapping and the
// Connect wire error object. It gives services a schema-first RPC surface
// with zero dependencies; full protobuf codecs, server streaming and gRPC
// reflection are the connect-go/grpc-go adoption path, which must stay
// wire-compatible with what is checked here.
//
// The wire facts implemented here are taken from the Connect protocol spec
// and connectrpc.com/connect v1.21.1 (protocol_connect.go): the error codes,
// the code→HTTP table, Connect-Timeout-Ms and Connect-Protocol-Version
// handling, the unary GET query parameters (connect=v1, message, encoding,
// base64, compression) and the JSON error shape
// {"code":…,"message":…,"details":[{"type":…,"value":…,"debug":…}]}.

// Connect wire error codes (the eleven gRPC codes Connect defines plus the
// three Connect-specific ones).
const (
	CodeCanceled           = "canceled"
	CodeUnknown            = "unknown"
	CodeInvalidArgument    = "invalid_argument"
	CodeDeadlineExceeded   = "deadline_exceeded"
	CodeNotFound           = "not_found"
	CodeAlreadyExists      = "already_exists"
	CodePermissionDenied   = "permission_denied"
	CodeResourceExhausted  = "resource_exhausted"
	CodeFailedPrecondition = "failed_precondition"
	CodeAborted            = "aborted"
	CodeOutOfRange         = "out_of_range"
	CodeUnimplemented      = "unimplemented"
	CodeInternal           = "internal"
	CodeUnavailable        = "unavailable"
	CodeDataLoss           = "data_loss"
	CodeUnauthenticated    = "unauthenticated"
)

// codeToHTTP is the authoritative Connect unary code→HTTP status mapping,
// transcribed verbatim from connect-go v1.21.1 (protocol_connect.go,
// connectCodeToHTTP). A client reading connect-go trailers sees the same
// code here, so the representations cannot drift within one service.
var codeToHTTP = map[string]int{
	CodeCanceled:           499, // nginx's status for a client-side cancel
	CodeUnknown:            500,
	CodeInvalidArgument:    400,
	CodeDeadlineExceeded:   504,
	CodeNotFound:           404,
	CodeAlreadyExists:      409,
	CodePermissionDenied:   403,
	CodeResourceExhausted:  429,
	CodeFailedPrecondition: 400,
	CodeAborted:            409,
	CodeOutOfRange:         400,
	CodeUnimplemented:      501,
	CodeInternal:           500,
	CodeUnavailable:        503,
	CodeDataLoss:           500,
	CodeUnauthenticated:    401,
}

// httpStatusForCode maps a Connect error code to its unary HTTP status
// (default 500, like connect-go's switch default).
func httpStatusForCode(code string) int {
	if s, ok := codeToHTTP[code]; ok {
		return s
	}
	return 500
}

// ConnectDetail is one error detail. Type is the type URL
// (e.g. "google.rpc.ErrorInfo"); Value is its message bytes, carried on the
// JSON wire as base64 (encoding/json marshals []byte that way, matching
// connect-go's Any-value encoding); Debug is an optional human-readable form
// of the message, omitted when nil.
type ConnectDetail struct {
	Type  string `json:"type"`
	Value []byte `json:"value,omitempty"`
	Debug any    `json:"debug,omitempty"`
}

// ConnectError is an RPC failure in Connect terms. Handlers registered on an
// [RPC] should return these (directly or via [AsConnectError] wrappers);
// anything else is reported as internal with a fixed message so services
// cannot leak internals into a generated client's typed error.
type ConnectError struct {
	Code    string
	Message string
	Details []ConnectDetail

	// httpStatus overrides the code→HTTP table for protocol-level
	// rejections the spec pins to a specific status: an unsupported
	// Message-Codec must be 415 Unsupported Media Type even though its
	// code is unimplemented (501 by table). Zero means use the table;
	// service-returned errors can never set it, so they stay honest to
	// the table a generated client was built against.
	httpStatus int
}

// Error formats the Connect error code and message.
func (e *ConnectError) Error() string { return e.Code + ": " + e.Message }

// NewConnectError builds a ConnectError with a standard code.
func NewConnectError(code, message string) *ConnectError {
	return &ConnectError{Code: code, Message: message}
}

// WithDetails attaches details and returns e.
func (e *ConnectError) WithDetails(d ...ConnectDetail) *ConnectError {
	e.Details = append(e.Details, d...)
	return e
}

// errorBody is the Connect wire error JSON.
type errorBody struct {
	Code    string          `json:"code"`
	Message string          `json:"message,omitempty"`
	Details []ConnectDetail `json:"details,omitempty"`
}

// UnaryFunc is one RPC method. The request is the raw JSON request body;
// return the raw JSON response (or nil/empty for an empty message). A
// returned *ConnectError becomes the wire error; any other error becomes
// internal. The ctx carries the deadline derived from Connect-Timeout-Ms,
// clamped to Options.MaxRPCTimeout.
type UnaryFunc func(ctx context.Context, principal any, request json.RawMessage) (json.RawMessage, error)

// RPC is a registry of Connect-JSON unary methods, served under one mounted
// path prefix. It is safe to register during startup and read concurrently
// once mounted.
type RPC struct {
	methods map[string]rpcMethod
}

type rpcMethod struct {
	fn         UnaryFunc
	idempotent bool
}

// NewRPC returns an empty registry.
func NewRPC() *RPC { return &RPC{methods: map[string]rpcMethod{}} }

// Register adds a method under its fully qualified name
// "pkg.Service/Method" (this is the URL segment clients POST to; the schema
// index carries the same string). Panics on a duplicate or malformed name:
// registration is startup code, not request handling.
func (r *RPC) Register(name string, fn UnaryFunc) { r.add(name, fn, false) }

// RegisterIdempotent adds a method that also answers the Connect unary GET
// form (NO_SIDE_EFFECTS in protobuf terms): the request arrives in the
// message query parameter and caches/CDNs may hold the response, so handlers
// must not mutate state.
func (r *RPC) RegisterIdempotent(name string, fn UnaryFunc) { r.add(name, fn, true) }

func (r *RPC) add(name string, fn UnaryFunc, idempotent bool) {
	if fn == nil {
		panic("realtime: rpc: nil handler for " + name)
	}
	i := strings.IndexByte(name, '/')
	if i <= 0 || i == len(name)-1 || strings.ContainsAny(name[i+1:], "/") {
		panic(`realtime: rpc: method name must be "pkg.Service/Method", got ` + strconv.Quote(name))
	}
	if _, dup := r.methods[name]; dup {
		panic("realtime: rpc: duplicate method " + name)
	}
	r.methods[name] = rpcMethod{fn: fn, idempotent: idempotent}
}

// Handler serves the registry. o supplies Auth and the MaxRPCTimeout clamp.
// Mount it under "{prefix}/rpc/{spec...}" (see [Register]); the spec
// wildcard captures "pkg.Service/Method".
func (r *RPC) Handler(o Options) http.Handler {
	o = o.withDefaults()
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		spec := req.PathValue("spec")
		m, ok := r.methods[spec]
		if !ok {
			writeConnectError(w, CodeUnimplemented, `method "`+spec+`" not found`, nil, 0)
			return
		}
		p, err := authenticate(o, req)
		if err != nil {
			// The connect wire owns this surface: an Auth failure is always
			// unauthenticated, whatever the inner error was.
			writeConnectError(w, CodeUnauthenticated, "unauthenticated", nil, 0)
			return
		}
		ctx, cancel, err := connectTimeout(req.Context(), req.Header.Get("Connect-Timeout-Ms"), o.MaxRPCTimeout)
		if err != nil {
			writeConnectErr(w, err)
			return
		}
		defer cancel()
		if ce := connectVersionErr(req); ce != nil {
			writeConnectErr(w, ce)
			return
		}

		var body json.RawMessage
		switch req.Method {
		case http.MethodPost:
			if ce := contentTypeErr(req); ce != nil {
				writeConnectErr(w, ce)
				return
			}
			b, err := io.ReadAll(http.MaxBytesReader(w, req.Body, rpcMaxBodyBytes))
			if err != nil {
				writeConnectError(w, CodeInvalidArgument, "request body too large or unreadable", nil, 0)
				return
			}
			body = b
		case http.MethodGet:
			if !m.idempotent {
				writeConnectError(w, CodeInvalidArgument, "GET requires a method registered as idempotent", nil, 0)
				return
			}
			var ce *ConnectError
			body, ce = connectGET(req)
			if ce != nil {
				writeConnectErr(w, ce)
				return
			}
		default:
			writeConnectError(w, CodeInvalidArgument, "unsupported method "+strconv.Quote(req.Method), nil, 0)
			return
		}
		if len(body) == 0 {
			body = json.RawMessage("{}")
		}

		resp, err := m.fn(ctx, p, body)
		if err != nil {
			writeConnectErr(w, err)
			return
		}
		if len(resp) == 0 {
			resp = json.RawMessage("{}")
		}
		if !json.Valid(resp) {
			writeConnectError(w, CodeInternal, "internal server error", nil, 0)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(resp)
	})
}

// rpcMaxBodyBytes caps one Connect request body.
const rpcMaxBodyBytes = 1 << 20

// connectVersionErr checks the Connect-Protocol-Version header (POST) or the
// connect query parameter (GET). An absent header on POST is accepted as v1,
// like connect-go when the handler is not version-required; GET demands the
// parameter because the request shape is only defined for v1.
func connectVersionErr(req *http.Request) error {
	if req.Method == http.MethodGet {
		v := req.URL.Query().Get("connect")
		if v == "" {
			return NewConnectError(CodeInvalidArgument, `missing required parameter "connect"`)
		}
		if v != "v1" {
			return NewConnectError(CodeInvalidArgument, `parameter "connect" must be "v1", got `+strconv.Quote(v))
		}
		return nil
	}
	if v := req.Header.Get("Connect-Protocol-Version"); v != "" && v != "1" {
		return NewConnectError(CodeInvalidArgument, `header "Connect-Protocol-Version" must be "1", got `+strconv.Quote(v))
	}
	return nil
}

// contentTypeErr enforces the standard JSON profile: application/json only.
// An unsupported Message-Codec is CodeUnimplemented served with HTTP 415
// Unsupported Media Type — the spec's "must respond with an HTTP status
// code of 415" for exactly this case, which the code→HTTP table (501)
// would otherwise contradict.
func contentTypeErr(req *http.Request) error {
	ct := req.Header.Get("Content-Type")
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	if !strings.EqualFold(ct, "application/json") {
		return &ConnectError{
			Code:       CodeUnimplemented,
			Message:    "invalid content-type: " + strconv.Quote(req.Header.Get("Content-Type")) + `; expecting "application/json"`,
			httpStatus: http.StatusUnsupportedMediaType,
		}
	}
	return nil
}

// connectGET extracts the request message from a unary GET: ?message=… with
// ?encoding=json, ?base64=1 for base64url (no padding) and ?compression
// identity-only (the standard profile does not gzip small query payloads).
func connectGET(req *http.Request) (json.RawMessage, *ConnectError) {
	q := req.URL.Query()
	if enc := q.Get("encoding"); enc != "json" {
		// Same rule as the POST content-type: an unsupported codec is
		// unimplemented at HTTP 415, not 501.
		return nil, &ConnectError{
			Code:       CodeUnimplemented,
			Message:    `parameter "encoding" must be "json", got ` + strconv.Quote(enc),
			httpStatus: http.StatusUnsupportedMediaType,
		}
	}
	if c := q.Get("compression"); c != "" && c != "identity" {
		return nil, NewConnectError(CodeUnimplemented, `compression `+strconv.Quote(c)+` is not supported for unary GET`)
	}
	msg := q.Get("message")
	if msg == "" {
		return nil, NewConnectError(CodeInvalidArgument, `missing required parameter "message"`)
	}
	if q.Get("base64") == "1" {
		b, err := base64.RawURLEncoding.DecodeString(msg)
		if err != nil {
			return nil, NewConnectError(CodeInvalidArgument, `parameter "message" is not valid base64url`)
		}
		return json.RawMessage(b), nil
	}
	return json.RawMessage(msg), nil
}

// connectTimeout parses Connect-Timeout-Ms like connect-go: 1-10 ASCII
// digits, positive, otherwise invalid_argument; it then clamps the deadline
// to now+min(ms, MaxRPCTimeout) so a client cannot pin a worker forever.
// The returned ctx carries that deadline and cancel releases its timer when
// the handler returns (a no-op when no timeout header was sent).
func connectTimeout(ctx context.Context, header string, max time.Duration) (context.Context, context.CancelFunc, error) {
	noop := func() {}
	if header == "" {
		return ctx, noop, nil
	}
	bad := NewConnectError(CodeInvalidArgument, "invalid Connect-Timeout-Ms: "+strconv.Quote(header))
	if len(header) > 10 {
		return ctx, noop, bad
	}
	for i := 0; i < len(header); i++ {
		if header[i] < '0' || header[i] > '9' {
			return ctx, noop, bad
		}
	}
	ms, err := strconv.ParseUint(header, 10, 64)
	if err != nil || ms == 0 {
		return ctx, noop, bad
	}
	d := time.Duration(ms) * time.Millisecond
	if d > max {
		d = max
	}
	tctx, cancel := context.WithTimeout(ctx, d)
	return tctx, cancel, nil
}

// writeConnectErr renders err on the Connect wire: a *ConnectError keeps its
// code; a context deadline becomes deadline_exceeded; anything else is
// internal with a fixed message.
func writeConnectErr(w http.ResponseWriter, err error) {
	var ce *ConnectError
	if errors.As(err, &ce) {
		writeConnectError(w, ce.Code, ce.Message, ce.Details, ce.httpStatus)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeConnectError(w, CodeDeadlineExceeded, "deadline exceeded", nil, 0)
		return
	}
	writeConnectError(w, CodeInternal, "internal server error", nil, 0)
}

// writeConnectError renders the wire error; a zero status uses the
// code→HTTP table.
func writeConnectError(w http.ResponseWriter, code, message string, details []ConnectDetail, status int) {
	if status == 0 {
		status = httpStatusForCode(code)
	}
	b, err := jsonx.Marshal(errorBody{Code: code, Message: message, Details: details})
	if err != nil {
		http.Error(w, "{}", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
