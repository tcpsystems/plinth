package reqid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
)

// Trace is a W3C Trace Context (traceparent/tracestate) carried with a request.
type Trace struct {
	TraceID, SpanID, Flags, State string
}

// Traceparent renders the header value.
func (t Trace) Traceparent() string { return "00-" + t.TraceID + "-" + t.SpanID + "-" + t.Flags }

type traceKey struct{}

// TraceFromContext returns the request's trace, if the middleware ran.
func TraceFromContext(ctx context.Context) (Trace, bool) {
	t, ok := ctx.Value(traceKey{}).(Trace)
	return t, ok
}

func isHex(s string, n int) bool {
	if len(s) != n {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func zeros(s string) bool {
	for _, c := range s {
		if c != '0' {
			return false
		}
	}
	return true
}

// ParseTraceparent validates a version-00 traceparent value.
func ParseTraceparent(v string) (Trace, bool) {
	if len(v) != 55 || v[2] != '-' || v[35] != '-' || v[52] != '-' || v[:2] != "00" {
		return Trace{}, false
	}
	t := Trace{TraceID: v[3:35], SpanID: v[36:52], Flags: v[53:]}
	if !isHex(t.TraceID, 32) || !isHex(t.SpanID, 16) || !isHex(t.Flags, 2) || zeros(t.TraceID) || zeros(t.SpanID) {
		return Trace{}, false
	}
	return t, true
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// TraceMiddleware keeps a valid incoming trace id (with a fresh span id) or
// starts a new trace, stores it in the context and echoes traceparent. A
// tracestate header is forwarded only together with a valid traceparent.
func TraceMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t, ok := ParseTraceparent(r.Header.Get("traceparent"))
		if ok {
			if s := r.Header.Get("tracestate"); len(s) <= 512 {
				t.State = s
			}
			t.SpanID = randHex(8)
		} else {
			t = Trace{TraceID: randHex(16), SpanID: randHex(8), Flags: "00"}
		}
		r = r.Clone(context.WithValue(r.Context(), traceKey{}, t))
		r.Header.Set("traceparent", t.Traceparent())
		w.Header().Set("traceparent", t.Traceparent())
		next.ServeHTTP(w, r)
	})
}

// Inject copies the context's trace onto an outgoing request.
func Inject(ctx context.Context, h http.Header) {
	if t, ok := TraceFromContext(ctx); ok {
		h.Set("traceparent", t.Traceparent())
		if t.State != "" {
			h.Set("tracestate", t.State)
		}
	}
}
