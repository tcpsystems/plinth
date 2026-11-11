package reqid

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTraceparent(t *testing.T) {
	const in = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	var got Trace
	h := TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = TraceFromContext(r.Context())
		out := http.Header{}
		Inject(r.Context(), out)
		if out.Get("traceparent") != got.Traceparent() || out.Get("tracestate") != got.State {
			t.Fatal(out)
		}
	}))
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("traceparent", in)
	r.Header.Set("tracestate", "k=v")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if got.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || got.SpanID == "00f067aa0ba902b7" || w.Header().Get("traceparent") != got.Traceparent() {
		t.Fatal(got)
	}
	for _, bad := range []string{"", "garbage", "00-" + strings.Repeat("0", 32) + "-00f067aa0ba902b7-01", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("traceparent", bad)
		r.Header.Set("tracestate", "x=y")
		got = Trace{}
		h.ServeHTTP(httptest.NewRecorder(), r)
		if _, ok := ParseTraceparent(got.Traceparent()); !ok || got.State != "" {
			t.Fatalf("%q -> %+v", bad, got)
		}
	}
}
