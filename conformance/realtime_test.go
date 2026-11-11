package conformance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/realtime"
	"github.com/tcpsystems/plinth/reqid"
)

// TestRunRealtimeDogfood runs the realtime conformance suite against plinth's
// own [realtime.Register] — the suite is only trustworthy if it passes (and
// is exercised) on the reference implementation it was written for.
func TestRunRealtimeDogfood(t *testing.T) {
	h := realtime.NewHub(realtime.HubOptions{})
	rpc := realtime.NewRPC()
	echo := func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		return body, nil
	}
	rpc.Register("conformance.RPC/Echo", echo)
	rpc.RegisterIdempotent("conformance.RPC/Get", echo)

	schemas := realtime.NewSchemas("conformance-svc", "/api/rt")
	if err := schemas.AddEvent(rtKind, "conformance/*",
		json.RawMessage(`{"type":"object","properties":{"tick":{"type":"number"}},"required":["tick"]}`)); err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	o := realtime.Options{Heartbeat: 200 * time.Millisecond, Retry: 500 * time.Millisecond}
	if err := realtime.Register(mux, "/api/rt", h, o, rpc, schemas); err != nil {
		t.Fatal(err)
	}
	// A real service serves behind reqid.Middleware; the suite's problem
	// checks (instance == X-Request-ID) assume that contract.
	srv := httptest.NewServer(reqid.Middleware(mux))
	t.Cleanup(srv.Close)

	RunRealtime(t, Target{BaseURL: srv.URL}, Realtime{
		StreamPath:        "/api/rt/stream",
		WSPath:            "/api/rt/ws",
		SchemaPath:        "/api/rt/schema",
		RPCPath:           "/api/rt/rpc/conformance.RPC/Echo",
		RPCIdempotentPath: "/api/rt/rpc/conformance.RPC/Get",
		Topics:            []string{"conformance/*"},
		Topic:             "conformance/run",
		Publish: func(topic, kind string, data any) error {
			b, err := json.Marshal(data)
			if err != nil {
				return err
			}
			_, err = h.Publish(realtime.Event{Topic: topic, Kind: kind, Data: b})
			return err
		},
		Heartbeat: o.Heartbeat,
	})
}

// TestRunRealtimeSkips verifies the skip semantics a service needs: empty
// paths drop whole groups, and a Header suppresses the unauthenticated
// topics check.
func TestRunRealtimeSkips(t *testing.T) {
	h := realtime.NewHub(realtime.HubOptions{})
	mux := http.NewServeMux()
	if err := realtime.Register(mux, "/rt", h, realtime.Options{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	// A real service serves behind reqid.Middleware; the suite's problem
	// checks (instance == X-Request-ID) assume that contract.
	srv := httptest.NewServer(reqid.Middleware(mux))
	t.Cleanup(srv.Close)

	RunRealtime(t, Target{BaseURL: srv.URL}, Realtime{
		StreamPath: "/rt/stream",
		Topics:     []string{"conf/*"},
		Topic:      "conf/run",
		Header:     http.Header{"Authorization": []string{"Bearer x"}}, // skips missing-topics
		Publish: func(topic, kind string, data any) error {
			b, _ := json.Marshal(data)
			_, err := h.Publish(realtime.Event{Topic: topic, Kind: kind, Data: b})
			return err
		},
	})
	// Only the two SSE checks (envelope, resume) should have run; the
	// skipped groups leave nothing to assert beyond the run succeeding.
}
