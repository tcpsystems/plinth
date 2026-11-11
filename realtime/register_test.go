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
)

func TestRegisterAllSurfaces(t *testing.T) {
	h := NewHub(HubOptions{})
	schemas := NewSchemas("test-svc", "/api/rt")
	if err := schemas.AddEvent("run.started", "acct/1", json.RawMessage(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	rpc := echoRPC()

	mux := http.NewServeMux()
	if err := Register(mux, "/api/rt", h, Options{}, rpc, schemas); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Subscribe both transports before publishing so each sees the event
	// live (a late dial at cursor 0 would instead replay it).
	s := openStream(t, srv.URL+"/api/rt/stream?topics=acct/1", nil)
	conn := dialWS(t, strings.Replace(srv.URL, "http", "ws", 1)+"/api/rt/ws?topics=acct/1", nil)
	e := pub(t, h, "acct/1", "run.started")

	s.until(t, "id: "+strconv.FormatUint(e.Seq, 10)+"\nevent: run.started\ndata: ")
	if f := readFrame(t, conn); f.Type != "event" || f.Seq != e.Seq {
		t.Fatalf("ws frame %+v, want event seq %d", f, e.Seq)
	}

	// RPC mounted under the spec wildcard.
	resp, err := http.Post(srv.URL+"/api/rt/rpc/echo.Echo/Echo", "application/json", strings.NewReader(`{"n":2}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || string(b) != `{"n":2}` {
		t.Fatalf("rpc: status %d body %s", resp.StatusCode, b)
	}
	// GET reaches the handler too (method-not-found is the handler's 501,
	// not the mux's 404/405).
	resp, err = http.Get(srv.URL + "/api/rt/rpc/echo.Echo/Nope?connect=v1&encoding=json&message=%7B%7D")
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotImplemented || !strings.Contains(string(b), `"unimplemented"`) {
		t.Fatalf("rpc GET unknown: status %d body %s", resp.StatusCode, b)
	}

	// Schema index mounted and carrying the RPC method list.
	resp, err = http.Get(srv.URL + "/api/rt/schema")
	if err != nil {
		t.Fatal(err)
	}
	b, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("schema: status %d body %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("schema content-type %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("schema cache-control %q, want no-store", cc)
	}
	var idx SchemaIndex
	if err := json.Unmarshal(b, &idx); err != nil {
		t.Fatalf("schema body %s: %v", b, err)
	}
	if idx.Service != "test-svc" || idx.RealtimePrefix != "/api/rt" {
		t.Fatalf("index %+v", idx)
	}
	if len(idx.Events) != 1 || idx.Events[0].Kind != "run.started" || idx.Events[0].TopicPattern != "acct/1" {
		t.Fatalf("events %+v", idx.Events)
	}
	if idx.Connect == nil || len(idx.Connect.RPCMethods) != 1 || idx.Connect.RPCMethods[0] != "echo.Echo/Echo" {
		t.Fatalf("connect %+v", idx.Connect)
	}
}

func TestRegisterOptionalSurfaces(t *testing.T) {
	h := NewHub(HubOptions{})
	mux := http.NewServeMux()
	if err := Register(mux, "/rt", h, Options{}, nil, nil); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	// Transports are up.
	s := openStream(t, srv.URL+"/rt/stream?topics=x", nil)
	pub(t, h, "x", "y.z")
	s.until(t, "event: y.z")
	// Absent surfaces 404 instead of half-mounting.
	if code, _ := getProblem(t, srv.URL+"/rt/schema", nil); code != http.StatusNotFound {
		t.Fatalf("schema without Schemas: status %d, want 404", code)
	}
	resp, err := http.Post(srv.URL+"/rt/rpc/echo.Echo/Echo", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("rpc without RPC: status %d, want 404", resp.StatusCode)
	}
}

func TestRegisterValidation(t *testing.T) {
	h := NewHub(HubOptions{})
	for _, tc := range []struct {
		name, prefix string
		hub          *Hub
		wantErr      bool
	}{
		{"nil hub", "/rt", nil, true},
		{"empty", "", h, true},
		{"no leading slash", "rt", h, true},
		{"trailing slash", "/rt/", h, true},
		{"wildcard", "/rt/*", h, true},
		{"brace", "/rt/{x}", h, true},
		{"ok", "/rt", h, false},
	} {
		err := Register(http.NewServeMux(), tc.prefix, tc.hub, Options{}, nil, nil)
		if (err != nil) != tc.wantErr {
			t.Errorf("%s: err %v, wantErr %v", tc.name, err, tc.wantErr)
		}
	}
}

func TestRPCMethodNamesSorted(t *testing.T) {
	r := NewRPC()
	fn := func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		return nil, nil
	}
	r.Register("z.Svc/A", fn)
	r.Register("a.Svc/Z", fn)
	r.Register("a.Svc/A", fn)
	want := []string{"a.Svc/A", "a.Svc/Z", "z.Svc/A"}
	got := r.MethodNames()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("MethodNames %v, want %v", got, want)
	}
	if len(NewRPC().MethodNames()) != 0 {
		t.Fatal("empty registry should list no methods")
	}
}
