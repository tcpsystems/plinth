package realtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// rpcServer mounts the RPC handler under both POST and GET (the same shape
// [Register] uses).
func rpcServer(t *testing.T, r *RPC, o Options) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	hh := r.Handler(o)
	mux.Handle("POST /rpc/{spec...}", hh)
	mux.Handle("GET /rpc/{spec...}", hh)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// echoRPC registers "echo.Echo/Echo", returning the raw request body.
func echoRPC() *RPC {
	r := NewRPC()
	r.Register("echo.Echo/Echo", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		return body, nil
	})
	return r
}

// postRPC calls a method over POST application/json and returns the response.
func postRPC(t *testing.T, srv *httptest.Server, spec, body string, hdrs map[string]string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest("POST", srv.URL+"/rpc/"+spec, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdrs {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

// wantWireErr asserts the response is a Connect wire error with this code
// (message, if non-empty, must appear in the message) and status.
func wantWireErr(t *testing.T, resp *http.Response, body []byte, code string, status int, wantMsg string) {
	t.Helper()
	if resp.StatusCode != status {
		t.Fatalf("status %d, want %d (body %s)", resp.StatusCode, status, body)
	}
	var e errorBody
	if err := json.Unmarshal(body, &e); err != nil {
		t.Fatalf("error body %s: %v", body, err)
	}
	if e.Code != code {
		t.Fatalf("code %q, want %q", e.Code, code)
	}
	if wantMsg != "" && !strings.Contains(e.Message, wantMsg) {
		t.Fatalf("message %q, want it to contain %q", e.Message, wantMsg)
	}
}

func TestRPCPostEcho(t *testing.T) {
	srv := rpcServer(t, echoRPC(), Options{})
	resp, b := postRPC(t, srv, "echo.Echo/Echo", `{"a":1}`, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s, want 200", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type %q", ct)
	}
	if string(b) != `{"a":1}` {
		t.Fatalf("body %s, want echo", b)
	}
}

func TestRPCPrincipalAndEmptyBody(t *testing.T) {
	r := NewRPC()
	var got any
	r.Register("auth.Svc/Who", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		got = principal
		return body, nil
	})
	srv := rpcServer(t, r, Options{Auth: func(req *http.Request) (any, error) {
		if req.Header.Get("Authorization") != "Bearer tok" {
			return nil, http.ErrNoCookie
		}
		return "user-1", nil
	}})
	// Empty body arrives as the JSON empty message.
	resp, b := postRPC(t, srv, "auth.Svc/Who", "", map[string]string{"Authorization": "Bearer tok"})
	if resp.StatusCode != http.StatusOK || string(b) != "{}" {
		t.Fatalf("status %d body %s, want 200 {}", resp.StatusCode, b)
	}
	if got != "user-1" {
		t.Fatalf("principal %v, want user-1", got)
	}
	// Auth failure is unauthenticated on the Connect wire, whatever the
	// inner error was.
	resp, b = postRPC(t, srv, "auth.Svc/Who", "{}", nil)
	wantWireErr(t, resp, b, CodeUnauthenticated, http.StatusUnauthorized, "")
	// Unknown methods fail before auth, like connect-go's routing.
	resp, b = postRPC(t, srv, "auth.Svc/Nope", "{}", nil)
	wantWireErr(t, resp, b, CodeUnimplemented, http.StatusNotImplemented, "not found")
}

// TestRPCCodeTable pins every Connect code to the HTTP status of the
// authoritative table (connect-go v1.21.1 protocol_connect.go).
func TestRPCCodeTable(t *testing.T) {
	codes := map[string]int{
		CodeCanceled: 499, CodeUnknown: 500, CodeInvalidArgument: 400,
		CodeDeadlineExceeded: 504, CodeNotFound: 404, CodeAlreadyExists: 409,
		CodePermissionDenied: 403, CodeResourceExhausted: 429,
		CodeFailedPrecondition: 400, CodeAborted: 409, CodeOutOfRange: 400,
		CodeUnimplemented: 501, CodeInternal: 500, CodeUnavailable: 503,
		CodeDataLoss: 500, CodeUnauthenticated: 401,
	}
	for code, status := range codes {
		t.Run(code, func(t *testing.T) {
			r := NewRPC()
			c := code
			r.Register("err.Svc/Fail", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
				return nil, NewConnectError(c, "boom")
			})
			srv := rpcServer(t, r, Options{})
			resp, b := postRPC(t, srv, "err.Svc/Fail", "{}", nil)
			wantWireErr(t, resp, b, code, status, "boom")
		})
	}
}

func TestRPCErrorWireShape(t *testing.T) {
	r := NewRPC()
	r.Register("err.Svc/Details", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		// A wrapped ConnectError still surfaces its own code.
		return nil, fmt.Errorf("while opening: %w",
			NewConnectError(CodeNotFound, "no such run").WithDetails(ConnectDetail{
				Type:  "google.rpc.ErrorInfo",
				Value: []byte("x"),
				Debug: map[string]string{"reason": "RUN_GONE"},
			}))
	})
	srv := rpcServer(t, r, Options{})
	resp, b := postRPC(t, srv, "err.Svc/Details", "{}", nil)
	wantWireErr(t, resp, b, CodeNotFound, http.StatusNotFound, "no such run")
	// Exact wire shape: value is base64, debug passes through, and an
	// error without message/details carries only the code.
	var e struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Details []struct {
			Type  string            `json:"type"`
			Value string            `json:"value"`
			Debug map[string]string `json:"debug"`
		} `json:"details"`
	}
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatal(err)
	}
	if len(e.Details) != 1 || e.Details[0].Type != "google.rpc.ErrorInfo" {
		t.Fatalf("details %+v", e.Details)
	}
	if e.Details[0].Value != "eA==" {
		t.Fatalf("detail value %q, want base64 eA==", e.Details[0].Value)
	}
	if got, _ := base64.StdEncoding.DecodeString(e.Details[0].Value); string(got) != "x" {
		t.Fatalf("detail value decodes to %q", got)
	}
	if e.Details[0].Debug["reason"] != "RUN_GONE" {
		t.Fatalf("debug %+v", e.Details[0].Debug)
	}
}

func TestRPCUnknownErrorBecomesInternal(t *testing.T) {
	r := NewRPC()
	r.Register("err.Svc/Raw", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		return nil, fmt.Errorf("sql: close 1006 unexpected") // must not leak
	})
	srv := rpcServer(t, r, Options{})
	resp, b := postRPC(t, srv, "err.Svc/Raw", "{}", nil)
	wantWireErr(t, resp, b, CodeInternal, http.StatusInternalServerError, "internal server error")
	if strings.Contains(string(b), "sql:") {
		t.Fatalf("internal error text leaked: %s", b)
	}
}

func TestRPCDeadlineFromHeader(t *testing.T) {
	r := NewRPC()
	r.Register("slow.Svc/Wait", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		<-ctx.Done()
		return nil, ctx.Err() // writeConnectErr maps this to deadline_exceeded
	})
	srv := rpcServer(t, r, Options{})
	resp, b := postRPC(t, srv, "slow.Svc/Wait", "{}", map[string]string{"Connect-Timeout-Ms": "1"})
	wantWireErr(t, resp, b, CodeDeadlineExceeded, http.StatusGatewayTimeout, "")
}

func TestRPCTimeoutHeaderValidation(t *testing.T) {
	srv := rpcServer(t, echoRPC(), Options{})
	for _, bad := range []string{"0", "-5", "abc", "1.5", "", "12345678901"} {
		if bad == "" { // absent is legal (infinite, subject to clamp)
			continue
		}
		resp, b := postRPC(t, srv, "echo.Echo/Echo", "{}", map[string]string{"Connect-Timeout-Ms": bad})
		wantWireErr(t, resp, b, CodeInvalidArgument, http.StatusBadRequest, "Connect-Timeout-Ms")
	}
	// A valid header reaches the handler as a deadline clamped to
	// Options.MaxRPCTimeout: 100ms asks for 1s, so the deadline must sit
	// inside roughly the clamp window, not the client's 1s.
	var sawDeadline time.Duration
	r := NewRPC()
	r.Register("dl.Svc/Check", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Error("no deadline on ctx")
		}
		sawDeadline = time.Until(dl)
		return body, nil
	})
	srv2 := rpcServer(t, r, Options{MaxRPCTimeout: 100 * time.Millisecond})
	resp, b := postRPC(t, srv2, "dl.Svc/Check", "{}", map[string]string{"Connect-Timeout-Ms": "1000"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s", resp.StatusCode, b)
	}
	if sawDeadline <= 0 || sawDeadline > 150*time.Millisecond {
		t.Fatalf("clamped deadline %.3v after now, want ~100ms", sawDeadline)
	}
}

func TestRPCProtocolVersion(t *testing.T) {
	srv := rpcServer(t, echoRPC(), Options{})
	// Absent or "1" is accepted; anything else is the client's error.
	for _, v := range []string{"", "1"} {
		h := map[string]string{}
		if v != "" {
			h["Connect-Protocol-Version"] = v
		}
		if resp, b := postRPC(t, srv, "echo.Echo/Echo", "{}", h); resp.StatusCode != http.StatusOK {
			t.Fatalf("version %q: status %d body %s", v, resp.StatusCode, b)
		}
	}
	resp, b := postRPC(t, srv, "echo.Echo/Echo", "{}", map[string]string{"Connect-Protocol-Version": "2"})
	wantWireErr(t, resp, b, CodeInvalidArgument, http.StatusBadRequest, "Connect-Protocol-Version")
}

func TestRPCContentType(t *testing.T) {
	srv := rpcServer(t, echoRPC(), Options{})
	// The standard profile speaks application/json only; a codec we cannot
	// parse is unimplemented at HTTP 415 (the spec's required status).
	for _, ct := range []string{"application/proto", "text/plain", ""} {
		req, err := http.NewRequest("POST", srv.URL+"/rpc/echo.Echo/Echo", strings.NewReader("{}"))
		if err != nil {
			t.Fatal(err)
		}
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		wantWireErr(t, resp, b, CodeUnimplemented, http.StatusUnsupportedMediaType, "content-type")
	}
	// A charset parameter is still application/json.
	resp, b := postRPC(t, srv, "echo.Echo/Echo", "{}", map[string]string{"Content-Type": "application/json; charset=utf-8"})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s, want 200", resp.StatusCode, b)
	}
}

func TestRPCOversizeBody(t *testing.T) {
	srv := rpcServer(t, echoRPC(), Options{})
	body := `{"pad":"` + strings.Repeat("x", rpcMaxBodyBytes+1) + `"}`
	resp, b := postRPC(t, srv, "echo.Echo/Echo", body, nil)
	wantWireErr(t, resp, b, CodeInvalidArgument, http.StatusBadRequest, "too large")
}

func TestRPCInvalidResponse(t *testing.T) {
	r := NewRPC()
	r.Register("bad.Svc/Noise", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		return json.RawMessage("not json at all"), nil
	})
	srv := rpcServer(t, r, Options{})
	resp, b := postRPC(t, srv, "bad.Svc/Noise", "{}", nil)
	wantWireErr(t, resp, b, CodeInternal, http.StatusInternalServerError, "")
}

func TestRPCUnsupportedHTTPMethod(t *testing.T) {
	// Mounted without a method restriction so PUT reaches the handler's
	// own switch (through Register the mux would answer 405 first).
	mux := http.NewServeMux()
	mux.Handle("/rpc/{spec...}", echoRPC().Handler(Options{}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	resp, err := http.DefaultClient.Do(req(t, "PUT", srv.URL+"/rpc/echo.Echo/Echo", "{}"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	wantWireErr(t, resp, b, CodeInvalidArgument, http.StatusBadRequest, "unsupported method")
}

func req(t *testing.T, method, url, body string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Content-Type", "application/json")
	return r
}

// getRPC calls the unary GET form; query holds everything after "?" of the
// Connect GET request.
func getRPC(t *testing.T, srv *httptest.Server, spec, query string) (*http.Response, []byte) {
	t.Helper()
	resp, err := http.Get(srv.URL + "/rpc/" + spec + "?" + query)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func TestRPCGetIdempotent(t *testing.T) {
	r := NewRPC()
	r.RegisterIdempotent("query.Svc/Get", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		return body, nil
	})
	r.Register("query.Svc/Mutate", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		t.Error("mutating handler ran under GET")
		return nil, nil
	})
	srv := rpcServer(t, r, Options{})

	// A non-idempotent method must refuse the GET form.
	resp, b := getRPC(t, srv, "query.Svc/Mutate", "connect=v1&encoding=json&message=%7B%7D")
	wantWireErr(t, resp, b, CodeInvalidArgument, http.StatusBadRequest, "idempotent")

	// Plain percent-encoded JSON message.
	resp, b = getRPC(t, srv, "query.Svc/Get", "connect=v1&encoding=json&message=%7B%22a%22%3A1%7D")
	if resp.StatusCode != http.StatusOK || string(b) != `{"a":1}` {
		t.Fatalf("status %d body %s", resp.StatusCode, b)
	}

	// Base64url (unpadded, per RFC 4648 §5) message.
	msg := base64.RawURLEncoding.EncodeToString([]byte(`{"b":2}`))
	resp, b = getRPC(t, srv, "query.Svc/Get", "connect=v1&encoding=json&base64=1&message="+msg)
	if resp.StatusCode != http.StatusOK || string(b) != `{"b":2}` {
		t.Fatalf("base64 form: status %d body %s", resp.StatusCode, b)
	}
}

func TestRPCGetParameterErrors(t *testing.T) {
	r := NewRPC()
	r.RegisterIdempotent("query.Svc/Get", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
		return body, nil
	})
	srv := rpcServer(t, r, Options{})
	for _, tc := range []struct {
		name, query, code string
		status            int
	}{
		{"no connect param", `encoding=json&message=%7B%7D`, CodeInvalidArgument, http.StatusBadRequest},
		{"wrong connect", `connect=v2&encoding=json&message=%7B%7D`, CodeInvalidArgument, http.StatusBadRequest},
		{"missing encoding", `connect=v1&message=%7B%7D`, CodeUnimplemented, http.StatusUnsupportedMediaType},
		{"proto encoding", `connect=v1&encoding=proto&message=AAA`, CodeUnimplemented, http.StatusUnsupportedMediaType},
		{"gzip compression", `connect=v1&encoding=json&compression=gzip&message=AAA`, CodeUnimplemented, http.StatusNotImplemented},
		{"no message", `connect=v1&encoding=json`, CodeInvalidArgument, http.StatusBadRequest},
		{"bad base64", `connect=v1&encoding=json&base64=1&message=@@@`, CodeInvalidArgument, http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, b := getRPC(t, srv, "query.Svc/Get", tc.query)
			wantWireErr(t, resp, b, tc.code, tc.status, "")
		})
	}
	// Unknown query parameters are tolerated (spec: servers must accept
	// them alongside the Connect parameters).
	resp, b := getRPC(t, srv, "query.Svc/Get", "connect=v1&encoding=json&message=%7B%7D&x=1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d body %s, want 200", resp.StatusCode, b)
	}
}

func TestRPCRegistrationPanics(t *testing.T) {
	for _, tc := range []struct {
		name string
		f    func()
	}{
		{"nil handler", func() { NewRPC().Register("p.Svc/M", nil) }},
		{"no slash", func() {
			NewRPC().Register("p.Svc.M", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
				return nil, nil
			})
		}},
		{"empty service", func() {
			NewRPC().Register("/M", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
				return nil, nil
			})
		}},
		{"empty method", func() {
			NewRPC().Register("p.Svc/", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
				return nil, nil
			})
		}},
		{"double slash", func() {
			NewRPC().Register("p.Svc/a/b", func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
				return nil, nil
			})
		}},
		{"duplicate", func() {
			r := NewRPC()
			fn := func(ctx context.Context, principal any, body json.RawMessage) (json.RawMessage, error) {
				return nil, nil
			}
			r.Register("p.Svc/M", fn)
			r.Register("p.Svc/M", fn)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("no panic")
				}
			}()
			tc.f()
		})
	}
}
