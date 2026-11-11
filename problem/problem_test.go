package problem

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tcpsystems/plinth/reqid"
)

func TestWrapStatuses(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		code   string
	}{
		{ErrNotFound, 404, "not_found"}, {ErrConflict, 409, "conflict"}, {ErrInvalid, 400, "invalid"},
		{ErrUnauthorized, 401, "unauthorized"}, {ErrForbidden, 403, "forbidden"}, {ErrRateLimited, 429, "rate_limited"},
		{errors.New("pq: syntax error at or near SELECT"), 500, "internal"},
		{fmt.Errorf("select failed: %w", ErrNotFound), 404, "not_found"},
	} {
		p := Wrap(tc.err)
		if p.Status != tc.status || p.Code != tc.code || p.Type != TypeBase+tc.code {
			t.Errorf("%v: %+v", tc.err, p)
		}
		if p.Detail != "" {
			t.Errorf("wrapped text leaked: %q", p.Detail)
		}
	}
}

func TestCustomCodeAndDetail(t *testing.T) {
	p := Wrap(New(ErrInvalid, "invalid_page_size", "page_size too large"))
	if p.Code != "invalid_page_size" || p.Type != TypeBase+"invalid_page_size" || p.Detail != "page_size too large" || p.Status != 400 {
		t.Fatalf("%+v", p)
	}
}

func TestWriteFields(t *testing.T) {
	h := reqid.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, fmt.Errorf("SELECT * FROM t: %w", ErrNotFound))
	}))
	r := httptest.NewRequest("GET", "/x", nil)
	r.Header.Set("X-Request-ID", "req-1")
	r.Header.Set("Traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 404 || w.Header().Get("Content-Type") != ContentType {
		t.Fatal(w.Code, w.Header())
	}
	var p Problem
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if p.Instance != "req-1" || p.TraceID != "0af7651916cd43dd8448eb211c80319c" || p.Code != "not_found" {
		t.Fatalf("%+v", p)
	}
	if strings.Contains(w.Body.String(), "SELECT") {
		t.Fatal("SQL leaked")
	}
}

func TestTraceIDRejectsBad(t *testing.T) {
	for _, v := range []string{"", "garbage", "00-00000000000000000000000000000000-b7ad6b7169203331-01", "00-0AF7651916CD43DD8448EB211C80319C-b7ad6b7169203331-01", "00-short-b7ad6b7169203331-01"} {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Traceparent", v)
		if TraceID(r) != "" {
			t.Errorf("accepted %q", v)
		}
	}
}

func TestNotFoundHandler(t *testing.T) {
	w := httptest.NewRecorder()
	NotFoundHandler().ServeHTTP(w, httptest.NewRequest("GET", "/nope", nil))
	if w.Code != 404 || w.Header().Get("Content-Type") != ContentType {
		t.Fatal(w.Code)
	}
}

func TestStatusOverride(t *testing.T) {
	p := Wrap(NewStatus(422, ErrInvalid, "x_reuse", "d"))
	if p.Status != 422 || p.Code != "x_reuse" {
		t.Fatal(p)
	}
}
