package mw

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func ok(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }

func TestRecover(t *testing.T) {
	var buf bytes.Buffer
	h := Recover(slog.New(slog.NewTextHandler(&buf, nil)))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("secret sql: select *")
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/x", nil))
	if rec.Code != 500 || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(buf.String(), "secret sql") {
		t.Fatal("panic not logged")
	}
}

func TestChainOrder(t *testing.T) {
	var order []string
	tag := func(s string) Middleware {
		return func(n http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { order = append(order, s); n.ServeHTTP(w, r) })
		}
	}
	Chain(http.HandlerFunc(ok), tag("a"), tag("b")).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	if strings.Join(order, "") != "ab" {
		t.Fatal(order)
	}
}

func TestSecurityHeaders(t *testing.T) {
	rec := httptest.NewRecorder()
	SecurityHeaders()(http.HandlerFunc(ok)).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatal(rec.Header())
	}
}

func TestCSRF(t *testing.T) {
	h := CSRF("https://admin.example.com")(http.HandlerFunc(ok))
	r := httptest.NewRequest("POST", "https://api.tcp.systems/x", nil)
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != 403 {
		t.Fatalf("cross-site POST got %d", rec.Code)
	}
	r = httptest.NewRequest("POST", "https://api.tcp.systems/x", nil)
	r.Header.Set("Origin", "https://admin.example.com")
	r.Header.Set("Sec-Fetch-Site", "cross-site")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != 204 {
		t.Fatalf("trusted origin got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "https://api.tcp.systems/x", nil))
	if rec.Code != 204 {
		t.Fatalf("GET got %d", rec.Code)
	}
}

func TestCORS(t *testing.T) {
	h := CORS([]string{"https://a.example"}, []string{"GET", "POST"}, []string{"Authorization"})(http.HandlerFunc(ok))
	r := httptest.NewRequest("OPTIONS", "/", nil)
	r.Header.Set("Origin", "https://a.example")
	r.Header.Set("Access-Control-Request-Method", "POST")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Code != 204 || rec.Header().Get("Access-Control-Allow-Origin") != "https://a.example" || rec.Header().Get("Access-Control-Allow-Methods") != "GET, POST" {
		t.Fatalf("%d %v", rec.Code, rec.Header())
	}
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Origin", "https://evil.example")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("evil origin allowed")
	}
}

func TestTimeout(t *testing.T) {
	h := Timeout(10 * time.Millisecond)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 503 {
		t.Fatal(rec.Code)
	}
}
