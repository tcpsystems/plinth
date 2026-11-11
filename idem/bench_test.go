package idem

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func benchHandler() http.Handler {
	h := Middleware(Options{Store: NewMemoryStore()})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	return h
}

func benchRequest(body string) *http.Request {
	r := httptest.NewRequest("POST", "/v1/items", strings.NewReader(body))
	r.Header.Set(Header, "bench-key")
	return r
}

// BenchmarkMiddlewareMiss measures a keyed request that has no stored record:
// hash the body, run the handler, store the response.
func BenchmarkMiddlewareMiss(b *testing.B) {
	h := benchHandler()
	body := `{"title":"a"}`
	i := 0
	for b.Loop() {
		i++
		r := benchRequest(body)
		// A fresh key per iteration so every request is a miss.
		r.Header.Set(Header, "miss-"+strconv.Itoa(i))
		h.ServeHTTP(httptest.NewRecorder(), r)
	}
}

// BenchmarkMiddlewareReplay measures a duplicate request that replays the
// stored response.
func BenchmarkMiddlewareReplay(b *testing.B) {
	h := benchHandler()
	h.ServeHTTP(httptest.NewRecorder(), benchRequest(`{"title":"a"}`))
	for b.Loop() {
		h.ServeHTTP(httptest.NewRecorder(), benchRequest(`{"title":"a"}`))
	}
}
