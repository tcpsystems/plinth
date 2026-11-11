package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLimiter(t *testing.T) {
	now := time.Unix(1000, 0)
	l := &Limiter{Limit: 2, Window: time.Minute, Key: func(*http.Request) string { return "a" },
		Exempt: func(r *http.Request) bool { return r.Header.Get("X-Internal") == "1" },
		Now:    func() time.Time { return now }}
	h := l.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	do := func(internal bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/v1/x", nil)
		if internal {
			r.Header.Set("X-Internal", "1")
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	w := do(false)
	if w.Code != 200 || w.Header().Get("RateLimit") != "limit=2, remaining=1, reset=60" || w.Header().Get("RateLimit-Policy") != "2;w=60" {
		t.Fatal(w.Code, w.Header())
	}
	do(false)
	now = now.Add(10 * time.Second)
	w = do(false)
	if w.Code != 429 || w.Header().Get("Retry-After") != "50" || w.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatal(w.Code, w.Header(), w.Body.String())
	}
	for i := 0; i < 10; i++ {
		if do(true).Code != 200 {
			t.Fatal("internal key limited")
		}
	}
	now = now.Add(time.Minute)
	if do(false).Code != 200 {
		t.Fatal("window did not reset")
	}
}
