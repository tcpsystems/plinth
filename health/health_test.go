package health

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProbes(t *testing.T) {
	for _, h := range []http.Handler{http.HandlerFunc(Liveness), Readiness(), Readiness(func(context.Context) error { return nil })} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
		if w.Code != 200 || w.Body.String() != "{\"status\":\"ok\"}\n" || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	h := Readiness(func(ctx context.Context) error { called = true; return ctx.Err() }, func(context.Context) error { t.Fatal("ran after failure"); return nil })
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil).WithContext(ctx))
	if !called || w.Code != 503 {
		t.Fatal(w)
	}
	w = httptest.NewRecorder()
	Readiness(func(context.Context) error { return errors.New("private DSN") }).ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	if strings.Contains(w.Body.String(), "private") {
		t.Fatal("dependency error exposed")
	}
}
