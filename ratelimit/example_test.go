package ratelimit_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/tcpsystems/plinth/ratelimit"
)

func ExampleLimiter_Middleware() {
	limiter := &ratelimit.Limiter{
		Limit:  5,
		Window: time.Minute,
		Key: func(r *http.Request) string {
			return "client-1"
		},
	}

	h := limiter.Middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, "ok")
	}))

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	fmt.Println(w.Code)
	fmt.Println(w.Header().Get("Ratelimit"))

	// Output: 200
	// limit=5, remaining=4, reset=60
}

func ExampleSetHeaders() {
	h := http.Header{}
	ratelimit.SetHeaders(h, ratelimit.State{
		Limit:     100,
		Remaining: 99,
		Reset:     time.Minute,
		Window:    5 * time.Minute,
	})
	fmt.Println(h.Get("Ratelimit"))
	fmt.Println(h.Get("Ratelimit-Policy"))

	// Output: limit=100, remaining=99, reset=60
	// 100;w=300
}
