package logx_test

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"

	"github.com/tcpsystems/plinth/logx"
)

func ExampleMiddleware() {
	h := logx.Middleware(slog.Default(), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/api/resource?secret=abc", nil)
	h.ServeHTTP(w, r)
	fmt.Println(w.Code)

	// Output: 200
}
