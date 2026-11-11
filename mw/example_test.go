package mw_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"

	"github.com/tcpsystems/plinth/mw"
)

func ExampleChain() {
	h := mw.Chain(
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintln(w, "ok")
		}),
		mw.SecurityHeaders(),
	)

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	fmt.Println(w.Code)
	fmt.Println(w.Header().Get("X-Content-Type-Options"))

	// Output: 200
	// nosniff
}
