package health_test

import (
	"context"
	"fmt"
	"net/http/httptest"

	"github.com/tcpsystems/plinth/health"
)

func ExampleLiveness() {
	w := httptest.NewRecorder()
	health.Liveness(w, nil)
	fmt.Println(w.Code)
	fmt.Println(w.Header().Get("Content-Type"))

	// Output: 200
	// application/json
}

func ExampleNewRegistry() {
	reg := health.NewRegistry()
	reg.Add("db", func(context.Context) error { return nil })
	reg.Add("cache", func(context.Context) error { return nil })
	h := reg.Ready()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))
	fmt.Println(w.Code)

	// Output: 200
}
