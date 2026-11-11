package problem_test

import (
	"fmt"
	"net/http/httptest"

	"github.com/tcpsystems/plinth/problem"
)

func ExampleWriteError() {
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/items/999", nil)

	p := problem.Wrap(problem.ErrNotFound)
	fmt.Println(p.Status)
	problem.Write(w, r, p)

	fmt.Println(w.Code)
	fmt.Println(w.Header().Get("Content-Type"))

	// Output: 404
	// 404
	// application/problem+json
}

func ExampleNew() {
	p := problem.New(problem.ErrInvalid, "bad_page", "page_size must be positive")
	w := problem.Wrap(p)
	fmt.Println(w.Status)
	fmt.Println(w.Detail)

	// Output: 400
	// page_size must be positive
}
