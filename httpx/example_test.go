package httpx_test

import (
	"fmt"
	"net/http/httptest"

	"github.com/tcpsystems/plinth/httpx"
)

func ExampleWriteJSON() {
	type greeting struct {
		Message string `json:"message"`
	}

	w := httptest.NewRecorder()
	httpx.WriteJSON(w, 200, greeting{Message: "hello"})
	fmt.Println(w.Code)
	fmt.Println(w.Body.String())

	// Output: 200
	// {"message":"hello"}
}
