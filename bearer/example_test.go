package bearer_test

import (
	"fmt"
	"net/http/httptest"

	"github.com/tcpsystems/plinth/bearer"
)

func ExampleToken() {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer my-token-abc")
	tok, ok := bearer.Token(r)
	fmt.Printf("ok=%v token=%q\n", ok, tok)

	r2 := httptest.NewRequest("GET", "/", nil)
	_, ok2 := bearer.Token(r2)
	fmt.Printf("ok=%v\n", ok2)

	// Output: ok=true token="my-token-abc"
	// ok=false
}
