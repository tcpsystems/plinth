package conformance_test

import (
	"fmt"
	"net/http"

	"github.com/tcpsystems/plinth/conformance"
)

func ExampleTarget() {
	// Target describes a running service for conformance.Run to check.
	// It requires a BaseURL, an *http.Client, and optionally Features.
	t := conformance.Target{
		BaseURL: "http://localhost:8080",
		Client:  http.DefaultClient,
		Features: conformance.Features{
			PaginationPath:      "/items",
			IdempotentPath:      "/items",
			IdempotencyRequired: true,
		},
	}
	fmt.Println(t.BaseURL)
	fmt.Println(t.Features.IdempotencyRequired)

	// Output: http://localhost:8080
	// true
}
