package buildinfo_test

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"

	"github.com/tcpsystems/plinth/buildinfo"
)

func ExampleHandler() {
	const service = "demo"
	rec := httptest.NewRecorder()
	buildinfo.Handler(service).ServeHTTP(rec, httptest.NewRequest("GET", "/version", nil))

	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	fmt.Println(body["service"])
	fmt.Println(body["commit"] != nil)

	// Output: demo
	// true
}
