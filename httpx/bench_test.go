package httpx

import (
	"net/http/httptest"
	"strings"
	"testing"
)

type benchItem struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Done  bool   `json:"done"`
}

var benchPayload = map[string]any{
	"items": []benchItem{
		{ID: "018f0000-0000-7000-8000-000000000001", Title: "first card", Done: false},
		{ID: "018f0000-0000-7000-8000-000000000002", Title: "second card", Done: true},
	},
	"next_page_token": "eyJpZCI6IjAxOGYifQ.c2lnbmF0dXJl",
}

const benchBody = `{"id":"018f0000-0000-7000-8000-000000000001","title":"first card","done":false}`

// BenchmarkWriteJSON measures encoding a page envelope and writing it to a
// recorder, the path every list handler takes.
func BenchmarkWriteJSON(b *testing.B) {
	for b.Loop() {
		WriteJSON(httptest.NewRecorder(), 200, benchPayload)
	}
}

// BenchmarkDecodeJSON measures a size-capped decode of a 77-byte object, the
// path every JSON handler takes on a request body.
func BenchmarkDecodeJSON(b *testing.B) {
	var dst benchItem
	for b.Loop() {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest("POST", "/", strings.NewReader(benchBody))
		if err := DecodeJSON(rec, r, &dst, 1<<20, false); err != nil {
			b.Fatal(err)
		}
	}
}
