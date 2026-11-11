package page

import (
	"net/http/httptest"
	"testing"
)

type benchCursor struct {
	CreatedAt string `json:"created_at"`
	ID        string `json:"id"`
}

var benchKey = []byte("0123456789abcdef0123456789abcdef")

// BenchmarkEncodeDecode measures signing and verifying a keyset page token,
// done once per page request that supplies a token.
func BenchmarkEncodeDecode(b *testing.B) {
	s, err := NewSigner(benchKey)
	if err != nil {
		b.Fatal(err)
	}
	cur := benchCursor{CreatedAt: "2026-11-10T00:00:00Z", ID: "018f0000-0000-7000-8000-000000000001"}
	tok, err := s.Encode(cur)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for b.Loop() {
		var back benchCursor
		if err := s.Decode(tok, &back); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkParse measures reading page_size plus a signed token from a
// request, the first thing every list handler does.
func BenchmarkParse(b *testing.B) {
	s, err := NewSigner(benchKey)
	if err != nil {
		b.Fatal(err)
	}
	tok, err := s.Encode(benchCursor{CreatedAt: "2026-11-10T00:00:00Z", ID: "018f"})
	if err != nil {
		b.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/v1/items?page_size=50&page_token="+tok, nil)
	b.ResetTimer()
	for b.Loop() {
		var cur benchCursor
		if _, _, err := Parse(r, s, &cur); err != nil {
			b.Fatal(err)
		}
	}
}
