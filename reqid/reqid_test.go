package reqid

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

var uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestPropagation(t *testing.T) {
	seen := map[string]bool{}
	for _, headers := range [][]string{{"trace-123_ok.id"}, nil, {"a", "b"}, {"bad\nID"}, {strings.Repeat("a", 129)}, {""}} {
		var got string
		h := Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = FromContext(r.Context())
			if got != r.Header.Get(Header) {
				t.Fatal("downstream mismatch")
			}
		}))
		r := httptest.NewRequest("GET", "/", nil)
		for _, value := range headers {
			r.Header.Add(Header, value)
		}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if !valid(got) || w.Header().Get(Header) != got {
			t.Fatal(got)
		}
		if len(headers) == 1 && headers[0] == "trace-123_ok.id" {
			if got != headers[0] {
				t.Fatal(got)
			}
		} else if !uuidv7Re.MatchString(got) {
			t.Fatalf("generated ID not UUIDv7: %q", got)
		}
		if seen[got] {
			t.Fatal("duplicate generated ID")
		}
		seen[got] = true
		if FromContext(r.Context()) != "" {
			t.Fatal("mutated input context")
		}
	}
	if FromContext(context.Background()) != "" {
		t.Fatal("unexpected ID")
	}
}

func TestNewUUIDv7Format(t *testing.T) {
	for i := 0; i < 100; i++ {
		id := NewUUIDv7()
		if !uuidv7Re.MatchString(id) {
			t.Fatalf("NewUUIDv7 not UUIDv7: %q", id)
		}
	}
}

func TestNewUUIDv7Uniqueness(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := NewUUIDv7()
		if seen[id] {
			t.Fatalf("duplicate UUIDv7: %q", id)
		}
		seen[id] = true
	}
}
