package page

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tcpsystems/plinth/problem"
)

type cur struct {
	After string `json:"after"`
}

func signer(t *testing.T) *Signer {
	s, err := NewSigner([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestSize(t *testing.T) {
	for q, want := range map[string]int{"": 50, "?page_size=1": 1, "?page_size=200": 200} {
		got, err := Size(httptest.NewRequest("GET", "/"+q, nil))
		if err != nil || got != want {
			t.Errorf("%q: %d %v", q, got, err)
		}
	}
	for _, q := range []string{"?page_size=201", "?page_size=0", "?page_size=-1", "?page_size=x", "?page_size=1e2"} {
		_, err := Size(httptest.NewRequest("GET", "/"+q, nil))
		if p := problem.Wrap(err); p.Status != 400 || p.Code != "invalid_page_size" {
			t.Errorf("%q accepted: %+v", q, p)
		}
	}
}

func TestTokenRoundTripAndTamper(t *testing.T) {
	s := signer(t)
	tok, err := s.Encode(cur{After: "abc"})
	if err != nil {
		t.Fatal(err)
	}
	var c cur
	if err := s.Decode(tok, &c); err != nil || c.After != "abc" {
		t.Fatal(err, c)
	}
	other, _ := NewSigner([]byte("fedcba9876543210"))
	flipped := []byte(tok)
	if flipped[3] == 'A' {
		flipped[3] = 'B'
	} else {
		flipped[3] = 'A'
	}
	for _, bad := range []string{"", "garbage!", string(flipped), tok + "AAAA", strings.Repeat("A", 2000)} {
		if s.Decode(bad, &c) == nil {
			t.Errorf("accepted %q", bad)
		}
	}
	if other.Decode(tok, &c) == nil {
		t.Error("accepted token signed with another key")
	}
}

func TestParse(t *testing.T) {
	s := signer(t)
	var c cur
	size, first, err := Parse(httptest.NewRequest("GET", "/", nil), s, &c)
	if err != nil || size != 50 || !first {
		t.Fatal(size, first, err)
	}
	tok, _ := s.Encode(cur{After: "z"})
	size, first, err = Parse(httptest.NewRequest("GET", "/?page_size=5&page_token="+tok, nil), s, &c)
	if err != nil || size != 5 || first || c.After != "z" {
		t.Fatal(size, first, err, c)
	}
	if _, _, err = Parse(httptest.NewRequest("GET", "/?page_token=bad", nil), s, &c); problem.Wrap(err).Code != "invalid_page_token" {
		t.Fatal(err)
	}
}

func TestTrimAndKeyset(t *testing.T) {
	items, more := Trim([]int{1, 2, 3}, 2)
	if len(items) != 2 || !more || Limit(2) != 3 {
		t.Fatal(items, more)
	}
	if _, more = Trim([]int{1, 2}, 2); more {
		t.Fatal("more")
	}
	if got := Keyset(1, "created_at", "id"); got != "(created_at, id) > ($1, $2)" {
		t.Fatal(got)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("no panic on bad column")
		}
	}()
	Keyset(1, "id; DROP TABLE x")
}

func TestShortKey(t *testing.T) {
	if _, err := NewSigner([]byte("short")); err == nil {
		t.Fatal("accepted")
	}
}

func TestResponseEnvelope(t *testing.T) {
	type item struct{ ID int }
	resp := Response[item]{Items: []item{{ID: 1}, {ID: 2}}, NextPageToken: "abc123"}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got["items"] == nil {
		t.Fatal("items missing")
	}
	items := got["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("want 2 items, got %d", len(items))
	}
	if got["next_page_token"] != "abc123" {
		t.Fatalf("next_page_token mismatch: %v", got["next_page_token"])
	}
}

func TestResponseOmitEmptyToken(t *testing.T) {
	type item struct{ ID int }
	resp := Response[item]{Items: []item{{ID: 1}}}
	b, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if _, ok := got["next_page_token"]; ok {
		t.Fatalf("next_page_token should be omitted, got %v", got["next_page_token"])
	}
}

func TestWrite(t *testing.T) {
	type item struct{ ID int }
	resp := Response[item]{Items: []item{{ID: 1}, {ID: 2}}, NextPageToken: "tok"}
	w := httptest.NewRecorder()
	r := httptest.NewRequest("GET", "/", nil)
	Write(w, r, resp)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Fatalf("content-type %q", ct)
	}
	cc := w.Header().Get("Cache-Control")
	if cc != "no-store" {
		t.Fatalf("cache-control %q", cc)
	}
	var got Response[item]
	if err := json.NewDecoder(w.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 2 || got.NextPageToken != "tok" {
		t.Fatalf("body mismatch: %+v", got)
	}
}
