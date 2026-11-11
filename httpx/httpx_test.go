package httpx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tcpsystems/plinth/problem"
)

func TestWriteJSON(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSON(rec, http.StatusCreated, map[string]int{"a": 1})
	if rec.Code != 201 || rec.Header().Get("Content-Type") != "application/json" || rec.Body.String() != "{\"a\":1}\n" {
		t.Fatalf("%d %v %q", rec.Code, rec.Header(), rec.Body.String())
	}
}

func TestWriteJSONEncodeFailure(t *testing.T) {
	rec := httptest.NewRecorder()
	WriteJSON(rec, 200, make(chan int))
	if rec.Code != 500 {
		t.Fatalf("got %d", rec.Code)
	}
}

func decode(t *testing.T, body string, max int64, strict bool) (struct{ A int }, error) {
	t.Helper()
	var v struct{ A int }
	r := httptest.NewRequest("POST", "/", strings.NewReader(body))
	return v, DecodeJSON(httptest.NewRecorder(), r, &v, max, strict)
}

func TestDecodeJSON(t *testing.T) {
	v, err := decode(t, `{"A":2}`, 0, true)
	if err != nil || v.A != 2 {
		t.Fatal(v, err)
	}
}

func TestDecodeJSONInvalidAndTooLarge(t *testing.T) {
	_, err := decode(t, `{"A":`, 0, false)
	if p := problem.Wrap(err); p.Status != 400 || p.Code != "invalid_body" {
		t.Fatalf("%+v", p)
	}
	_, err = decode(t, `{"A":1,"pad":"`+strings.Repeat("x", 100)+`"}`, 16, false)
	if p := problem.Wrap(err); p.Status != 413 {
		t.Fatalf("%+v", p)
	}
	_, err = decode(t, `{"A":1,"B":2}`, 0, true)
	if err == nil {
		t.Fatal("strict must reject unknown member")
	}
}
