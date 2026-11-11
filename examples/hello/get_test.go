package main

import (
	"encoding/json"
	"net/http"
	"testing"
)

func get(t *testing.T, url string, v any) {
	t.Helper()
	r, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 || json.NewDecoder(r.Body).Decode(v) != nil {
		t.Fatalf("GET %s: %d", url, r.StatusCode)
	}
}
