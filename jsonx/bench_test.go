package jsonx

import "testing"

type benchItem struct {
	A int    `json:"a"`
	B string `json:"b"`
}

// BenchmarkMarshal measures the engine's encode path for a small object, the
// unit every JSON response and SSE frame passes through.
func BenchmarkMarshal(b *testing.B) {
	v := benchItem{A: 1, B: "x"}
	for b.Loop() {
		if _, err := Marshal(v); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkUnmarshal measures the engine's decode path into a struct.
func BenchmarkUnmarshal(b *testing.B) {
	data := []byte(`{"a":1,"b":"x"}`)
	var v benchItem
	for b.Loop() {
		if err := Unmarshal(data, &v); err != nil {
			b.Fatal(err)
		}
	}
}
