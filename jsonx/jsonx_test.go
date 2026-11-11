package jsonx

import (
	"bytes"
	"strings"
	"testing"
)

type item struct {
	Name string   `json:"name"`
	Tags []string `json:"tags"`
}

func TestMarshalV1Compat(t *testing.T) {
	b, err := Marshal(item{Name: "a"})
	if err != nil || string(b) != `{"name":"a","tags":null}` {
		t.Fatalf("got %s, %v (engine %s)", b, err, Engine())
	}
}

func TestUnmarshalLenient(t *testing.T) {
	var it item
	if err := Unmarshal([]byte(`{"NAME":"x","extra":1}`), &it); err != nil || it.Name != "x" {
		t.Fatalf("lenient: %+v %v", it, err)
	}
}

func TestUnmarshalStrictRejectsUnknown(t *testing.T) {
	var it item
	if err := UnmarshalStrict([]byte(`{"name":"x","extra":1}`), &it); err == nil {
		t.Fatal("expected unknown member error")
	}
	if err := UnmarshalStrict([]byte(`{"name":"x"}`), &it); err != nil {
		t.Fatal(err)
	}
}

func TestWriteRead(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, map[string]int{"a": 1}); err != nil || buf.String() != "{\"a\":1}\n" {
		t.Fatalf("%q %v", buf.String(), err)
	}
	var m map[string]int
	if err := Read(strings.NewReader(buf.String()), &m, true); err != nil || m["a"] != 1 {
		t.Fatal(m, err)
	}
}

func TestEngineNamed(t *testing.T) {
	if e := Engine(); e != "json/v2" && e != "json/v1" {
		t.Fatal(e)
	}
}
