package jsonx_test

import (
	"fmt"
	"strings"

	"github.com/tcpsystems/plinth/jsonx"
)

func ExampleMarshal() {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	b, err := jsonx.Marshal(item{ID: 1, Name: "alpha"})
	fmt.Println(string(b))
	fmt.Println(err)

	// Output: {"id":1,"name":"alpha"}
	// <nil>
}

func ExampleUnmarshal() {
	type item struct {
		ID   int    `json:"id"`
		Name string `json:"name"`
	}
	var it item
	err := jsonx.Unmarshal([]byte(`{"id":2,"name":"beta"}`), &it)
	fmt.Printf("id=%d name=%q\n", it.ID, it.Name)
	fmt.Println(err)

	// Output: id=2 name="beta"
	// <nil>
}

func ExampleUnmarshalStrict() {
	type item struct {
		ID int `json:"id"`
	}
	var it item
	err := jsonx.UnmarshalStrict([]byte(`{"id":3,"extra":"x"}`), &it)
	fmt.Println(err != nil)

	// Output: true
}

func ExampleWrite() {
	var buf strings.Builder
	_ = jsonx.Write(&buf, map[string]int{"count": 5})
	fmt.Println(buf.String())

	// Output: {"count":5}
}
