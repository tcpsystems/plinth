package page_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/page"
)

func ExampleNewSigner() {
	s, err := page.NewSigner([]byte("0123456789abcdef"))
	if err != nil {
		panic(err)
	}

	type cursor struct {
		After string `json:"after"`
	}

	tok, err := s.Encode(cursor{After: "abc123"})
	if err != nil {
		panic(err)
	}
	fmt.Println(len(tok) > 0)

	var c cursor
	_ = s.Decode(tok, &c)
	fmt.Println(c.After)

	// Output: true
	// abc123
}

func ExampleTrim() {
	items, more := page.Trim([]int{1, 2, 3, 4, 5}, 3)
	fmt.Println(items)
	fmt.Println(more)

	// Output: [1 2 3]
	// true
}

func ExampleKeyset() {
	fmt.Println(page.Keyset(1, "created_at", "id"))

	// Output: (created_at, id) > ($1, $2)
}
