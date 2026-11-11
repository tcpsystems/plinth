package outbox_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/outbox"
)

func ExampleNewID() {
	id1 := outbox.NewID()
	id2 := outbox.NewID()
	fmt.Println(len(id1) == 32)
	fmt.Println(id1 != id2)

	// Output: true
	// true
}
