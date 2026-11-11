package reqid_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/reqid"
)

func ExampleNewUUIDv7() {
	id1 := reqid.NewUUIDv7()
	id2 := reqid.NewUUIDv7()
	fmt.Println(len(id1) == 36)
	fmt.Println(id1 != id2)

	// Output: true
	// true
}
