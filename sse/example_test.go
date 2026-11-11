package sse_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/sse"
)

func ExampleNewBroker() {
	// Broker requires an *sql.DB, which we cannot create in a self-contained
	// example. Show the exported type and defaults instead.
	fmt.Println(sse.HeartbeatInterval)

	// Output: 15s
}
