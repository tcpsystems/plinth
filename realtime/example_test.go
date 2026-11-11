package realtime_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/realtime"
)

func ExampleNewHub() {
	// Hub is the in-process delivery fabric for SSE, WebSocket, and
	// Connect-JSON. It is configured through HubOptions and started with
	// Hub.ServeHTTP.
	hub := realtime.NewHub(realtime.HubOptions{})
	fmt.Println(hub != nil)

	// Output: true
}

func ExampleNewSchemas() {
	s := realtime.NewSchemas("my-service", "/ws")
	fmt.Println(s != nil)

	// Output: true
}
