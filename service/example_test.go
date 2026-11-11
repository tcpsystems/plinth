package service_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/service"
)

func ExampleReadVersion() {
	v := service.ReadVersion("my-service", 3, map[string]string{"service": "1"})
	fmt.Println(v.Service)
	fmt.Println(v.SchemaVersion)
	fmt.Println(v.Contracts["service"])

	// Output: my-service
	// 3
	// 1
}
