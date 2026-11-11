package envx_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/envx"
)

func ExampleMap() {
	m := map[string]string{
		"PORT": "8080",
		"HOST": "localhost",
	}
	lookup := envx.Map(m)

	port := lookup("PORT")
	host := lookup("MISSING")

	fmt.Println(port)
	fmt.Println(host)

	// Output: 8080

}
