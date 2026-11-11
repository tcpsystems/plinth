package config_test

import (
	"fmt"

	"github.com/tcpsystems/plinth/config"
)

func ExampleDSN() {
	lookup := config.Lookup(func(name string) string {
		switch name {
		case "DATABASE_URL":
			return "postgres://localhost/mydb"
		default:
			return ""
		}
	})

	dsn := config.DSN(lookup, "postgres://fallback/db", "DATABASE_URL")
	fmt.Println(dsn)

	dsn = config.DSN(lookup, "fallback", "MISSING_KEY")
	fmt.Println(dsn)

	// Output: postgres://localhost/mydb
	// fallback
}
