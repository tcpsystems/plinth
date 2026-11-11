package migrate_test

import (
	"fmt"
	"testing/fstest"

	"github.com/tcpsystems/plinth/migrate"
)

func ExampleLoad() {
	fsys := fstest.MapFS{
		"sql/001_create_users.sql": {Data: []byte("CREATE TABLE users (id int);")},
		"sql/002_create_posts.sql": {Data: []byte("CREATE TABLE posts (id int);")},
	}
	ms, err := migrate.Load(fsys, "sql")
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d migrations loaded\n", len(ms))
	fmt.Printf("first=%d name=%q\n", ms[0].Version, ms[0].Name)

	// Output: 2 migrations loaded
	// first=1 name="create_users"
}
