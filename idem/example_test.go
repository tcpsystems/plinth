package idem_test

import (
	"context"
	"fmt"
	"time"

	"github.com/tcpsystems/plinth/idem"
)

func ExampleMemoryStore() {
	store := idem.NewMemoryStore()

	// Store a record.
	now := time.Now()
	_, err := store.Put(context.Background(), idem.Record{
		Key:         "key-1",
		RequestHash: "abc",
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
		ExpiresAt:   now.Add(time.Hour),
	}, now)
	if err != nil {
		panic(err)
	}

	// Retrieve it.
	rec, err := store.Get(context.Background(), "key-1", now)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(rec.Body))

	// Non-existent key returns (nil, nil).
	_, err = store.Get(context.Background(), "missing", now)
	fmt.Println(err != nil)

	// Output: {"ok":true}
	// false
}
