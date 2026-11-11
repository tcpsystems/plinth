package retry_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tcpsystems/plinth/retry"
)

func ExampleDo() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	calls := 0
	err := retry.Do(ctx, retry.Policy{Attempts: 3, Base: 10 * time.Millisecond, Max: 20 * time.Millisecond}, func(context.Context) error {
		calls++
		if calls < 2 {
			return errors.New("fail")
		}
		return nil
	})
	fmt.Println(err)
	fmt.Println(calls)

	// Output: <nil>
	// 2
}
