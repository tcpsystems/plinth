package retry

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

var errBoom = errors.New("boom")

func TestDoSucceedsAfterRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		start := time.Now()
		err := Do(t.Context(), Policy{Attempts: 5, Base: time.Second, Max: 4 * time.Second}, func(context.Context) error {
			calls++
			if calls < 3 {
				return errBoom
			}
			return nil
		})
		if err != nil || calls != 3 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
		// delays are jittered in [0,1s] and [0,2s]: bounded, and virtual time is exact
		if el := time.Since(start); el > 3*time.Second {
			t.Fatalf("waited %v", el)
		}
	})
}

func TestDoExhaustsAttempts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		err := Do(t.Context(), Policy{Attempts: 3}, func(context.Context) error { calls++; return errBoom })
		if !errors.Is(err, errBoom) || calls != 3 {
			t.Fatalf("err=%v calls=%d", err, calls)
		}
	})
}

func TestPermanentStops(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{}, func(context.Context) error { calls++; return Permanent(errBoom) })
	if !errors.Is(err, errBoom) || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestRetryablePredicate(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{Retryable: func(error) bool { return false }}, func(context.Context) error { calls++; return errBoom })
	if err == nil || calls != 1 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestContextCancelDuringWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
		defer cancel()
		err := Do(ctx, Policy{Attempts: 10, Base: time.Hour, Max: time.Hour}, func(context.Context) error { return errBoom })
		if !errors.Is(err, errBoom) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestDelayBounds(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Max: time.Second}
	for n := 1; n < 80; n++ {
		if d := p.Delay(n); d < 0 || d > time.Second {
			t.Fatalf("n=%d delay=%v", n, d)
		}
	}
}
