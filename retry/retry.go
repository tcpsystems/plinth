// Package retry runs an operation with capped exponential backoff and full
// jitter, honouring context cancellation. It replaces the per-repo
// retry/backoff/sleepCtx helpers (12 copies, 148 LOC measured).
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Policy configures Do. Zero values give 4 attempts, 100ms base, 5s cap.
type Policy struct {
	Attempts int           // total tries including the first; <=0 means 4
	Base     time.Duration // first delay ceiling; <=0 means 100ms
	Max      time.Duration // delay ceiling; <=0 means 5s
	// Retryable reports whether err deserves another attempt; nil retries
	// every error except those wrapped with Permanent.
	Retryable func(error) bool
}

type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks err as not worth retrying.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// Delay is the full-jitter backoff before retry number n (1-based): a uniform
// value in [0, min(Max, Base<<(n-1))].
func (p Policy) Delay(n int) time.Duration {
	base, max := p.Base, p.Max
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	if max <= 0 {
		max = 5 * time.Second
	}
	ceil := max
	if n-1 < 32 {
		if c := base << (n - 1); c > 0 && c < max {
			ceil = c
		}
	}
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}

// Do calls fn until it succeeds, returns a non-retryable error, attempts are
// exhausted or ctx ends. It returns the last error from fn, or ctx's error if
// the context ended while waiting.
func Do(ctx context.Context, p Policy, fn func(ctx context.Context) error) error {
	attempts := p.Attempts
	if attempts <= 0 {
		attempts = 4
	}
	var err error
	for n := 1; n <= attempts; n++ {
		if err = fn(ctx); err == nil {
			return nil
		}
		var perm permanent
		if errors.As(err, &perm) || (p.Retryable != nil && !p.Retryable(err)) || n == attempts {
			return err
		}
		t := time.NewTimer(p.Delay(n))
		select {
		case <-ctx.Done():
			t.Stop()
			return errors.Join(err, ctx.Err())
		case <-t.C:
		}
	}
	return err
}
