package jobs_test

import (
	"testing"
	"time"

	"github.com/tcpsystems/plinth/jobs"
)

func TestDefaultBackoff(t *testing.T) {
	if got := jobs.DefaultBackoff(1); got != time.Second {
		t.Fatalf("attempt 1: %v", got)
	}
	if got := jobs.DefaultBackoff(3); got != 4*time.Second {
		t.Fatalf("attempt 3: %v", got)
	}
	if got := jobs.DefaultBackoff(1000); got != 5*time.Minute {
		t.Fatalf("attempt 1000: %v", got)
	}
	capped := jobs.ExponentialBackoff(time.Second, time.Minute)
	for _, a := range []int{-1, 1, 6, 7, 8, 64} {
		if d := capped(a); d <= 0 || d > time.Minute {
			t.Fatalf("attempt %d: %v", a, d)
		}
	}
	if got := capped(7); got != time.Minute {
		t.Fatalf("attempt 7: %v", got)
	}
	if got := jobs.ExponentialBackoff(0, 0)(2); got != 2*time.Second {
		t.Fatalf("defaults: %v", got)
	}
}
