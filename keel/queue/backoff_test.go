package queue

import (
	"testing"
	"time"
)

func TestBackoffIsExponentialAndBounded(t *testing.T) {
	b := Backoff{Base: time.Second, Max: time.Minute}
	want := map[int]time.Duration{
		-3: time.Second, 0: time.Second, 1: time.Second, 2: 2 * time.Second,
		3: 4 * time.Second, 6: 32 * time.Second, 7: time.Minute, 50: time.Minute,
		5000: time.Minute, // 2^4999 overflows float64; must still clamp
	}
	for attempt, d := range want {
		if got := b.Delay(attempt); got != d {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, d)
		}
	}
}

func TestNewClientValidates(t *testing.T) {
	if _, err := NewClient(Config{}); err == nil {
		t.Error("nil database accepted")
	}
}
