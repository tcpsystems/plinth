package conformance

import (
	"strings"
	"testing"
)

func TestCaptureLogger(t *testing.T) {
	l, out := CaptureLogger()
	l.Info("hello", "k", "v")
	if !strings.Contains(out(), `"k":"v"`) {
		t.Fatal(out())
	}
}
