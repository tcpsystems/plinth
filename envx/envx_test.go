package envx

import (
	"slices"
	"testing"
	"time"
)

func TestTypedReads(t *testing.T) {
	e := Map(map[string]string{
		"S": "  hi ", "I": "42", "I64": "9000000000", "F": "1.5", "B": "YES", "D": "2s", "L": "a, b,,c", "BAD": "x",
	})
	if e.String("S", "d") != "hi" || e.String("NONE", "d") != "d" {
		t.Fatal("string")
	}
	if e.Int("I", 0) != 42 || e.Int("BAD", 7) != 7 || e.Int("NONE", 8) != 8 {
		t.Fatal("int")
	}
	if e.Int64("I64", 0) != 9000000000 || e.Int64("BAD", 1) != 1 {
		t.Fatal("int64")
	}
	if e.Float("F", 0) != 1.5 || e.Float("BAD", 2) != 2 {
		t.Fatal("float")
	}
	if !e.Bool("B", false) || e.Bool("BAD", true) != true || e.Bool("NONE", false) {
		t.Fatal("bool")
	}
	if e.Duration("D", 0) != 2*time.Second || e.Duration("BAD", time.Minute) != time.Minute {
		t.Fatal("duration")
	}
	if !slices.Equal(e.List("L", nil), []string{"a", "b", "c"}) || !slices.Equal(e.List("NONE", []string{"z"}), []string{"z"}) {
		t.Fatal("list")
	}
	if e.First("d", "NONE", "S") != "hi" || e.First("d", "NONE") != "d" {
		t.Fatal("first")
	}
}

func TestBoolFalseWords(t *testing.T) {
	for _, v := range []string{"0", "false", "No", "OFF"} {
		if Map(map[string]string{"B": v}).Bool("B", true) {
			t.Fatal(v)
		}
	}
}

func TestOSLookup(t *testing.T) {
	t.Setenv("ENVX_TEST", "5")
	if OS.Int("ENVX_TEST", 0) != 5 {
		t.Fatal("os")
	}
}
