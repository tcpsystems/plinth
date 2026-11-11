// Package envx reads typed configuration from the environment. It replaces the
// envOr/envInt/envBool/envDuration families copied into 10 repos (34 functions,
// 629 LOC measured). Lookups are injected (see Lookup) so tests need no
// process environment, matching package config.
package envx

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Lookup returns the value of a variable, or "" when unset.
type Lookup func(string) string

// OS is the process environment.
var OS Lookup = os.Getenv

func (l Lookup) get(name string) string { return strings.TrimSpace(l(name)) }

// String returns the trimmed value of name, or def when empty.
func (l Lookup) String(name, def string) string {
	if v := l.get(name); v != "" {
		return v
	}
	return def
}

// Int returns name as an int, or def when unset or malformed.
func (l Lookup) Int(name string, def int) int {
	if n, err := strconv.Atoi(l.get(name)); err == nil {
		return n
	}
	return def
}

// Int64 returns name as an int64, or def when unset or malformed.
func (l Lookup) Int64(name string, def int64) int64 {
	if n, err := strconv.ParseInt(l.get(name), 10, 64); err == nil {
		return n
	}
	return def
}

// Float returns name as a float64, or def when unset or malformed.
func (l Lookup) Float(name string, def float64) float64 {
	if f, err := strconv.ParseFloat(l.get(name), 64); err == nil {
		return f
	}
	return def
}

// Bool accepts 1/true/yes/on and 0/false/no/off (any case); otherwise def.
func (l Lookup) Bool(name string, def bool) bool {
	switch strings.ToLower(l.get(name)) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return def
}

// Duration parses a time.ParseDuration string, or returns def.
func (l Lookup) Duration(name string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(l.get(name)); err == nil {
		return d
	}
	return def
}

// List splits name on commas, trimming blanks; def when the result is empty.
func (l Lookup) List(name string, def []string) []string {
	var out []string
	for _, p := range strings.Split(l.get(name), ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// First returns the first non-empty value among names, or def.
func (l Lookup) First(def string, names ...string) string {
	for _, n := range names {
		if v := l.get(n); v != "" {
			return v
		}
	}
	return def
}

// Map adapts a map to a Lookup, for tests.
func Map(m map[string]string) Lookup { return func(k string) string { return m[k] } }
