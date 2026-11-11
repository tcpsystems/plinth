// Package config reads bootstrap DSNs and secret references only. Runtime policy
// belongs in the application's database. Values must never be logged.
package config

import (
	"errors"
	"strings"
)

// Lookup is compatible with os.Getenv and permits isolated tests.
type Lookup func(string) string

// DSN returns the first nonblank DSN, then fallback. Parsing and pooling remain
// with the application's database driver; URL and keyword DSNs are supported.
func DSN(get Lookup, fallback string, names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(get(name)); value != "" {
			return value
		}
	}
	return fallback
}

// SecretRef loads an opaque secret-store reference, without fetching a secret.
// The application owns the resolver and its access control. Errors omit values.
func SecretRef(get Lookup, name string) (string, error) {
	ref := strings.TrimSpace(get(name))
	if ref == "" {
		return "", errors.New("secret reference is required")
	}
	for _, c := range ref {
		if c <= 32 || c == 127 {
			return "", errors.New("invalid secret reference")
		}
	}
	return ref, nil
}
