// Package health supplies public probes without exposing dependency errors.
package health

import (
	"context"
	"net/http"
)

// Liveness checks only that the HTTP server can respond; never query the DB.
func Liveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte("{\"status\":\"ok\"}\n"))
}

// Readiness runs checks in order with the request context and a caller-supplied
// timeout. Use context.WithTimeout in check to bound dependency calls. No error
// text is returned to unauthenticated callers. No checks means ready.
func Readiness(checks ...func(context.Context) error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		for _, check := range checks {
			if err := check(r.Context()); err != nil {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("{\"status\":\"unavailable\"}\n"))
				return
			}
		}
		Liveness(w, r)
	})
}
