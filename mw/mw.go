// Package mw has the small net/http middlewares services otherwise rewrite:
// panic recovery, security headers, CSRF defence via http.CrossOriginProtection
// (Go 1.25), an allowlist CORS policy and a handler timeout. Request ids and
// completion logging live in reqid and logx; compose with Chain.
package mw

import (
	"log/slog"
	"net/http"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/tcpsystems/plinth/problem"
)

// Middleware wraps a handler.
type Middleware func(http.Handler) http.Handler

// Chain applies mws so that the first listed is outermost.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for _, m := range slices.Backward(mws) {
		h = m(h)
	}
	return h
}

// Recover turns a handler panic into a bare 500 problem and logs the panic
// value and stack to logger (never to the client). http.ErrAbortHandler is
// re-panicked as net/http expects.
func Recover(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				v := recover()
				if v == nil {
					return
				}
				if v == http.ErrAbortHandler {
					panic(v)
				}
				logger.Error("panic", "path", r.URL.Path, "panic", v, "stack", string(debug.Stack()))
				problem.WriteError(w, r, problem.NewStatus(http.StatusInternalServerError, nil, "internal", "internal error"))
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// SecurityHeaders sets nosniff, no framing and a no-referrer policy.
func SecurityHeaders() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			h := w.Header()
			h.Set("X-Content-Type-Options", "nosniff")
			h.Set("X-Frame-Options", "DENY")
			h.Set("Referrer-Policy", "no-referrer")
			next.ServeHTTP(w, r)
		})
	}
}

// CSRF rejects cross-origin state-changing requests using
// http.CrossOriginProtection (Sec-Fetch-Site / Origin check, no tokens).
// trusted origins, such as "https://admin.example.com", bypass the check.
func CSRF(trusted ...string) Middleware {
	cop := http.NewCrossOriginProtection()
	for _, o := range trusted {
		if err := cop.AddTrustedOrigin(o); err != nil {
			panic("mw.CSRF: " + err.Error())
		}
	}
	return func(next http.Handler) http.Handler { return cop.Handler(next) }
}

// CORS allows the exact origins listed (never "*" with credentials). Preflight
// OPTIONS requests from an allowed origin are answered with 204.
func CORS(origins []string, methods []string, headers []string) Middleware {
	m, hd := strings.Join(methods, ", "), strings.Join(headers, ", ")
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			o := r.Header.Get("Origin")
			if o == "" || !slices.Contains(origins, o) {
				next.ServeHTTP(w, r)
				return
			}
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", o)
			h.Add("Vary", "Origin")
			if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
				h.Set("Access-Control-Allow-Methods", m)
				h.Set("Access-Control-Allow-Headers", hd)
				h.Set("Access-Control-Max-Age", "600")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// Timeout bounds the whole handler with http.TimeoutHandler semantics.
func Timeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.TimeoutHandler(next, d, `{"type":"https://tcp.systems/problems/timeout","title":"Service Unavailable","status":503,"code":"timeout"}`)
	}
}
