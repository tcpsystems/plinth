// Package ratelimit writes the IETF RateLimit and RateLimit-Policy headers and
// a fixed-window limiter that sets them.
package ratelimit

import (
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/tcpsystems/plinth/problem"
)

// State is the quota state of one caller at one instant.
type State struct {
	Limit     int
	Remaining int
	Reset     time.Duration // time until the window resets
	Window    time.Duration
}

// SetHeaders writes RateLimit and RateLimit-Policy.
func SetHeaders(h http.Header, s State) {
	rem := s.Remaining
	if rem < 0 {
		rem = 0
	}
	h.Set("RateLimit", fmt.Sprintf("limit=%d, remaining=%d, reset=%d", s.Limit, rem, ceilSeconds(s.Reset)))
	h.Set("RateLimit-Policy", fmt.Sprintf("%d;w=%d", s.Limit, ceilSeconds(s.Window)))
}

func ceilSeconds(d time.Duration) int { return int((d + time.Second - 1) / time.Second) }

// Limiter is an in-memory fixed-window limiter keyed by caller. Exempt callers
// (internal keys) are never limited and never capped.
type Limiter struct {
	Limit  int
	Window time.Duration
	Key    func(*http.Request) string // caller key
	Exempt func(*http.Request) bool   // optional
	Now    func() time.Time

	mu   sync.Mutex
	wins map[string]*win
}

type win struct {
	start time.Time
	n     int
}

func (l *Limiter) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

// Middleware enforces the limit, sets headers on every response and answers
// 429 problem+json with Retry-After when exhausted.
func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if l.Exempt != nil && l.Exempt(r) {
			next.ServeHTTP(w, r)
			return
		}
		now := l.now()
		l.mu.Lock()
		if l.wins == nil {
			l.wins = map[string]*win{}
		}
		k := l.Key(r)
		c := l.wins[k]
		if c == nil || now.Sub(c.start) >= l.Window {
			c = &win{start: now}
			l.wins[k] = c
		}
		c.n++
		st := State{Limit: l.Limit, Remaining: l.Limit - c.n, Reset: c.start.Add(l.Window).Sub(now), Window: l.Window}
		l.mu.Unlock()
		SetHeaders(w.Header(), st)
		if c.n > l.Limit {
			w.Header().Set("Retry-After", strconv.Itoa(ceilSeconds(st.Reset)))
			problem.Write(w, r, problem.Problem{
				Type: problem.Type(problem.CodeRateLimited), Title: "Too Many Requests",
				Status: http.StatusTooManyRequests, Code: problem.CodeRateLimited,
				Detail: "rate limit exceeded",
			})
			return
		}
		next.ServeHTTP(w, r)
	})
}
