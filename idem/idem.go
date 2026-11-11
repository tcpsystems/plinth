// Package idem implements the Idempotency-Key contract as HTTP middleware.
//
// A request with a key is executed once. The first response (status below 500)
// is stored with a hash of method, path and body; a repeat with the same hash
// replays it with "Idempotent-Replay: true", and a repeat with a different hash
// is a 422 problem idempotency_key_reuse. Concurrent duplicates in one process
// are serialized, across processes the Store's Put decides (first writer wins).
package idem

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/tcpsystems/plinth/problem"
)

// Header and ReplayHeader are the request and response header names.
const (
	Header       = "Idempotency-Key"
	ReplayHeader = "Idempotent-Replay"
	// DefaultTTL is how long a stored response can be replayed.
	DefaultTTL = 24 * time.Hour
	maxKey     = 255
)

// Record is a stored response.
type Record struct {
	Key         string
	RequestHash string
	Status      int
	ContentType string
	Body        []byte
	ExpiresAt   time.Time
}

// Store persists records. Implementations must treat expired records as absent.
type Store interface {
	// Get returns the live record for key, or (nil, nil).
	Get(ctx context.Context, key string, now time.Time) (*Record, error)
	// Put stores rec unless a live record exists; stored reports whether it did.
	Put(ctx context.Context, rec Record, now time.Time) (stored bool, err error)
}

// Options configures Middleware.
type Options struct {
	Store    Store
	Required bool                       // non-GET requests without a key get 400 idempotency_key_required
	TTL      time.Duration              // default DefaultTTL
	MaxBody  int64                      // request body cap, default 1 MiB
	Scope    func(*http.Request) string // optional prefix (for example the caller) so keys never collide across callers
	Now      func() time.Time           // test clock
}

func safeMethod(m string) bool {
	return m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions
}

type recorder struct {
	http.ResponseWriter
	status int
	buf    bytes.Buffer
}

func (r *recorder) WriteHeader(c int) {
	if r.status == 0 {
		r.status = c
	}
	r.ResponseWriter.WriteHeader(c)
}
func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	r.buf.Write(b)
	return r.ResponseWriter.Write(b)
}
func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

type locks struct {
	mu sync.Mutex
	m  map[string]*lock
}
type lock struct {
	sync.Mutex
	n int
}

func (l *locks) acquire(k string) func() {
	l.mu.Lock()
	e := l.m[k]
	if e == nil {
		e = &lock{}
		l.m[k] = e
	}
	e.n++
	l.mu.Unlock()
	e.Lock()
	return func() {
		e.Unlock()
		l.mu.Lock()
		if e.n--; e.n == 0 {
			delete(l.m, k)
		}
		l.mu.Unlock()
	}
}

// Middleware wraps next. It panics if opts.Store is nil.
func Middleware(opts Options) func(http.Handler) http.Handler {
	if opts.Store == nil {
		panic("idem: Store is required")
	}
	if opts.TTL <= 0 {
		opts.TTL = DefaultTTL
	}
	if opts.MaxBody <= 0 {
		opts.MaxBody = 1 << 20
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	lk := &locks{m: map[string]*lock{}}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if safeMethod(r.Method) {
				next.ServeHTTP(w, r)
				return
			}
			key := r.Header.Get(Header)
			if key == "" {
				if opts.Required {
					problem.WriteError(w, r, problem.New(problem.ErrInvalid, "idempotency_key_required", "Idempotency-Key header is required"))
					return
				}
				next.ServeHTTP(w, r)
				return
			}
			if len(key) > maxKey || !printable(key) {
				problem.WriteError(w, r, problem.New(problem.ErrInvalid, "idempotency_key_invalid", "Idempotency-Key must be 1-255 printable ASCII characters"))
				return
			}
			body, err := io.ReadAll(io.LimitReader(r.Body, opts.MaxBody+1))
			if err != nil || int64(len(body)) > opts.MaxBody {
				problem.WriteError(w, r, problem.New(problem.ErrInvalid, "request_too_large", "request body too large"))
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			h := sha256.New()
			h.Write([]byte(r.Method + "\n" + r.URL.RequestURI() + "\n"))
			h.Write(body)
			hash := hex.EncodeToString(h.Sum(nil))
			full := key
			if opts.Scope != nil {
				full = opts.Scope(r) + "\x00" + key
			}
			defer lk.acquire(full)()

			now := opts.Now()
			rec, err := opts.Store.Get(r.Context(), full, now)
			if err != nil {
				problem.WriteError(w, r, err)
				return
			}
			if rec != nil {
				if rec.RequestHash != hash {
					problem.WriteError(w, r, problem.NewStatus(http.StatusUnprocessableEntity, problem.ErrInvalid, "idempotency_key_reuse", "Idempotency-Key was already used with a different request"))
					return
				}
				if rec.ContentType != "" {
					w.Header().Set("Content-Type", rec.ContentType)
				}
				w.Header().Set(ReplayHeader, "true")
				w.WriteHeader(rec.Status)
				_, _ = w.Write(rec.Body)
				return
			}
			rw := &recorder{ResponseWriter: w}
			next.ServeHTTP(rw, r)
			if rw.status == 0 {
				rw.status = http.StatusOK
			}
			if rw.status >= 500 {
				return // let the client retry
			}
			_, _ = opts.Store.Put(context.WithoutCancel(r.Context()), Record{
				Key: full, RequestHash: hash, Status: rw.status, ContentType: w.Header().Get("Content-Type"),
				Body: rw.buf.Bytes(), ExpiresAt: now.Add(opts.TTL),
			}, now)
		})
	}
}

func printable(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}
