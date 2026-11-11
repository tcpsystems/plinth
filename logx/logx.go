// Package logx provides structured request logging without credentials, query
// strings or bodies. Applications choose their slog handler and output.
package logx

import (
	"bufio"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/tcpsystems/plinth/reqid"
)

type writer struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (w *writer) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *writer) WriteHeader(code int) {
	if w.status != 0 {
		return
	}
	if code >= 200 || code == http.StatusSwitchingProtocols {
		w.status = code
	}
	w.ResponseWriter.WriteHeader(code)
}
func (w *writer) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += n
	return n, err
}
func (w *writer) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *writer) Flush() { _ = w.FlushError() }
func (w *writer) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

// Middleware logs once on completion or panic; panics propagate to the server.
// Place reqid.Middleware outside it. Streaming is forwarded without buffering.
func Middleware(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &writer{ResponseWriter: w}
		completed := false
		defer func() {
			code := sw.status
			if code == 0 {
				code = http.StatusOK
			}
			if !completed {
				code = http.StatusInternalServerError
			}
			logger.InfoContext(r.Context(), "http_request", "method", r.Method, "path", r.URL.Path,
				"status", code, "bytes", sw.bytes, "duration_ms", time.Since(start).Milliseconds(),
				"request_id", reqid.FromContext(r.Context()), "completed", completed)
		}()
		next.ServeHTTP(sw, r)
		completed = true
	})
}
