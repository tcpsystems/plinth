// Package httpx holds the JSON request/response helpers every service
// re-implemented as writeJSON/decodeBody (61 copies measured across 12 repos).
// Encoding goes through jsonx so the engine has one switch point.
package httpx

import (
	"errors"
	"net/http"

	"github.com/tcpsystems/plinth/jsonx"
	"github.com/tcpsystems/plinth/problem"
)

// DefaultMaxBody is the request body limit used when DecodeJSON gets max <= 0.
const DefaultMaxBody = 1 << 20

// WriteJSON writes v as application/json with the given status. An encoding
// failure before any byte is written becomes a bare 500.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	b, err := jsonx.Marshal(v)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(append(b, '\n'))
}

// DecodeJSON reads one JSON value from r.Body into v, capping the body at max
// bytes (DefaultMaxBody if max <= 0). Unknown members are rejected when strict.
// Errors are problem errors: 413 body_too_large or 400 invalid_body; the
// decoder's own text is never included.
func DecodeJSON(w http.ResponseWriter, r *http.Request, v any, max int64, strict bool) error {
	if max <= 0 {
		max = DefaultMaxBody
	}
	err := jsonx.Read(http.MaxBytesReader(w, r.Body, max), v, strict)
	if err == nil {
		return nil
	}
	var mbe *http.MaxBytesError
	if errors.As(err, &mbe) {
		return problem.NewStatus(http.StatusRequestEntityTooLarge, problem.ErrInvalid, "body_too_large", "request body too large")
	}
	return problem.New(problem.ErrInvalid, "invalid_body", "request body is not valid JSON for this endpoint")
}
