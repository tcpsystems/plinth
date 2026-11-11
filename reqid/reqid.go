// Package reqid propagates bounded request correlation IDs. IDs are untrusted
// correlation labels, never authorization or idempotency keys.
//
// Generated IDs use UUIDv7 (RFC 9562): a 128-bit value with a 48-bit Unix
// timestamp in milliseconds, version 7, RFC 4122 variant, and 74 random bits.
// String form: 018f4f8a-7b3d-7c8a-8b9c-0123456789ab (36 chars).
package reqid

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"
)

// Header is the HTTP header name used to propagate request correlation IDs.
const Header = "X-Request-ID"

type key struct{}

// FromContext returns the request correlation ID stored in ctx, or "".
func FromContext(ctx context.Context) string { id, _ := ctx.Value(key{}).(string); return id }

func valid(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// NewUUIDv7 generates a UUIDv7 (RFC 9562) string.
func NewUUIDv7() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	ms := time.Now().UnixMilli()
	b[0] = byte(ms >> 40)
	b[1] = byte(ms >> 32)
	b[2] = byte(ms >> 24)
	b[3] = byte(ms >> 16)
	b[4] = byte(ms >> 8)
	b[5] = byte(ms)
	// version 7 in bits 4-7 of byte 6
	b[6] = (b[6] & 0x0f) | 0x70
	// variant 10 in bits 6-7 of byte 8
	b[8] = (b[8] & 0x3f) | 0x80
	return hex.EncodeToString(b[:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" + hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" + hex.EncodeToString(b[10:])
}

// Middleware accepts one safe incoming ID or generates a UUIDv7, and
// sets both the downstream header/context and response header.
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ids := r.Header.Values(Header)
		id := ""
		if len(ids) == 1 && valid(ids[0]) {
			id = ids[0]
		}
		if id == "" {
			id = NewUUIDv7()
		}
		r = r.Clone(context.WithValue(r.Context(), key{}, id))
		r.Header.Set(Header, id)
		w.Header().Set(Header, id)
		next.ServeHTTP(w, r)
	})
}
