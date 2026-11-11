// Package webhooks signs and verifies deliveries per the Standard Webhooks
// specification (webhook-id, webhook-timestamp, webhook-signature, v1 HMAC-SHA256).
package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	HeaderID        = "webhook-id"
	HeaderTimestamp = "webhook-timestamp"
	HeaderSignature = "webhook-signature"
	// Tolerance is the maximum clock difference accepted on verify.
	Tolerance = 5 * time.Minute
)

var (
	ErrMissingHeaders = errors.New("webhooks: missing headers")
	ErrBadTimestamp   = errors.New("webhooks: timestamp outside tolerance")
	ErrBadSignature   = errors.New("webhooks: no valid signature")
)

// Sign returns the "v1,<base64>" signature of id.timestamp.body.
func Sign(secret []byte, id string, ts time.Time, body []byte) string {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(id + "." + strconv.FormatInt(ts.Unix(), 10) + "."))
	m.Write(body)
	return "v1," + base64.StdEncoding.EncodeToString(m.Sum(nil))
}

// SetHeaders adds the three delivery headers.
func SetHeaders(h http.Header, secret []byte, id string, ts time.Time, body []byte) {
	h.Set(HeaderID, id)
	h.Set(HeaderTimestamp, strconv.FormatInt(ts.Unix(), 10))
	h.Set(HeaderSignature, Sign(secret, id, ts, body))
}

// Verify checks headers against body using any of the given secrets (rotation).
// The header may hold several space-separated signatures.
func Verify(h http.Header, body []byte, now time.Time, secrets ...[]byte) error {
	id, tss, sig := h.Get(HeaderID), h.Get(HeaderTimestamp), h.Get(HeaderSignature)
	if id == "" || tss == "" || sig == "" {
		return ErrMissingHeaders
	}
	n, err := strconv.ParseInt(tss, 10, 64)
	if err != nil {
		return ErrBadTimestamp
	}
	ts := time.Unix(n, 0)
	if d := now.Sub(ts); d > Tolerance || d < -Tolerance {
		return ErrBadTimestamp
	}
	for _, s := range secrets {
		want := []byte(Sign(s, id, ts, body))
		for _, got := range strings.Fields(sig) {
			if hmac.Equal(want, []byte(got)) {
				return nil
			}
		}
	}
	return ErrBadSignature
}

// Backoff returns the delay before retry attempt n (1-based): 5s doubling, capped at 1h.
func Backoff(n int) time.Duration {
	d := 5 * time.Second
	for i := 1; i < n && d < time.Hour; i++ {
		d *= 2
	}
	if d > time.Hour {
		d = time.Hour
	}
	return d
}
