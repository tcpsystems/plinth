package webhooks

import (
	"net/http"
	"testing"
	"time"
)

func TestSignVerify(t *testing.T) {
	// Standard Webhooks vector shape: fixed inputs give a fixed signature.
	secret, body := []byte("whsec-test"), []byte(`{"event":"card.created"}`)
	now := time.Unix(1700000000, 0)
	h := http.Header{}
	SetHeaders(h, secret, "msg_1", now, body)
	if Sign(secret, "msg_1", now, body) != h.Get(HeaderSignature) {
		t.Fatal("deterministic")
	}
	if err := Verify(h, body, now.Add(time.Minute), secret); err != nil {
		t.Fatal(err)
	}
	if err := Verify(h, body, now, []byte("old"), secret); err != nil {
		t.Fatal("rotation", err)
	}
	if Verify(h, []byte("tampered"), now, secret) != ErrBadSignature {
		t.Fatal("tamper")
	}
	if Verify(h, body, now.Add(10*time.Minute), secret) != ErrBadTimestamp {
		t.Fatal("replay")
	}
	if Verify(http.Header{}, body, now, secret) != ErrMissingHeaders {
		t.Fatal("missing")
	}
	h.Set(HeaderSignature, "v1,AAAA "+h.Get(HeaderSignature))
	if Verify(h, body, now, secret) != nil {
		t.Fatal("multi signature")
	}
}

func TestBackoff(t *testing.T) {
	if Backoff(1) != 5*time.Second || Backoff(2) != 10*time.Second || Backoff(40) != time.Hour {
		t.Fatal(Backoff(1), Backoff(2), Backoff(40))
	}
}
