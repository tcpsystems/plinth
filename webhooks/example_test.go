package webhooks_test

import (
	"fmt"
	"net/http"
	"time"

	"github.com/tcpsystems/plinth/webhooks"
)

func ExampleSign() {
	secret := []byte("whsec-test")
	body := []byte(`{"event":"order.created"}`)
	id := "msg_1"
	ts := time.Unix(1700000000, 0)

	sig := webhooks.Sign(secret, id, ts, body)
	fmt.Println(len(sig) > 0)

	h := http.Header{}
	webhooks.SetHeaders(h, secret, id, ts, body)
	fmt.Println(h.Get("webhook-signature") == sig)

	// Output: true
	// true
}

func ExampleVerify() {
	secret := []byte("whsec-test")
	body := []byte(`{"event":"order.created"}`)
	h := http.Header{}
	webhooks.SetHeaders(h, secret, "msg_1", time.Unix(1700000000, 0), body)

	err := webhooks.Verify(h, body, time.Unix(1700000000, 0), secret)
	fmt.Println(err)

	// Output: <nil>
}
