package bearer

import (
	"net/http/httptest"
	"testing"
)

func TestToken(t *testing.T) {
	for _, tt := range []struct{ header, want string }{
		{"Bearer abc-._~+/09==", "abc-._~+/09=="}, {"bEaReR   token", "token"},
		{"", ""}, {"Basic token", ""}, {"Bearer ", ""}, {"Bearer a b", ""},
		{"Bearer\ttoken", ""}, {"Bearer token\r\n", ""}, {"Bearer a,b", ""},
		{"Bearer =abc", ""}, {"Bearer a=b", ""}, {"Bearer a\x00", ""}, {"Bearer ü", ""},
	} {
		t.Run(tt.header, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/?access_token=ignored", nil)
			r.Header.Set("Authorization", tt.header)
			got, ok := Token(r)
			if got != tt.want || ok != (tt.want != "") {
				t.Fatalf("got %q,%v", got, ok)
			}
		})
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Add("Authorization", "Bearer first")
	r.Header.Add("Authorization", "Bearer second")
	if _, ok := Token(r); ok {
		t.Fatal("accepted duplicate headers")
	}
}
