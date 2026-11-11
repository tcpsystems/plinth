// Package bearer parses credentials; authentication and authorization stay local.
package bearer

import (
	"net/http"
	"strings"
)

// Token accepts one Authorization header with a case-insensitive Bearer scheme
// and a token68 credential. Duplicate headers, commas, controls and extra fields
// fail closed. It never reads query parameters or cookies.
func Token(r *http.Request) (string, bool) {
	headers := r.Header.Values("Authorization")
	if len(headers) != 1 {
		return "", false
	}
	scheme, token, ok := strings.Cut(headers[0], " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	token = strings.TrimLeft(token, " ")
	if token == "" {
		return "", false
	}
	padding := false
	for i, c := range token {
		if c == '=' {
			if i == 0 {
				return "", false
			}
			padding = true
			continue
		}
		if padding || !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("-._~+/", c)) {
			return "", false
		}
	}
	return token, true
}
