package access

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
)

// Principal is a verified Access identity together with the roles the
// service resolved for it.
type Principal struct {
	Identity Identity
	Roles    []string
}

// HasRole reports whether the principal holds role.
func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

type ctxKey struct{}

// FromContext returns the principal Middleware stored on the request.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// Token extracts the Access token from a request: the Cf-Access-Jwt-Assertion
// header Access adds when it proxies, else the CF_Authorization cookie.
// A present-but-empty header does not fall through to the cookie.
func Token(r *http.Request) string {
	if vals := r.Header.Values(HeaderName); len(vals) > 0 {
		if len(vals) > 1 {
			return "" // ambiguous: treated as missing, never merged
		}
		return vals[0]
	}
	if c, err := r.Cookie(CookieName); err == nil {
		return c.Value
	}
	return ""
}

// Middleware verifies the Access token on every request, resolves roles with
// Config.Roles and stores a Principal on the context. It answers:
//
//	401 no token, malformed token, bad signature, expired, wrong issuer or audience
//	403 valid token but the identity has no role
//	503 signing keys or the role store unavailable
//
// The next handler only runs for a verified identity with at least one role.
// It panics at construction if the Verifier has no role resolver, because
// authenticating without authorizing would let Access stand in for roles.
func (v *Verifier) Middleware(next http.Handler) http.Handler {
	if v.roles == nil {
		panic("access: Middleware requires Config.Roles")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := v.Verify(r.Context(), Token(r))
		if err != nil {
			switch {
			case errors.Is(err, ErrKeysUnavailable):
				deny(w, http.StatusServiceUnavailable, "unavailable", "Authentication unavailable")
			default:
				w.Header().Set("WWW-Authenticate", `Bearer realm="access"`)
				deny(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
			}
			return
		}
		roles, err := v.roles(r.Context(), id)
		switch {
		case errors.Is(err, ErrNoRole), err == nil && len(roles) == 0:
			deny(w, http.StatusForbidden, "forbidden", "No role for this service")
			return
		case err != nil:
			deny(w, http.StatusServiceUnavailable, "unavailable", "Authorization unavailable")
			return
		}
		p := Principal{Identity: id, Roles: slices.Clone(roles)}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, p)))
	})
}

// RequireRole wraps next so it runs only when the principal on the request
// holds role; without a principal it answers 401, without the role 403.
func RequireRole(role string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := FromContext(r.Context())
		if !ok {
			deny(w, http.StatusUnauthorized, "unauthorized", "Authentication required")
			return
		}
		if !p.HasRole(role) {
			deny(w, http.StatusForbidden, "forbidden", "Missing role "+role)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// deny writes a problem+json body with a fixed message: no token content,
// key material or internal error text is ever echoed.
func deny(w http.ResponseWriter, status int, code, title string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"type": "https://tcp.systems/problems/" + code, "title": title, "status": status, "code": code,
	})
}
