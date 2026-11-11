# access

Verifies Cloudflare Access JWTs at the origin, using only the standard
library.

## Access is an outer gate, not the authorization model

Access proves a request came through the edge policy for one application and
tells you who the caller is. It does not say what they may do in your service.
Treat a verified token as authentication only:

- Roles come from your own store, through `Config.Roles`. `Middleware` panics
  at construction when no resolver is configured, so Access can never stand in
  for roles by accident.
- A valid token whose identity has no role is answered `403`.
- Nothing ever falls back to an anonymous caller. Every failure is an explicit
  response and the wrapped handler does not run.

```go
v, err := access.New(access.Config{
    Team:     "acme.cloudflareaccess.com",
    Audience: []string{os.Getenv("ACCESS_AUD")},
    Roles: func(ctx context.Context, id access.Identity) ([]string, error) {
        roles, err := store.RolesFor(ctx, id.Subject) // your authorization data
        if len(roles) == 0 && err == nil {
            return nil, access.ErrNoRole
        }
        return roles, err
    },
})
mux.Handle("/admin/", v.Middleware(access.RequireRole("admin", adminHandler)))
```

`access.FromContext(r.Context())` returns the `Principal` (identity and
roles) inside the wrapped handler.

## What is checked

| Check | Failure |
|---|---|
| Token present in `Cf-Access-Jwt-Assertion` (else the `CF_Authorization` cookie); duplicate headers count as missing | 401 |
| Compact JWT, algorithm exactly `RS256`, key id present, signature valid against the team JWKS | 401 |
| `exp` present and not past, `nbf` and `iat` not in the future, each with clock skew (default 30s, max 2m) | 401 |
| `iss` equals `https://<team>` | 401 |
| `aud` (string or array) contains a configured application AUD tag | 401 |
| Subject or email present | 401 |
| Identity has at least one role | 403 |
| JWKS unreachable with no cached key for the token, or the role store errors | 503 |

Responses are `application/problem+json` with fixed text: token contents, key
material and internal errors are never echoed.

## Keys and rotation

Keys are fetched from `https://<team>/cdn-cgi/access/certs` and cached. A
token with an unknown key id triggers one refetch, at most once per
`MinRefresh` (default 30s), so a rotation is picked up without letting forged
key ids drive traffic to the edge. A successful fetch replaces the whole set,
so a rotated-out key stops verifying. If the endpoint is down, cached keys keep
working; a token needing an unknown key gets `503`.

Use `Verify` directly when you are not behind net/http middleware; classify
errors with `errors.Is` against `ErrMissingToken`, `ErrInvalidToken`,
`ErrExpired`, `ErrIssuer`, `ErrAudience`, `ErrUnknownKey` and
`ErrKeysUnavailable`.
