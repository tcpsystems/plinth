// Package access verifies Cloudflare Access JWTs at the origin.
//
// Access is an outer gate: it proves a request passed through the edge policy
// for an application, nothing more. The verified identity says who the caller
// is; what they may do comes from the roles the service resolves for that
// identity (see Config.Roles). A valid token with no resolved role is denied,
// and every failure is an explicit error response: nothing falls back to an
// anonymous caller.
//
// Only RS256 is accepted, which is what Access signs with. Tokens are checked
// against the team JWKS (fetched over HTTPS and cached; an unknown key id
// triggers a rate-limited refetch so key rotation is picked up), the issuer,
// the application audience and the time claims with a bounded clock skew.
// The package uses only the standard library.
package access

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Verification failures. Use errors.Is to classify.
var (
	// ErrMissingToken means the request carried no Access token.
	ErrMissingToken = errors.New("access: missing token")
	// ErrInvalidToken means the token is malformed, uses a disallowed
	// algorithm, or its signature does not verify.
	ErrInvalidToken = errors.New("access: invalid token")
	// ErrExpired means the token is expired or not yet valid.
	ErrExpired = errors.New("access: token expired or not yet valid")
	// ErrIssuer means the issuer claim does not match the team domain.
	ErrIssuer = errors.New("access: wrong issuer")
	// ErrAudience means the audience does not include this application.
	ErrAudience = errors.New("access: wrong audience")
	// ErrUnknownKey means no key in the team JWKS matches the token key id.
	ErrUnknownKey = errors.New("access: unknown signing key")
	// ErrKeysUnavailable means the JWKS could not be fetched and no cached
	// key matched. It is a server-side condition, not a bad credential.
	ErrKeysUnavailable = errors.New("access: signing keys unavailable")
	// ErrNoRole is returned by a RoleResolver when the identity has no
	// access to this service.
	ErrNoRole = errors.New("access: identity has no role")
)

// HeaderName is the header Access adds to requests it forwards.
const HeaderName = "Cf-Access-Jwt-Assertion"

// CookieName is the cookie Access sets on the browser.
const CookieName = "CF_Authorization"

// Identity is the verified content of an Access token.
type Identity struct {
	Subject  string
	Email    string // empty for service tokens
	Issuer   string
	Audience []string
	Type     string // "app", "service-token" or as sent by Access
	Expires  time.Time
	IssuedAt time.Time
}

// RoleResolver maps a verified identity to the service's own roles. It is the
// authorization model; Access only authenticates. Return ErrNoRole (or an
// empty slice) to deny, any other error to report the role store unavailable.
type RoleResolver func(ctx context.Context, id Identity) ([]string, error)

// Config configures a Verifier.
type Config struct {
	// Team is the Access team domain, e.g. "acme.cloudflareaccess.com".
	// The expected issuer is "https://" + Team.
	Team string
	// Audience lists the accepted application AUD tags. At least one.
	Audience []string
	// Roles resolves the service roles for a verified identity. Required by
	// Middleware; Verify does not call it.
	Roles RoleResolver
	// JWKSURL overrides the keys endpoint. Default:
	// https://<Team>/cdn-cgi/access/certs.
	JWKSURL string
	// Skew is the tolerated clock difference for exp, nbf and iat.
	// Default 30s, maximum 2m.
	Skew time.Duration
	// MinRefresh is the shortest gap between JWKS fetches. Default 30s.
	MinRefresh time.Duration
	// Client fetches the JWKS. Default: a client with a 5s timeout.
	Client *http.Client
	// Now overrides the clock, for tests.
	Now func() time.Time
}

// Verifier checks Access tokens. It is safe for concurrent use.
type Verifier struct {
	issuer   string
	audience []string
	roles    RoleResolver
	url      string
	skew     time.Duration
	minGap   time.Duration
	client   *http.Client
	now      func() time.Time

	mu      sync.Mutex
	keys    map[string]*rsa.PublicKey
	fetched time.Time
	hasSet  bool
}

// New validates cfg and returns a Verifier.
func New(cfg Config) (*Verifier, error) {
	team := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(cfg.Team, "https://"), "http://"), "/")
	if team == "" || strings.ContainsAny(team, "/ ") {
		return nil, errors.New("access: Team must be a bare domain such as acme.cloudflareaccess.com")
	}
	if len(cfg.Audience) == 0 {
		return nil, errors.New("access: at least one Audience is required")
	}
	for _, a := range cfg.Audience {
		if a == "" {
			return nil, errors.New("access: empty Audience")
		}
	}
	v := &Verifier{
		issuer:   "https://" + team,
		audience: append([]string(nil), cfg.Audience...),
		roles:    cfg.Roles,
		url:      cfg.JWKSURL,
		skew:     cfg.Skew,
		minGap:   cfg.MinRefresh,
		client:   cfg.Client,
		now:      cfg.Now,
	}
	if v.url == "" {
		v.url = "https://" + team + "/cdn-cgi/access/certs"
	}
	if v.skew == 0 {
		v.skew = 30 * time.Second
	}
	if v.skew < 0 || v.skew > 2*time.Minute {
		return nil, errors.New("access: Skew must be between 0 and 2m")
	}
	if v.minGap == 0 {
		v.minGap = 30 * time.Second
	}
	if v.client == nil {
		v.client = &http.Client{Timeout: 5 * time.Second}
	}
	if v.now == nil {
		v.now = time.Now
	}
	return v, nil
}

type header struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

type claims struct {
	Iss   string          `json:"iss"`
	Sub   string          `json:"sub"`
	Aud   json.RawMessage `json:"aud"`
	Exp   *float64        `json:"exp"`
	Nbf   *float64        `json:"nbf"`
	Iat   *float64        `json:"iat"`
	Email string          `json:"email"`
	Type  string          `json:"type"`
}

func unix(f float64) time.Time { return time.Unix(int64(f), 0) }

// Verify checks token and returns the identity it carries.
func (v *Verifier) Verify(ctx context.Context, token string) (Identity, error) {
	if token == "" {
		return Identity{}, ErrMissingToken
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, fmt.Errorf("%w: not a compact JWT", ErrInvalidToken)
	}
	var h header
	if err := decodeSegment(parts[0], &h); err != nil {
		return Identity{}, fmt.Errorf("%w: header", ErrInvalidToken)
	}
	if h.Alg != "RS256" {
		return Identity{}, fmt.Errorf("%w: algorithm %q not allowed", ErrInvalidToken, h.Alg)
	}
	if h.Kid == "" {
		return Identity{}, fmt.Errorf("%w: missing key id", ErrInvalidToken)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) == 0 {
		return Identity{}, fmt.Errorf("%w: signature encoding", ErrInvalidToken)
	}
	key, err := v.key(ctx, h.Kid)
	if err != nil {
		return Identity{}, err
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
		return Identity{}, fmt.Errorf("%w: signature", ErrInvalidToken)
	}

	var c claims
	if err := decodeSegment(parts[1], &c); err != nil {
		return Identity{}, fmt.Errorf("%w: claims", ErrInvalidToken)
	}
	now := v.now()
	if c.Exp == nil {
		return Identity{}, fmt.Errorf("%w: no exp", ErrInvalidToken)
	}
	if !now.Before(unix(*c.Exp).Add(v.skew)) {
		return Identity{}, ErrExpired
	}
	if c.Nbf != nil && now.Add(v.skew).Before(unix(*c.Nbf)) {
		return Identity{}, ErrExpired
	}
	if c.Iat != nil && now.Add(v.skew).Before(unix(*c.Iat)) {
		return Identity{}, ErrExpired
	}
	if c.Iss != v.issuer {
		return Identity{}, ErrIssuer
	}
	aud, err := audiences(c.Aud)
	if err != nil {
		return Identity{}, fmt.Errorf("%w: aud", ErrInvalidToken)
	}
	if !v.audienceOK(aud) {
		return Identity{}, ErrAudience
	}
	if c.Sub == "" && c.Email == "" {
		return Identity{}, fmt.Errorf("%w: no subject", ErrInvalidToken)
	}
	id := Identity{Subject: c.Sub, Email: c.Email, Issuer: c.Iss, Audience: aud, Type: c.Type, Expires: unix(*c.Exp)}
	if c.Iat != nil {
		id.IssuedAt = unix(*c.Iat)
	}
	return id, nil
}

func (v *Verifier) audienceOK(got []string) bool {
	for _, g := range got {
		for _, want := range v.audience {
			if g == want {
				return true
			}
		}
	}
	return false
}

func audiences(raw json.RawMessage) ([]string, error) {
	if len(raw) == 0 {
		return nil, errors.New("missing")
	}
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return []string{one}, nil
	}
	var many []string
	if err := json.Unmarshal(raw, &many); err != nil {
		return nil, err
	}
	return many, nil
}

func decodeSegment(seg string, into any) error {
	b, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, into)
}

// key returns the signing key for kid, refetching the JWKS at most once per
// MinRefresh when the key is not cached (key rotation).
func (v *Verifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if k, ok := v.keys[kid]; ok {
		return k, nil
	}
	if !v.hasSet || v.now().Sub(v.fetched) >= v.minGap {
		if err := v.refreshLocked(ctx); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrKeysUnavailable, err)
		}
		if k, ok := v.keys[kid]; ok {
			return k, nil
		}
	}
	return nil, ErrUnknownKey
}

func (v *Verifier) refreshLocked(ctx context.Context) error {
	// Record the attempt first so a failing endpoint is not hammered.
	v.fetched = v.now()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.url, nil)
	if err != nil {
		return err
	}
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(body, &set); err != nil {
		return err
	}
	keys := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kty != "RSA" || k.Kid == "" || (k.Use != "" && k.Use != "sig") {
			continue
		}
		nb, err1 := base64.RawURLEncoding.DecodeString(k.N)
		eb, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if err1 != nil || err2 != nil || len(nb) < 256 || len(eb) == 0 || len(eb) > 4 {
			continue
		}
		e := int(new(big.Int).SetBytes(eb).Int64())
		if e < 3 {
			continue
		}
		keys[k.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(nb), E: e}
	}
	if len(keys) == 0 {
		return errors.New("jwks has no usable keys")
	}
	// Replace the set: a rotated-out key must stop verifying.
	v.keys = keys
	v.hasSet = true
	return nil
}
