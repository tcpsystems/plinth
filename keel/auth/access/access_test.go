package access

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	team = "acme.cloudflareaccess.com"
	aud  = "app-aud-tag"
)

var t0 = time.Unix(1_800_000_000, 0)

type signer struct {
	kid string
	key *rsa.PrivateKey
}

func newSigner(t *testing.T, kid string) signer {
	t.Helper()
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return signer{kid, k}
}

func b64(b []byte) string { return base64.RawURLEncoding.EncodeToString(b) }

func (s signer) sign(t *testing.T, hdr map[string]any, cl map[string]any) string {
	t.Helper()
	if hdr == nil {
		hdr = map[string]any{"alg": "RS256", "kid": s.kid, "typ": "JWT"}
	}
	hb, _ := json.Marshal(hdr)
	cb, _ := json.Marshal(cl)
	in := b64(hb) + "." + b64(cb)
	sum := sha256.Sum256([]byte(in))
	sig, err := rsa.SignPKCS1v15(rand.Reader, s.key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return in + "." + b64(sig)
}

func (s signer) jwk() map[string]any {
	return map[string]any{
		"kty": "RSA", "kid": s.kid, "use": "sig", "alg": "RS256",
		"n": b64(s.key.N.Bytes()), "e": b64(big.NewInt(int64(s.key.E)).Bytes()),
	}
}

func claimsAt(now time.Time) map[string]any {
	return map[string]any{
		"iss": "https://" + team, "aud": []string{aud}, "sub": "user-1", "email": "a@example.com",
		"type": "app", "iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}
}

// jwks serves a mutable key set and counts fetches.
type jwks struct {
	mu    sync.Mutex
	keys  []signer
	fail  bool
	hits  atomic.Int32
	srv   *httptest.Server
	clock atomic.Int64
}

func newJWKS(t *testing.T, keys ...signer) *jwks {
	j := &jwks{keys: keys}
	j.clock.Store(t0.Unix())
	j.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		j.hits.Add(1)
		j.mu.Lock()
		defer j.mu.Unlock()
		if j.fail {
			http.Error(w, "down", http.StatusBadGateway)
			return
		}
		var ks []map[string]any
		for _, k := range j.keys {
			ks = append(ks, k.jwk())
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": ks})
	}))
	t.Cleanup(j.srv.Close)
	return j
}

func (j *jwks) set(fail bool, keys ...signer) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.fail, j.keys = fail, keys
}

func (j *jwks) now() time.Time          { return time.Unix(j.clock.Load(), 0) }
func (j *jwks) advance(d time.Duration) { j.clock.Add(int64(d / time.Second)) }

func verifier(t *testing.T, j *jwks, roles RoleResolver) *Verifier {
	t.Helper()
	v, err := New(Config{Team: team, Audience: []string{aud}, JWKSURL: j.srv.URL, Roles: roles, Now: j.now})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestValidToken(t *testing.T) {
	s := newSigner(t, "k1")
	j := newJWKS(t, s)
	v := verifier(t, j, nil)
	id, err := v.Verify(context.Background(), s.sign(t, nil, claimsAt(t0)))
	if err != nil {
		t.Fatal(err)
	}
	if id.Subject != "user-1" || id.Email != "a@example.com" || id.Type != "app" || id.Issuer != "https://"+team {
		t.Fatalf("identity = %+v", id)
	}
}

func TestAudienceAsStringAndArray(t *testing.T) {
	s := newSigner(t, "k1")
	v := verifier(t, newJWKS(t, s), nil)
	c := claimsAt(t0)
	c["aud"] = aud
	if _, err := v.Verify(context.Background(), s.sign(t, nil, c)); err != nil {
		t.Fatalf("string aud: %v", err)
	}
	c["aud"] = []string{"other", aud}
	if _, err := v.Verify(context.Background(), s.sign(t, nil, c)); err != nil {
		t.Fatalf("array aud: %v", err)
	}
}

func TestRejections(t *testing.T) {
	s := newSigner(t, "k1")
	other := newSigner(t, "k1") // same kid, different key
	j := newJWKS(t, s)
	v := verifier(t, j, nil)
	mut := func(f func(map[string]any)) map[string]any {
		c := claimsAt(t0)
		f(c)
		return c
	}
	cases := []struct {
		name string
		tok  string
		want error
	}{
		{"expired", s.sign(t, nil, mut(func(c map[string]any) { c["exp"] = t0.Add(-time.Minute).Unix() })), ErrExpired},
		{"not yet valid", s.sign(t, nil, mut(func(c map[string]any) { c["nbf"] = t0.Add(time.Hour).Unix() })), ErrExpired},
		{"issued in future", s.sign(t, nil, mut(func(c map[string]any) { c["iat"] = t0.Add(time.Hour).Unix() })), ErrExpired},
		{"no exp", s.sign(t, nil, mut(func(c map[string]any) { delete(c, "exp") })), ErrInvalidToken},
		{"wrong audience", s.sign(t, nil, mut(func(c map[string]any) { c["aud"] = []string{"nope"} })), ErrAudience},
		{"no audience", s.sign(t, nil, mut(func(c map[string]any) { delete(c, "aud") })), ErrInvalidToken},
		{"wrong issuer", s.sign(t, nil, mut(func(c map[string]any) { c["iss"] = "https://evil.cloudflareaccess.com" })), ErrIssuer},
		{"no subject", s.sign(t, nil, mut(func(c map[string]any) { delete(c, "sub"); delete(c, "email") })), ErrInvalidToken},
		{"alg none", s.sign(t, map[string]any{"alg": "none", "kid": "k1"}, claimsAt(t0)), ErrInvalidToken},
		{"alg HS256", s.sign(t, map[string]any{"alg": "HS256", "kid": "k1"}, claimsAt(t0)), ErrInvalidToken},
		{"no kid", s.sign(t, map[string]any{"alg": "RS256"}, claimsAt(t0)), ErrInvalidToken},
		{"forged signature", other.sign(t, nil, claimsAt(t0)), ErrInvalidToken},
		{"unknown kid", newSigner(t, "zzz").sign(t, nil, claimsAt(t0)), ErrUnknownKey},
		{"empty", "", ErrMissingToken},
		{"garbage", "a.b", ErrInvalidToken},
		{"bad base64", "!!.!!.!!", ErrInvalidToken},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := v.Verify(context.Background(), c.tok)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestClockSkew(t *testing.T) {
	s := newSigner(t, "k1")
	j := newJWKS(t, s)
	v := verifier(t, j, nil) // 30s skew
	c := claimsAt(t0)
	c["exp"] = t0.Add(-10 * time.Second).Unix() // expired 10s ago: inside skew
	if _, err := v.Verify(context.Background(), s.sign(t, nil, c)); err != nil {
		t.Fatalf("within skew: %v", err)
	}
	c["exp"] = t0.Add(-31 * time.Second).Unix()
	if _, err := v.Verify(context.Background(), s.sign(t, nil, c)); !errors.Is(err, ErrExpired) {
		t.Fatalf("beyond skew: %v", err)
	}
	c = claimsAt(t0)
	c["nbf"] = t0.Add(20 * time.Second).Unix() // issuer clock slightly ahead
	c["iat"] = t0.Add(20 * time.Second).Unix()
	if _, err := v.Verify(context.Background(), s.sign(t, nil, c)); err != nil {
		t.Fatalf("nbf within skew: %v", err)
	}
	if _, err := New(Config{Team: team, Audience: []string{aud}, Skew: time.Hour}); err == nil {
		t.Fatal("oversized skew accepted")
	}
}

func TestJWKSRotation(t *testing.T) {
	oldK, newK := newSigner(t, "old"), newSigner(t, "new")
	j := newJWKS(t, oldK)
	v := verifier(t, j, nil)
	ctx := context.Background()
	if _, err := v.Verify(ctx, oldK.sign(t, nil, claimsAt(j.now()))); err != nil {
		t.Fatal(err)
	}
	// Edge rotates: new key published, old retired.
	j.set(false, newK)
	tok := newK.sign(t, nil, claimsAt(j.now()))
	// Within the cooldown an unknown kid does not refetch.
	if _, err := v.Verify(ctx, tok); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("inside cooldown: %v", err)
	}
	if j.hits.Load() != 1 {
		t.Fatalf("fetches = %d, want 1", j.hits.Load())
	}
	j.advance(time.Minute)
	if _, err := v.Verify(ctx, newK.sign(t, nil, claimsAt(j.now()))); err != nil {
		t.Fatalf("after rotation: %v", err)
	}
	// Retired key stops verifying once the set was replaced.
	if _, err := v.Verify(ctx, oldK.sign(t, nil, claimsAt(j.now()))); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("retired key: %v", err)
	}
}

func TestJWKSOutageKeepsCachedKeysAndReportsUnavailable(t *testing.T) {
	s := newSigner(t, "k1")
	j := newJWKS(t, s)
	v := verifier(t, j, nil)
	ctx := context.Background()
	if _, err := v.Verify(ctx, s.sign(t, nil, claimsAt(j.now()))); err != nil {
		t.Fatal(err)
	}
	j.set(true)
	j.advance(time.Minute)
	if _, err := v.Verify(ctx, s.sign(t, nil, claimsAt(j.now()))); err != nil {
		t.Fatalf("cached key during outage: %v", err)
	}
	newK := newSigner(t, "n")
	if _, err := v.Verify(ctx, newK.sign(t, nil, claimsAt(j.now()))); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("unknown kid during outage: %v", err)
	}
	// A fresh verifier with an unreachable JWKS never verifies anything.
	v2 := verifier(t, j, nil)
	if _, err := v2.Verify(ctx, s.sign(t, nil, claimsAt(j.now()))); !errors.Is(err, ErrKeysUnavailable) {
		t.Fatalf("cold start outage: %v", err)
	}
}

func TestNewValidation(t *testing.T) {
	for _, c := range []Config{
		{Audience: []string{aud}},
		{Team: team},
		{Team: team, Audience: []string{""}},
		{Team: "a/b", Audience: []string{aud}},
	} {
		if _, err := New(c); err == nil {
			t.Errorf("New(%+v) accepted", c)
		}
	}
	if _, err := New(Config{Team: "https://" + team + "/", Audience: []string{aud}}); err != nil {
		t.Fatalf("URL-form team: %v", err)
	}
}

func serve(t *testing.T, v *Verifier, req *http.Request) (*httptest.ResponseRecorder, *Principal) {
	t.Helper()
	var got *Principal
	h := v.Middleware(RequireRole("admin", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, _ := FromContext(r.Context())
		got = &p
		w.WriteHeader(http.StatusNoContent)
	})))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, got
}

func TestMiddleware(t *testing.T) {
	s := newSigner(t, "k1")
	j := newJWKS(t, s)
	roles := map[string][]string{"user-1": {"admin"}, "user-2": {"viewer"}, "user-3": nil}
	var storeDown bool
	resolve := func(_ context.Context, id Identity) ([]string, error) {
		if storeDown {
			return nil, errors.New("db down")
		}
		if r, ok := roles[id.Subject]; ok {
			return r, nil
		}
		return nil, ErrNoRole
	}
	v := verifier(t, j, resolve)
	tokFor := func(sub string) string {
		c := claimsAt(t0)
		c["sub"] = sub
		return s.sign(t, nil, c)
	}
	req := func(tok string, cookie bool) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		switch {
		case tok == "":
		case cookie:
			r.AddCookie(&http.Cookie{Name: CookieName, Value: tok})
		default:
			r.Header.Set(HeaderName, tok)
		}
		return r
	}

	t.Run("valid with role via header", func(t *testing.T) {
		rec, p := serve(t, v, req(tokFor("user-1"), false))
		if rec.Code != http.StatusNoContent || p == nil || !p.HasRole("admin") || p.Identity.Subject != "user-1" {
			t.Fatalf("code=%d principal=%+v", rec.Code, p)
		}
	})
	t.Run("valid via cookie", func(t *testing.T) {
		if rec, _ := serve(t, v, req(tokFor("user-1"), true)); rec.Code != http.StatusNoContent {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("missing token is 401 and never reaches handler", func(t *testing.T) {
		rec, p := serve(t, v, req("", false))
		if rec.Code != http.StatusUnauthorized || p != nil || rec.Header().Get("WWW-Authenticate") == "" {
			t.Fatalf("code=%d principal=%v", rec.Code, p)
		}
	})
	t.Run("empty header does not fall back to cookie", func(t *testing.T) {
		r := req(tokFor("user-1"), true)
		r.Header.Set(HeaderName, "")
		if rec, p := serve(t, v, r); rec.Code != http.StatusUnauthorized || p != nil {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("duplicate headers are rejected", func(t *testing.T) {
		r := req(tokFor("user-1"), false)
		r.Header.Add(HeaderName, tokFor("user-1"))
		if rec, p := serve(t, v, r); rec.Code != http.StatusUnauthorized || p != nil {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("expired is 401", func(t *testing.T) {
		c := claimsAt(t0)
		c["exp"] = t0.Add(-time.Hour).Unix()
		if rec, _ := serve(t, v, req(s.sign(t, nil, c), false)); rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("wrong audience is 401", func(t *testing.T) {
		c := claimsAt(t0)
		c["aud"] = []string{"other-app"}
		if rec, _ := serve(t, v, req(s.sign(t, nil, c), false)); rec.Code != http.StatusUnauthorized {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("valid identity without a role is 403", func(t *testing.T) {
		for _, sub := range []string{"user-3", "stranger"} {
			if rec, p := serve(t, v, req(tokFor(sub), false)); rec.Code != http.StatusForbidden || p != nil {
				t.Fatalf("%s: code=%d", sub, rec.Code)
			}
		}
	})
	t.Run("identity does not grant a role it lacks", func(t *testing.T) {
		if rec, p := serve(t, v, req(tokFor("user-2"), false)); rec.Code != http.StatusForbidden || p != nil {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("role store failure is 503, not allow", func(t *testing.T) {
		storeDown = true
		defer func() { storeDown = false }()
		if rec, p := serve(t, v, req(tokFor("user-1"), false)); rec.Code != http.StatusServiceUnavailable || p != nil {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("jwks outage is 503, not anonymous", func(t *testing.T) {
		j2 := newJWKS(t)
		j2.set(true)
		v2 := verifier(t, j2, resolve)
		if rec, p := serve(t, v2, req(tokFor("user-1"), false)); rec.Code != http.StatusServiceUnavailable || p != nil {
			t.Fatalf("code=%d", rec.Code)
		}
	})
	t.Run("responses leak nothing", func(t *testing.T) {
		tok := tokFor("user-1") + "x"
		rec, _ := serve(t, v, req(tok, false))
		body := rec.Body.String()
		if strings.Contains(body, tok) || strings.Contains(body, "signature") || rec.Header().Get("Content-Type") != "application/problem+json" {
			t.Fatalf("body=%q", body)
		}
	})
}

func TestMiddlewareRequiresRoles(t *testing.T) {
	v := verifier(t, newJWKS(t), nil)
	defer func() {
		if recover() == nil {
			t.Fatal("Middleware without Roles did not panic")
		}
	}()
	v.Middleware(http.NotFoundHandler())
}

func TestRequireRoleWithoutPrincipal(t *testing.T) {
	rec := httptest.NewRecorder()
	RequireRole("admin", http.NotFoundHandler()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
}
