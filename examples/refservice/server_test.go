package refservice

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/problem"
	"github.com/tcpsystems/plinth/ratelimit"
	"github.com/tcpsystems/plinth/webhooks"
)

func decodeProblem(t *testing.T, rr *httptest.ResponseRecorder, status int, code string) problem.Problem {
	t.Helper()
	if rr.Code != status {
		t.Fatalf("status = %d, want %d", rr.Code, status)
	}
	if ct := rr.Header().Get("Content-Type"); ct != problem.ContentType {
		t.Fatalf("content-type = %q, want %q", ct, problem.ContentType)
	}
	var p problem.Problem
	if err := json.NewDecoder(rr.Body).Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.Code != code || p.Type != problem.Type(code) || p.Status != status {
		t.Fatalf("problem = %+v, want code %q status %d", p, code, status)
	}
	if p.Instance == "" || p.Instance != rr.Header().Get("X-Request-ID") {
		t.Fatalf("instance %q does not match X-Request-ID %q", p.Instance, rr.Header().Get("X-Request-ID"))
	}
	return p
}

func TestServer(t *testing.T) {
	store := NewStore()
	srv := NewServer(store, nil, nil)
	h := srv.Handler()

	t.Run("healthz", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("healthz status = %d, want %d", rr.Code, http.StatusOK)
		}
		var body map[string]string
		if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
			t.Fatalf("decode healthz: %v", err)
		}
		if body["status"] != "ok" {
			t.Errorf("healthz status = %q, want %q", body["status"], "ok")
		}
	})

	t.Run("list empty", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/cards", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("list status = %d, want %d", rr.Code, http.StatusOK)
		}
		var body map[string]any
		if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
			t.Fatalf("decode list: %v", err)
		}
		cards, ok := body["cards"].([]any)
		if !ok {
			t.Fatal("list response missing cards array")
		}
		if len(cards) != 0 {
			t.Errorf("list cards count = %d, want 0", len(cards))
		}
	})

	t.Run("create", func(t *testing.T) {
		body := `{"kind":"standard"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/cards", bytes.NewReader([]byte(body)))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d", rr.Code, http.StatusCreated)
		}
		var card Card
		if err := json.NewDecoder(rr.Body).Decode(&card); err != nil {
			t.Fatalf("decode card: %v", err)
		}
		if card.Kind != "standard" {
			t.Errorf("card kind = %q, want %q", card.Kind, "standard")
		}
		if card.ID == "" {
			t.Error("id is empty")
		}
	})

	t.Run("get", func(t *testing.T) {
		// Create first
		body := `{"kind":"premium"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/cards", bytes.NewReader([]byte(body)))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		var card Card
		json.NewDecoder(rr.Body).Decode(&card)

		// Get it
		req = httptest.NewRequest(http.MethodGet, "/v1/cards/"+card.ID, nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Errorf("get status = %d, want %d", rr.Code, http.StatusOK)
		}
		var got Card
		json.NewDecoder(rr.Body).Decode(&got)
		if got.Kind != "premium" {
			t.Errorf("get kind = %q, want %q", got.Kind, "premium")
		}
	})

	t.Run("get not found", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/cards/nonexistent", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Errorf("get not found status = %d, want %d", rr.Code, http.StatusNotFound)
		}
		decodeProblem(t, rr, http.StatusNotFound, problem.CodeNotFound)
	})

	t.Run("delete", func(t *testing.T) {
		// Create first
		body := `{"kind":"basic"}`
		req := httptest.NewRequest(http.MethodPost, "/v1/cards", bytes.NewReader([]byte(body)))
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		var card Card
		json.NewDecoder(rr.Body).Decode(&card)

		// Delete it
		req = httptest.NewRequest(http.MethodDelete, "/v1/cards/"+card.ID, nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusNoContent {
			t.Errorf("delete status = %d, want %d", rr.Code, http.StatusNoContent)
		}

		// Verify gone
		req = httptest.NewRequest(http.MethodGet, "/v1/cards/"+card.ID, nil)
		rr = httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusNotFound {
			t.Errorf("get after delete status = %d, want %d", rr.Code, http.StatusNotFound)
		}
	})

	t.Run("events unavailable", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("events status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
		}
		decodeProblem(t, rr, http.StatusServiceUnavailable, problem.CodeUnavailable)
	})

	t.Run("bad request", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/v1/cards", bytes.NewReader([]byte("not json")))
		req.Header.Set("Content-Type", "application/json")
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		if rr.Code != http.StatusBadRequest {
			t.Errorf("bad request status = %d, want %d", rr.Code, http.StatusBadRequest)
		}
		p := decodeProblem(t, rr, http.StatusBadRequest, problem.CodeInvalid)
		if strings.Contains(p.Detail, "invalid character") {
			t.Errorf("detail leaks decoder text: %q", p.Detail)
		}
	})

	t.Run("unknown route", func(t *testing.T) {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/nope", nil))
		decodeProblem(t, rr, http.StatusNotFound, problem.CodeNotFound)
	})
}

func TestRequestIDEchoed(t *testing.T) {
	h := NewServer(NewStore(), nil, nil).Handler()
	req := httptest.NewRequest(http.MethodGet, "/v1/cards/x", nil)
	req.Header.Set("X-Request-ID", "req-123")
	req.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	p := decodeProblem(t, rr, http.StatusNotFound, problem.CodeNotFound)
	if p.Instance != "req-123" || p.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Errorf("instance/trace = %q/%q", p.Instance, p.TraceID)
	}
}

func TestRateLimit(t *testing.T) {
	l := &ratelimit.Limiter{Limit: 2, Window: time.Minute, Key: func(*http.Request) string { return "k" }}
	h := NewServer(NewStore(), nil, nil, Options{Limiter: l}).Handler()
	get := func(path string) *httptest.ResponseRecorder {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		return rr
	}
	for i := 0; i < 2; i++ {
		rr := get("/v1/cards")
		if rr.Code != 200 || rr.Header().Get("RateLimit") == "" || rr.Header().Get("RateLimit-Policy") == "" {
			t.Fatalf("req %d: code %d headers %v", i, rr.Code, rr.Header())
		}
	}
	rr := get("/v1/cards")
	decodeProblem(t, rr, http.StatusTooManyRequests, problem.CodeRateLimited)
	if rr.Header().Get("Retry-After") == "" {
		t.Error("missing Retry-After")
	}
	if get("/healthz").Code != 200 {
		t.Error("healthz must not be rate limited")
	}
}

func TestScopes(t *testing.T) {
	auth := func(tok string) ([]string, bool) {
		switch tok {
		case "reader":
			return []string{ScopeRead}, true
		case "admin":
			return []string{"ref:cards:*"}, true
		}
		return nil, false
	}
	h := NewServer(NewStore(), nil, nil, Options{Authorize: auth}).Handler()
	do := func(method, path, tok, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if tok != "" {
			req.Header.Set("Authorization", "Bearer "+tok)
		}
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
		return rr
	}
	decodeProblem(t, do("GET", "/v1/cards", "", ""), 401, problem.CodeUnauthorized)
	decodeProblem(t, do("GET", "/v1/cards", "bogus", ""), 401, problem.CodeUnauthorized)
	if rr := do("GET", "/v1/cards", "reader", ""); rr.Code != 200 {
		t.Fatalf("reader list = %d", rr.Code)
	}
	decodeProblem(t, do("POST", "/v1/cards", "reader", `{"kind":"a"}`), 403, problem.CodeForbidden)
	if rr := do("POST", "/v1/cards", "admin", `{"kind":"a"}`); rr.Code != 201 {
		t.Fatalf("admin create = %d", rr.Code)
	}
	if rr := do("GET", "/healthz", "", ""); rr.Code != 200 {
		t.Fatalf("healthz needs no auth, got %d", rr.Code)
	}
}

func TestWebhookDelivery(t *testing.T) {
	secret := []byte("whsec-test")
	got := make(chan error, 1)
	recv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got <- webhooks.Verify(r.Header, body, time.Now(), secret)
	}))
	defer recv.Close()
	srv := NewServer(NewStore(), nil, nil, Options{WebhookURL: recv.URL, WebhookSecret: secret})
	rr := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/cards", strings.NewReader(`{"kind":"a"}`)))
	if rr.Code != 201 {
		t.Fatalf("create = %d", rr.Code)
	}
	srv.Wait()
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("webhook verify: %v", err)
		}
	default:
		t.Fatal("no webhook delivered")
	}
}
