// Package refservice is a complete example service. It exposes a card CRUD
// API plus an SSE event stream and wires in the library's contract packages:
//
//   - reqid: X-Request-ID on every response (problem "instance")
//   - problem: every error is application/problem+json with a catalogue code
//   - ratelimit: RateLimit / RateLimit-Policy headers and 429 problems
//   - bearer: Authorization parsing and scope enforcement (optional)
//   - webhooks: Standard Webhooks signed delivery of card events (optional)
//   - sse and outbox: realtime stream and event persistence (optional)
//
// Every dependency except the store is optional; the service degrades
// gracefully when one is nil.
package refservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/tcpsystems/plinth/bearer"
	"github.com/tcpsystems/plinth/outbox"
	"github.com/tcpsystems/plinth/problem"
	"github.com/tcpsystems/plinth/ratelimit"
	"github.com/tcpsystems/plinth/reqid"
	"github.com/tcpsystems/plinth/sse"
	"github.com/tcpsystems/plinth/webhooks"
)

// Scopes required by the routes (<product>:<resource>:<action>).
const (
	ScopeRead  = "ref:cards:read"
	ScopeWrite = "ref:cards:write"
)

// ── Domain ────────────────────────────────────────────────────────────

// Card is a lightweight card entity.
type Card struct {
	ID        string    `json:"id"`
	Kind      string    `json:"kind"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CardCreate is the request body for creating a card.
type CardCreate struct {
	Kind string `json:"kind"`
}

// Store is a thread-safe in-memory card store.
type Store struct {
	mu     sync.RWMutex
	cards  map[string]Card
	seq    int64
	nextID int64
}

// NewStore returns an empty in-memory store.
func NewStore() *Store {
	return &Store{cards: make(map[string]Card)}
}

// List returns all cards.
func (s *Store) List() []Card {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Card, 0, len(s.cards))
	for _, c := range s.cards {
		out = append(out, c)
	}
	return out
}

// Create adds a card and returns it.
func (s *Store) Create(kind string) (Card, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := fmt.Sprintf("card-%d", s.nextID)
	s.nextID++
	now := time.Now().UTC()
	card := Card{
		ID:        id,
		Kind:      kind,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.cards[id] = card
	s.seq++
	return card, nil
}

// Get returns a card by ID.
func (s *Store) Get(id string) (Card, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	c, ok := s.cards[id]
	return c, ok
}

// Delete removes a card by ID. Returns true if found.
func (s *Store) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.cards[id]
	if ok {
		delete(s.cards, id)
		s.seq++
	}
	return ok
}

// ── Server ────────────────────────────────────────────────────────────

// Options configures the optional parts of a Server.
type Options struct {
	// Limiter, when set, rate-limits every route except /healthz.
	Limiter *ratelimit.Limiter
	// Authorize, when set, makes the routes require a bearer token. It
	// maps the token to its granted scopes; ok=false answers 401.
	Authorize func(token string) (scopes []string, ok bool)
	// WebhookURL and WebhookSecret, when both set, receive a signed POST for
	// every card.created and card.deleted event.
	WebhookURL    string
	WebhookSecret []byte
	// HTTPClient delivers webhooks (default: 5s timeout client).
	HTTPClient *http.Client
}

// Server is the reference HTTP service.
type Server struct {
	store  *Store
	broker *sse.Broker
	ob     outbox.Publisher
	opts   Options
	log    *slog.Logger
	wg     sync.WaitGroup
}

// NewServer creates a Server. broker and ob may be nil.
func NewServer(store *Store, broker *sse.Broker, ob outbox.Publisher, opts ...Options) *Server {
	s := &Server{store: store, broker: broker, ob: ob, log: slog.Default()}
	if len(opts) > 0 {
		s.opts = opts[0]
	}
	if s.opts.HTTPClient == nil {
		s.opts.HTTPClient = &http.Client{Timeout: 5 * time.Second}
	}
	return s
}

// Wait blocks until in-flight webhook deliveries finish (for tests/shutdown).
func (s *Server) Wait() { s.wg.Wait() }

// Handler returns the http.Handler for all routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.Handle("GET /v1/cards", s.guard(ScopeRead, s.listCards))
	mux.Handle("POST /v1/cards", s.guard(ScopeWrite, s.createCard))
	mux.Handle("GET /v1/cards/{id}", s.guard(ScopeRead, s.getCard))
	mux.Handle("DELETE /v1/cards/{id}", s.guard(ScopeWrite, s.deleteCard))
	mux.Handle("GET /v1/events", s.guard(ScopeRead, s.events))
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.Handle("/", problem.NotFoundHandler())

	var h http.Handler = mux
	if l := s.opts.Limiter; l != nil {
		limited := l.Middleware(h)
		h = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/healthz" {
				mux.ServeHTTP(w, r)
				return
			}
			limited.ServeHTTP(w, r)
		})
	}
	return reqid.Middleware(h)
}

// guard enforces bearer authentication and the scope when Authorize is set.
func (s *Server) guard(scope string, next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.opts.Authorize != nil {
			tok, ok := bearer.Token(r)
			var scopes []string
			if ok {
				scopes, ok = s.opts.Authorize(tok)
			}
			if !ok {
				w.Header().Set("WWW-Authenticate", "Bearer")
				problem.WriteError(w, r, problem.New(problem.ErrUnauthorized, "", "missing or invalid bearer token"))
				return
			}
			if !bearer.Require(w, r, scopes, scope) {
				return
			}
		}
		next(w, r)
	})
}

// ── Routes ────────────────────────────────────────────────────────────

func (s *Server) listCards(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"cards": s.store.List()})
}

func (s *Server) createCard(w http.ResponseWriter, r *http.Request) {
	var req CardCreate
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		problem.WriteError(w, r, problem.New(problem.ErrInvalid, "", "request body must be a JSON object"))
		return
	}
	if req.Kind == "" {
		problem.WriteError(w, r, problem.New(problem.ErrInvalid, "", "kind is required"))
		return
	}

	card, err := s.store.Create(req.Kind)
	if err != nil {
		// Unknown errors map to a generic 500 with no detail.
		s.log.Error("create card", "error", err, "request_id", reqid.FromContext(r.Context()))
		problem.WriteError(w, r, err)
		return
	}

	payload, _ := json.Marshal(card)
	s.publish(r.Context(), "card.created", card.ID, payload)

	w.Header().Set("Location", "/v1/cards/"+card.ID)
	writeJSON(w, http.StatusCreated, card)
}

func (s *Server) getCard(w http.ResponseWriter, r *http.Request) {
	card, ok := s.store.Get(r.PathValue("id"))
	if !ok {
		problem.WriteError(w, r, problem.New(problem.ErrNotFound, "", "card not found"))
		return
	}
	writeJSON(w, http.StatusOK, card)
}

func (s *Server) deleteCard(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !s.store.Delete(id) {
		problem.WriteError(w, r, problem.New(problem.ErrNotFound, "", "card not found"))
		return
	}
	s.publish(r.Context(), "card.deleted", id, []byte(`{}`))
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	if s.broker == nil {
		problem.Write(w, r, problem.Problem{
			Type: problem.Type(problem.CodeUnavailable), Title: "Service Unavailable",
			Status: http.StatusServiceUnavailable, Code: problem.CodeUnavailable,
			Detail: "event stream is not configured",
		})
		return
	}
	(&sse.Handler{Broker: s.broker}).ServeHTTP(w, r)
}

func (s *Server) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// publish persists the event to the outbox and delivers the signed webhook.
func (s *Server) publish(ctx context.Context, topic, key string, payload []byte) {
	if s.ob != nil {
		if err := s.ob.Publish(ctx, outbox.Event{Topic: topic, Key: key, Payload: payload}); err != nil {
			s.log.Error("outbox publish", "topic", topic, "error", err)
		}
	}
	if s.opts.WebhookURL == "" || len(s.opts.WebhookSecret) == 0 {
		return
	}
	body, _ := json.Marshal(map[string]any{"type": topic, "data": json.RawMessage(payload)})
	id := fmt.Sprintf("msg_%s_%d", key, time.Now().UnixNano())
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.deliver(id, body); err != nil {
			s.log.Warn("webhook delivery failed", "topic", topic, "error", err)
		}
	}()
}

func (s *Server) deliver(id string, body []byte) error {
	req, err := http.NewRequest(http.MethodPost, s.opts.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	webhooks.SetHeaders(req.Header, s.opts.WebhookSecret, id, time.Now(), body)
	resp, err := s.opts.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return errors.New("webhook endpoint answered " + resp.Status)
	}
	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
