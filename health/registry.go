package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// Pinger is satisfied by *sql.DB.
type Pinger interface{ PingContext(context.Context) error }

// Registry holds named readiness checks. It is safe for concurrent use.
type Registry struct {
	mu      sync.RWMutex
	names   []string
	checks  map[string]func(context.Context) error
	Timeout time.Duration // per request budget for all checks; default 3s
}

// NewRegistry returns an empty registry (ready).
func NewRegistry() *Registry {
	return &Registry{checks: map[string]func(context.Context) error{}, Timeout: 3 * time.Second}
}

// Add registers a named check; re-adding a name replaces it.
func (g *Registry) Add(name string, fn func(context.Context) error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.checks[name]; !ok {
		g.names = append(g.names, name)
	}
	g.checks[name] = fn
}

// DB checks that the database answers a ping.
func DB(p Pinger) func(context.Context) error { return p.PingContext }

// ConfigLoaded fails until loaded reports true.
func ConfigLoaded(loaded func() bool) func(context.Context) error {
	return func(context.Context) error {
		if !loaded() {
			return errors.New("config not loaded")
		}
		return nil
	}
}

// SchemaAtLeast fails when the database schema version reported by current is
// older than the version this binary expects. A newer schema passes, so a
// rolling deploy can run the old binary against a migrated database.
func SchemaAtLeast(expected int, current func(context.Context) (int, error)) func(context.Context) error {
	return func(ctx context.Context) error {
		got, err := current(ctx)
		if err != nil {
			return err
		}
		if got < expected {
			return fmt.Errorf("schema version %d older than expected %d", got, expected)
		}
		return nil
	}
}

// Ready serves GET /readyz. It runs every check, in registration order, under
// one timeout. The body names failing checks but never their errors.
func (g *Registry) Ready() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.RLock()
		names := append([]string(nil), g.names...)
		fns := make([]func(context.Context) error, len(names))
		for i, n := range names {
			fns[i] = g.checks[n]
		}
		g.mu.RUnlock()
		ctx, cancel := context.WithTimeout(r.Context(), g.Timeout)
		defer cancel()
		res := map[string]string{}
		status, code := "ok", http.StatusOK
		for i, fn := range fns {
			res[names[i]] = "ok"
			if err := fn(ctx); err != nil {
				res[names[i]] = "fail"
				status, code = "unavailable", http.StatusServiceUnavailable
			}
		}
		writeJSON(w, code, map[string]any{"status": status, "checks": res})
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// VersionHandler serves v (any JSON-marshalable value, normally
// service.Version) as JSON.
func VersionHandler(v any) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, v) })
}

// Mount registers GET /healthz, /readyz and /version on mux.
func Mount(mux *http.ServeMux, g *Registry, version any) {
	mux.HandleFunc("GET /healthz", Liveness)
	mux.Handle("GET /readyz", g.Ready())
	mux.Handle("GET /version", VersionHandler(version))
}
