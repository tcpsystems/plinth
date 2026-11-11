// Command hello is a complete service built from plinth packages. It is the
// reference for the shared HTTP contract and is exercised by conformance.Run.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"

	"github.com/tcpsystems/plinth/health"
	"github.com/tcpsystems/plinth/idem"
	"github.com/tcpsystems/plinth/logx"
	"github.com/tcpsystems/plinth/page"
	"github.com/tcpsystems/plinth/problem"
	"github.com/tcpsystems/plinth/reqid"
	"github.com/tcpsystems/plinth/service"
)

const schemaVersion = 1

type item struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type cursor struct {
	After int `json:"after"`
}

type app struct {
	items  []item // sorted by id; a real service reads these with page.Keyset
	signer *page.Signer
}

func (a *app) list(w http.ResponseWriter, r *http.Request) {
	var c cursor
	size, _, err := page.Parse(r, a.signer, &c)
	if err != nil {
		problem.WriteError(w, r, err)
		return
	}
	var rows []item
	for _, it := range a.items {
		if it.ID > c.After && len(rows) < page.Limit(size) {
			rows = append(rows, it)
		}
	}
	rows, more := page.Trim(rows, size)
	resp := page.Response[item]{Items: rows}
	if more {
		resp.NextPageToken, _ = a.signer.Encode(cursor{After: rows[len(rows)-1].ID})
	}
	page.Write(w, r, resp)
}

func (a *app) create(w http.ResponseWriter, r *http.Request) {
	var in struct{ Name string }
	if json.NewDecoder(r.Body).Decode(&in) != nil {
		problem.WriteError(w, r, problem.New(problem.ErrInvalid, "invalid_body", "body must be a JSON object"))
		return
	}
	writeJSON(w, http.StatusCreated, item{ID: 1000 + len(in.Name), Name: in.Name})
}

func (a *app) get(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.Atoi(r.PathValue("id"))
	if err != nil || id < 1 || id > len(a.items) {
		problem.WriteError(w, r, problem.New(problem.ErrNotFound, "", "no such item")) // foreign ids look the same
		return
	}
	writeJSON(w, http.StatusOK, a.items[id-1])
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// New assembles the handler: request id, access log, idempotency, routes.
func New(logger *slog.Logger, store idem.Store, ready *health.Registry) http.Handler {
	signer, _ := page.NewSigner([]byte("hello-example-signing-key-change-me"))
	a := &app{signer: signer}
	for i := 1; i <= 120; i++ {
		a.items = append(a.items, item{ID: i, Name: fmt.Sprintf("item-%d", i)})
	}
	mux := http.NewServeMux()
	health.Mount(mux, ready, service.ReadVersion("hello", schemaVersion, map[string]string{"service": "1"}))
	mux.HandleFunc("GET /v1/items", a.list)
	mux.HandleFunc("GET /v1/items/{id}", a.get)
	mux.HandleFunc("POST /v1/items", a.create)
	mux.Handle("/", problem.NotFoundHandler())
	withIdem := idem.Middleware(idem.Options{Store: store})(mux)
	return reqid.Middleware(logx.Middleware(logger, withIdem))
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ready := health.NewRegistry()
	ready.Add("config", health.ConfigLoaded(func() bool { return true }))
	addr := os.Getenv("HELLO_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	h := New(logger, idem.NewMemoryStore(), ready)
	if err := service.Run(context.Background(), service.Config{PublicAddr: addr, Logger: logger}, h); err != nil {
		logger.Error("exit", "err", err)
		os.Exit(1)
	}
}
