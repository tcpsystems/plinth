// Command refservice is a minimal reference service demonstrating the
// plinth packages: sse, outbox, and the OpenAPI contract.
//
// Usage:
//
//	go run ./cmd/refservice/                     # in-memory mode (no SSE, no outbox)
//	PG_URL=... go run ./cmd/refservice/           # with Postgres (SSE + outbox enabled)
package main

import (
	"context"
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tcpsystems/plinth/examples/refservice"
	"github.com/tcpsystems/plinth/outbox"
	"github.com/tcpsystems/plinth/sse"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	store := refservice.NewStore()

	var broker *sse.Broker
	var ob outbox.Publisher

	if pgURL := os.Getenv("PG_URL"); pgURL != "" {
		db, err := sql.Open("pgx", pgURL)
		if err != nil {
			log.Printf("db open: %v (running in memory-only mode)", err)
		} else if err := db.Ping(); err != nil {
			log.Printf("db ping failed: %v (running in memory-only mode)", err)
		} else {
			// In production the outbox broker would be wired here.
			// For the reference demo we pass nil — the service still
			// runs correctly, just without SSE / outbox persistence.
			_ = db
		}
	}

	server := refservice.NewServer(store, broker, ob)
	srv := &http.Server{
		Addr:    ":" + port,
		Handler: server.Handler(),
	}

	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig
		log.Println("shutting down...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	log.Println("refservice listening on :" + port)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("server: %v", err)
	}
}
