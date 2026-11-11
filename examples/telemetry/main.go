// Command telemetry demonstrates HTTP tracing, metrics and correlated logs.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/tcpsystems/plinth/httpx"
	"github.com/tcpsystems/plinth/logx"
	"github.com/tcpsystems/plinth/mw"
	"github.com/tcpsystems/plinth/reqid"
	"github.com/tcpsystems/plinth/telemetry"
)

func main() {
	if err := serve(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func serve() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	stop, err := telemetry.Setup(ctx, telemetry.Options{ServiceName: "telemetry-example", SampleRatio: 1})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := stop(shutdownCtx); err != nil {
			slog.Error("telemetry shutdown failed", "error", err)
		}
	}()
	logger := telemetry.Logger(nil)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /hello/{name}", func(w http.ResponseWriter, r *http.Request) {
		logger.InfoContext(r.Context(), "hello")
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"hello": r.PathValue("name")})
	})
	handler := mw.Chain(mux, reqid.Middleware, telemetry.Middleware("telemetry-example"), mw.Recover(logger), func(h http.Handler) http.Handler { return logx.Middleware(logger, h) })
	listener, err := net.Listen("tcp", "127.0.0.1:8080")
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	stopped := make(chan error, 1)
	go func() { stopped <- srv.Serve(listener) }()
	logger.Info("listening", "address", listener.Addr().String())
	select {
	case err := <-stopped:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		return srv.Shutdown(shutdownCtx)
	}
	return nil
}
