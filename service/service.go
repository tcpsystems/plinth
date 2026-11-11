// Package service runs an HTTP service with separate public and mesh listeners
// and graceful shutdown on SIGTERM.
package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config configures Run.
type Config struct {
	// PublicAddr and MeshAddr are listen addresses ("host:port"). The mesh
	// listener serves operator and service-to-service traffic and should be
	// bound to the WireGuard address. An empty MeshAddr disables it.
	PublicAddr, MeshAddr string
	// PublicListener and MeshListener override the addresses (tests, socket
	// activation). Run closes them.
	PublicListener, MeshListener net.Listener
	// MeshHandler serves the mesh listener; nil reuses the public handler.
	MeshHandler http.Handler
	// DrainTimeout bounds graceful shutdown; default 25s.
	DrainTimeout time.Duration
	// ReadHeaderTimeout default 5s.
	ReadHeaderTimeout time.Duration
	Logger            *slog.Logger
}

// Run serves until ctx ends or SIGTERM/SIGINT arrives, then stops accepting,
// lets in-flight requests finish for DrainTimeout, and returns. It returns nil
// after a clean shutdown and an error if a listener fails or the drain times out.
func Run(ctx context.Context, cfg Config, public http.Handler) error {
	if public == nil {
		return errors.New("service: nil handler")
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 25 * time.Second
	}
	if cfg.ReadHeaderTimeout <= 0 {
		cfg.ReadHeaderTimeout = 5 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.Default()
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
	defer stop()

	type srv struct {
		name string
		s    *http.Server
		l    net.Listener
	}
	var srvs []srv
	add := func(name string, l net.Listener, addr string, h http.Handler) error {
		if l == nil {
			if addr == "" {
				return nil
			}
			var err error
			if l, err = net.Listen("tcp", addr); err != nil {
				return fmt.Errorf("service: listen %s %q: %w", name, addr, err)
			}
		}
		srvs = append(srvs, srv{name, &http.Server{Handler: h, ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			BaseContext: func(net.Listener) context.Context { return context.WithoutCancel(ctx) }}, l})
		return nil
	}
	meshH := cfg.MeshHandler
	if meshH == nil {
		meshH = public
	}
	err := add("public", cfg.PublicListener, cfg.PublicAddr, public)
	if err == nil {
		err = add("mesh", cfg.MeshListener, cfg.MeshAddr, meshH)
	}
	if err != nil {
		for _, s := range srvs {
			_ = s.l.Close()
		}
		return err
	}
	if len(srvs) == 0 {
		return errors.New("service: no listeners configured")
	}

	errc := make(chan error, len(srvs))
	for _, s := range srvs {
		log.Info("listening", "listener", s.name, "addr", s.l.Addr().String())
		go func() {
			if err := s.s.Serve(s.l); !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("service: %s listener: %w", s.name, err)
			}
		}()
	}
	var failure error
	select {
	case <-ctx.Done():
		log.Info("shutdown requested", "drain_timeout", cfg.DrainTimeout.String())
	case failure = <-errc:
	}
	dctx, cancel := context.WithTimeout(context.Background(), cfg.DrainTimeout)
	defer cancel()
	var wg sync.WaitGroup
	shutErrs := make([]error, len(srvs))
	for i, s := range srvs {
		wg.Add(1)
		go func() { defer wg.Done(); shutErrs[i] = s.s.Shutdown(dctx) }()
	}
	wg.Wait()
	if failure != nil {
		return failure
	}
	return errors.Join(shutErrs...)
}
