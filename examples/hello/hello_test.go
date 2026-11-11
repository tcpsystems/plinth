package main

import (
	"net/http/httptest"
	"testing"

	"github.com/tcpsystems/plinth/conformance"
	"github.com/tcpsystems/plinth/health"
	"github.com/tcpsystems/plinth/idem"
)

func TestConformance(t *testing.T) {
	logger, logs := conformance.CaptureLogger()
	ready := health.NewRegistry()
	ready.Add("config", health.ConfigLoaded(func() bool { return true }))
	srv := httptest.NewServer(New(logger, idem.NewMemoryStore(), ready))
	defer srv.Close()
	conformance.Run(t, conformance.Target{BaseURL: srv.URL, Features: conformance.Features{
		PaginationPath: "/v1/items", IdempotentPath: "/v1/items", Logs: logs, SchemaVersion: schemaVersion,
	}})
}

func TestPaginationWalksAllItems(t *testing.T) {
	logger, _ := conformance.CaptureLogger()
	srv := httptest.NewServer(New(logger, idem.NewMemoryStore(), health.NewRegistry()))
	defer srv.Close()
	seen, token := 0, ""
	for i := 0; i < 10; i++ {
		var out struct {
			Items []item `json:"items"`
			Next  string `json:"next_page_token"`
		}
		get(t, srv.URL+"/v1/items?page_size=50&page_token="+token, &out)
		seen += len(out.Items)
		if token = out.Next; token == "" {
			break
		}
	}
	if seen != 120 {
		t.Fatalf("walked %d items, want 120", seen)
	}
}
