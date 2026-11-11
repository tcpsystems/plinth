package logx

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tcpsystems/plinth/reqid"
)

func TestLoggingAndStreaming(t *testing.T) {
	for _, mode := range []string{"empty", "body", "explicit", "stream", "early-hints", "panic"} {
		t.Run(mode, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, nil))
			h := reqid.Middleware(Middleware(logger, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "body":
					fmt.Fprint(w, "abc")
				case "explicit":
					w.WriteHeader(201)
					w.WriteHeader(403)
					fmt.Fprint(w, "abc")
				case "stream":
					if err := http.NewResponseController(w).Flush(); err != nil {
						t.Fatal(err)
					}
					fmt.Fprint(w, "event\n\n")
				case "early-hints":
					w.WriteHeader(103)
					w.WriteHeader(202)
				case "panic":
					panic("handler failure")
				}
			})))
			r := httptest.NewRequest("GET", "/public?token=secret-query", nil)
			r.Header.Set("Authorization", "Bearer secret-header")
			r.Header.Set(reqid.Header, "trace-1")
			w := httptest.NewRecorder()
			panicked := false
			func() {
				defer func() {
					if recover() != nil {
						panicked = true
					}
				}()
				h.ServeHTTP(w, r)
			}()
			if panicked != (mode == "panic") {
				t.Fatal("panic behavior changed")
			}
			var entry map[string]any
			if err := json.Unmarshal(buf.Bytes(), &entry); err != nil {
				t.Fatal(err)
			}
			status := 200
			switch mode {
			case "explicit":
				status = 201
			case "early-hints":
				status = 202
			case "panic":
				status = 500
			}
			if entry["status"] != float64(status) || entry["request_id"] != "trace-1" || entry["path"] != "/public" {
				t.Fatal(entry)
			}
			if strings.Contains(buf.String(), "secret") {
				t.Fatal("credential logged")
			}
			if mode == "stream" && !w.Flushed {
				t.Fatal("stream was buffered")
			}
			if entry["bytes"] != float64(w.Body.Len()) {
				t.Fatal("wrong byte count")
			}
		})
	}
}

func TestUnsupportedHijackAndController(t *testing.T) {
	w := &writer{ResponseWriter: httptest.NewRecorder()}
	if w.Unwrap() == nil {
		t.Fatal("missing unwrap")
	}
	if _, _, err := w.Hijack(); !errors.Is(err, http.ErrNotSupported) {
		t.Fatal(err)
	}
}
