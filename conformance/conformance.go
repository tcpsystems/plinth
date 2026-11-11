// Package conformance proves a running service follows the shared HTTP
// contract: probes, RFC 9457 problems, pagination limits, idempotency, trace
// correlation and log scrubbing. Call [Run] from a test against a started server.
//
// Conformance checks for response shape (pagination envelope, X-Request-ID)
// are reported but do not fail the test by default; they surface deviations
// so people and tools can see where a service stands.
package conformance

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

// uuidv7Re matches UUIDv7 format per RFC 9562.
var uuidv7Re = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// Features selects the optional checks. Empty paths skip that group.
type Features struct {
	// PaginationPath is a list endpoint honouring page_size and page_token.
	PaginationPath string
	// IdempotentPath is a POST endpoint behind idem.Middleware (JSON body).
	IdempotentPath string
	// IdempotencyRequired also expects 400 idempotency_key_required without a key.
	IdempotencyRequired bool
	// Logs returns everything the service has logged so far; enables the
	// Authorization scrub check. Use [CaptureLogger] to build one.
	Logs func() string
	// SchemaVersion, when non-zero, is the schema_version /version must report.
	SchemaVersion int
}

// Target is the service under test.
type Target struct {
	BaseURL  string
	Client   *http.Client
	Features Features
}

const traceID = "0af7651916cd43dd8448eb211c80319c"

type resp struct {
	code   int
	header http.Header
	body   []byte
}

func (g Target) do(t *testing.T, method, path string, body string, h map[string]string) resp {
	t.Helper()
	c := g.Client
	if c == nil {
		c = &http.Client{Timeout: 10 * time.Second}
	}
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, strings.TrimRight(g.BaseURL, "/")+path, rd)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range h {
		req.Header.Set(k, v)
	}
	res, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	return resp{res.StatusCode, res.Header, b}
}

type problemBody struct {
	Type, Title, Detail, Instance, Code, TraceID string
	Status                                       int
}

func (g Target) problem(t *testing.T, r resp, status int) problemBody {
	t.Helper()
	if r.code != status {
		t.Fatalf("status %d, want %d: %s", r.code, status, r.body)
	}
	if ct := r.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/problem+json") {
		t.Fatalf("content-type %q, want application/problem+json", ct)
	}
	var raw map[string]any
	if err := json.Unmarshal(r.body, &raw); err != nil {
		t.Fatalf("problem body is not JSON: %v", err)
	}
	var p problemBody
	p.Type, _ = raw["type"].(string)
	p.Title, _ = raw["title"].(string)
	p.Code, _ = raw["code"].(string)
	p.Instance, _ = raw["instance"].(string)
	p.TraceID, _ = raw["trace_id"].(string)
	if s, _ := raw["status"].(float64); int(s) != status {
		t.Errorf("body status %v, want %d", raw["status"], status)
	}
	if p.Code == "" || p.Type != "https://tcp.systems/problems/"+p.Code {
		t.Errorf("type %q / code %q do not follow https://tcp.systems/problems/<code>", p.Type, p.Code)
	}
	if p.Title == "" {
		t.Error("problem has no title")
	}
	if rid := r.header.Get("X-Request-ID"); rid == "" || p.Instance != rid {
		t.Errorf("instance %q must equal X-Request-ID %q", p.Instance, rid)
	}
	for _, bad := range []string{"SELECT ", "INSERT ", "goroutine ", ".go:", "pq:", "sql:"} {
		if strings.Contains(string(r.body), bad) {
			t.Errorf("problem body leaks internals (%q): %s", bad, r.body)
		}
	}
	return p
}

// Run executes every applicable check as subtests of t.
func Run(t *testing.T, g Target) {
	t.Helper()
	f := g.Features
	t.Run("healthz", func(t *testing.T) {
		r := g.do(t, "GET", "/healthz", "", nil)
		var b struct{ Status string }
		if r.code != 200 || json.Unmarshal(r.body, &b) != nil || b.Status != "ok" {
			t.Fatalf("%d %s", r.code, r.body)
		}
	})
	t.Run("readyz", func(t *testing.T) {
		r := g.do(t, "GET", "/readyz", "", nil)
		var b struct {
			Status string
			Checks map[string]string
		}
		if r.code != 200 || json.Unmarshal(r.body, &b) != nil || b.Status != "ok" || b.Checks == nil {
			t.Fatalf("%d %s", r.code, r.body)
		}
		for k, v := range b.Checks {
			if v != "ok" {
				t.Errorf("check %s = %s", k, v)
			}
		}
	})
	t.Run("version", func(t *testing.T) {
		r := g.do(t, "GET", "/version", "", nil)
		var m map[string]any
		if r.code != 200 || json.Unmarshal(r.body, &m) != nil {
			t.Fatalf("%d %s", r.code, r.body)
		}
		for _, k := range []string{"service", "git_sha", "build_time", "schema_version", "contracts", "arch"} {
			if _, ok := m[k]; !ok {
				t.Errorf("version lacks %q", k)
			}
		}
		if s, _ := m["service"].(string); s == "" {
			t.Error("version.service empty")
		}
		if f.SchemaVersion != 0 && m["schema_version"] != float64(f.SchemaVersion) {
			t.Errorf("schema_version %v, want %d", m["schema_version"], f.SchemaVersion)
		}
	})
	t.Run("problem_404_trace", func(t *testing.T) {
		r := g.do(t, "GET", "/_conformance/no-such-route", "", map[string]string{
			"Traceparent": "00-" + traceID + "-b7ad6b7169203331-01"})
		p := g.problem(t, r, 404)
		if p.TraceID != traceID {
			t.Errorf("trace_id %q, want the traceparent trace id %q", p.TraceID, traceID)
		}
	})
	t.Run("problem_400", func(t *testing.T) {
		path, method, body := f.PaginationPath+"?page_size=not-a-number", "GET", ""
		if f.PaginationPath == "" {
			if f.IdempotentPath == "" {
				t.Skip("no endpoint configured to force a 400")
			}
			path, method, body = f.IdempotentPath, "POST", "{"
		}
		hdr := map[string]string{"Idempotency-Key": "conformance-400"}
		g.problem(t, g.do(t, method, path, body, hdr), 400)
	})
	if f.PaginationPath != "" {
		t.Run("pagination", func(t *testing.T) {
			if r := g.do(t, "GET", f.PaginationPath, "", nil); r.code != 200 {
				t.Fatalf("default page: %d %s", r.code, r.body)
			}
			if r := g.do(t, "GET", f.PaginationPath+"?page_size=200", "", nil); r.code != 200 {
				t.Fatalf("page_size=200: %d %s", r.code, r.body)
			}
			for _, q := range []string{"page_size=201", "page_size=0", "page_size=-5"} {
				g.problem(t, g.do(t, "GET", f.PaginationPath+"?"+q, "", nil), 400)
			}
			g.problem(t, g.do(t, "GET", f.PaginationPath+"?page_token=not-a-real-token", "", nil), 400)
		})
	}
	if f.IdempotentPath != "" {
		t.Run("idempotency", func(t *testing.T) {
			key := "conformance-" + time.Now().UTC().Format("150405.000000000")
			h := map[string]string{"Idempotency-Key": key}
			a := g.do(t, "POST", f.IdempotentPath, `{"conformance":"a"}`, h)
			if a.code >= 300 {
				t.Fatalf("first request: %d %s", a.code, a.body)
			}
			if a.header.Get("Idempotent-Replay") != "" {
				t.Error("first response must not be a replay")
			}
			b := g.do(t, "POST", f.IdempotentPath, `{"conformance":"a"}`, h)
			if b.code != a.code || !bytes.Equal(a.body, b.body) || b.header.Get("Idempotent-Replay") != "true" {
				t.Errorf("replay differs: %d %q replay=%q vs %d %q", b.code, b.body, b.header.Get("Idempotent-Replay"), a.code, a.body)
			}
			p := g.problem(t, g.do(t, "POST", f.IdempotentPath, `{"conformance":"b"}`, h), 422)
			if p.Code != "idempotency_key_reuse" {
				t.Errorf("code %q, want idempotency_key_reuse", p.Code)
			}
			if f.IdempotencyRequired {
				p := g.problem(t, g.do(t, "POST", f.IdempotentPath, `{"conformance":"a"}`, nil), 400)
				if p.Code != "idempotency_key_required" {
					t.Errorf("code %q, want idempotency_key_required", p.Code)
				}
			}
		})
	}
	if f.Logs != nil {
		t.Run("log_scrub", func(t *testing.T) {
			const token = "conformance-test-bearer"
			g.do(t, "GET", "/healthz?probe=1", "", map[string]string{"Authorization": "Bearer " + token})
			g.do(t, "GET", "/_conformance/no-such-route", "", map[string]string{"Authorization": "Bearer " + token})
			time.Sleep(20 * time.Millisecond)
			logs := f.Logs()
			if logs == "" {
				t.Fatal("no log output captured; the check would prove nothing")
			}
			if strings.Contains(logs, token) {
				t.Fatal("Authorization value found in logs")
			}
		})
	}

	// Response envelope shape check (reporting, non-blocking).
	if f.PaginationPath != "" {
		t.Run("response_envelope", func(t *testing.T) {
			r := g.do(t, "GET", f.PaginationPath, "", nil)
			if r.code != 200 {
				t.Logf("response_envelope: endpoint returned %d, skipping envelope check", r.code)
				return
			}
			var raw map[string]any
			if err := json.Unmarshal(r.body, &raw); err != nil {
				t.Logf("response_envelope: body not JSON: %v", err)
				return
			}
			items, hasItems := raw["items"]
			if !hasItems {
				t.Logf("response_envelope: missing 'items' field")
			} else if _, ok := items.([]any); !ok {
				t.Logf("response_envelope: 'items' is not an array")
			}
			if token, hasToken := raw["next_page_token"]; hasToken {
				if token != nil && token != "" {
					if _, ok := token.(string); !ok {
						t.Logf("response_envelope: 'next_page_token' is not a string")
					}
				}
			}
		})
	}

	// X-Request-ID presence check (reporting, non-blocking).
	t.Run("request_id_presence", func(t *testing.T) {
		checkPaths := []string{"/healthz", "/readyz", "/version"}
		if f.PaginationPath != "" {
			checkPaths = append(checkPaths, f.PaginationPath)
		}
		if f.IdempotentPath != "" {
			checkPaths = append(checkPaths, f.IdempotentPath)
		}
		checkPaths = append(checkPaths, "/_conformance/no-such-route")
		for _, path := range checkPaths {
			method := "GET"
			body := ""
			h := map[string]string{}
			if path == f.IdempotentPath {
				method = "POST"
				body = `{"conformance":"check"}`
				h["Idempotency-Key"] = "conformance-rid-check"
			}
			r := g.do(t, method, path, body, h)
			rid := r.header.Get("X-Request-ID")
			if rid == "" {
				t.Logf("request_id_presence: %s %s missing X-Request-ID (status %d)", method, path, r.code)
			} else if !uuidv7Re.MatchString(rid) {
				t.Logf("request_id_presence: %s %s X-Request-ID not UUIDv7: %q", method, path, rid)
			}
		}
	})

	// X-Request-ID echo check (reporting, non-blocking).
	t.Run("request_id_echo", func(t *testing.T) {
		sentID := "018f4f8a-7b3d-7c8a-8b9c-0123456789ab"
		r := g.do(t, "GET", "/healthz", "", map[string]string{"X-Request-ID": sentID})
		echoed := r.header.Get("X-Request-ID")
		if echoed != sentID {
			t.Logf("request_id_echo: sent %q, got %q", sentID, echoed)
		}
	})
}

// CaptureLogger returns a JSON slog logger writing to memory and a function
// that returns what it has written. Hand the logger to the service under test.
func CaptureLogger() (*slog.Logger, func() string) {
	var c capture
	return slog.New(slog.NewJSONHandler(&c, nil)), c.String
}

type capture struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (c *capture) Write(p []byte) (int, error) { c.mu.Lock(); defer c.mu.Unlock(); return c.b.Write(p) }
func (c *capture) String() string              { c.mu.Lock(); defer c.mu.Unlock(); return c.b.String() }
