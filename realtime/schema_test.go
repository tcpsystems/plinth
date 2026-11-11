package realtime

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestValidateKind(t *testing.T) {
	for _, tc := range []struct {
		kind    string
		wantErr bool
	}{
		{"run.started", false},
		{"item.plan.step.updated", false},
		{"run", true},          // needs at least family.kind
		{"Run.x", true},        // uppercase
		{"run.", true},         // empty trailing segment
		{".run.x", true},       // empty leading segment
		{"run._x.y", true},     // segment must start alphanumeric
		{"run.sta rted", true}, // space
		{"", true},
	} {
		if err := ValidateKind(tc.kind); (err != nil) != tc.wantErr {
			t.Errorf("ValidateKind(%q) err %v, wantErr %v", tc.kind, err, tc.wantErr)
		}
	}
}

func TestValidateTopicPattern(t *testing.T) {
	for _, tc := range []struct {
		pat     string
		wantErr bool
	}{
		{"*", false},
		{"acct/42/run/7", false},
		{"acct/42/*", false},
		{"*x", true},         // bare * only whole-string
		{"acct/*/run", true}, // wildcard must be trailing
		{"Acct/x", true},     // uppercase
		{"/x", true},         // leading slash
		{"x/", true},         // trailing slash
		{"", true},
	} {
		if err := ValidateTopicPattern(tc.pat); (err != nil) != tc.wantErr {
			t.Errorf("ValidateTopicPattern(%q) err %v, wantErr %v", tc.pat, err, tc.wantErr)
		}
	}
}

func TestSchemasAddEvent(t *testing.T) {
	s := NewSchemas("svc", "/rt")
	for _, tc := range []struct {
		name, kind, pattern string
		schema              json.RawMessage
	}{
		{"bad kind", "run", "acct/1", json.RawMessage(`{}`)},
		{"bad pattern", "run.started", "Acct/x", json.RawMessage(`{}`)},
		{"invalid json", "run.started", "acct/1", json.RawMessage(`nope`)},
		{"non-object", "run.started", "acct/1", json.RawMessage(`[]`)},
	} {
		if err := s.AddEvent(tc.kind, tc.pattern, tc.schema); err == nil {
			t.Errorf("%s: accepted", tc.name)
		}
	}
	if len(s.Index().Events) != 0 {
		t.Fatalf("rejected adds left events: %+v", s.Index().Events)
	}
	if err := s.AddEvent("run.started", "acct/1", json.RawMessage(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEvent("run.started", "acct/2", json.RawMessage(`{}`)); err == nil {
		t.Fatal("duplicate kind accepted")
	}
	evs := s.Index().Events
	if len(evs) != 1 || evs[0].Kind != "run.started" || evs[0].TopicPattern != "acct/1" {
		t.Fatalf("events %+v", evs)
	}
	if string(evs[0].Schema) != `{"type":"object"}` {
		t.Fatalf("schema %s", evs[0].Schema)
	}
}

// TestSchemasAddEventClones proves the index owns its bytes: a caller
// mutating its RawMessage after AddEvent must not corrupt a served index.
func TestSchemasAddEventClones(t *testing.T) {
	s := NewSchemas("svc", "/rt")
	raw := json.RawMessage(`{"type":"object","title":"a"}`)
	if err := s.AddEvent("run.started", "*", raw); err != nil {
		t.Fatal(err)
	}
	// Overwrite in place with equally-sized valid JSON.
	copy(raw, `{"type":"object","title":"b"}`)
	if got := string(s.Index().Events[0].Schema); got != `{"type":"object","title":"a"}` {
		t.Fatalf("index schema mutated with caller bytes: %s", got)
	}
}

func TestSchemasPointers(t *testing.T) {
	s := NewSchemas("svc", "/rt")
	if idx := s.Index(); idx.REST != nil || idx.Connect != nil {
		t.Fatalf("fresh index has pointers: %+v", idx)
	}
	s.SetREST("/openapi.json")
	s.SetRPCMethods([]string{"a.Svc/A", "b.Svc/B"})
	idx := s.Index()
	if idx.REST == nil || idx.REST.OpenAPIPath != "/openapi.json" {
		t.Fatalf("rest %+v", idx.REST)
	}
	// SetRPCMethods creates the Connect block when SetConnect was never
	// called (the standard profile needs no descriptor to be listable).
	if idx.Connect == nil || idx.Connect.DescriptorSetPath != "" || idx.Connect.Reflection {
		t.Fatalf("connect %+v", idx.Connect)
	}
	if len(idx.Connect.RPCMethods) != 2 || idx.Connect.RPCMethods[0] != "a.Svc/A" {
		t.Fatalf("rpc methods %+v", idx.Connect.RPCMethods)
	}
	s.SetConnect("/descriptor.pb", true)
	if c := s.Index().Connect; c.DescriptorSetPath != "/descriptor.pb" || !c.Reflection || len(c.RPCMethods) != 2 {
		t.Fatalf("connect after SetConnect %+v", c)
	}
}

func TestSchemasHandler(t *testing.T) {
	s := NewSchemas("svc", "/rt")
	if err := s.AddEvent("run.started", "acct/1", json.RawMessage(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type %q", ct)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("cache-control %q, want no-store", cc)
	}
	var idx SchemaIndex
	if err := json.NewDecoder(resp.Body).Decode(&idx); err != nil {
		t.Fatal(err)
	}
	if idx.Service != "svc" || idx.RealtimePrefix != "/rt" || len(idx.Events) != 1 {
		t.Fatalf("served index %+v", idx)
	}
}
