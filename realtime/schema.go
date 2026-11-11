package realtime

import (
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"sync"

	"github.com/tcpsystems/plinth/jsonx"
	"github.com/tcpsystems/plinth/problem"
)

// Served schema: every service serves the shape of what it speaks, so
// clients are generated from the served schema and never hand-written. REST services
// serve OpenAPI 3.1 at their well-known path; Connect/gRPC services serve
// server reflection plus a buf-generated descriptor set; realtime streams
// (SSE and WebSocket) serve JSON Schema per event kind, listed in one index
// document per service. This file implements the realtime half of that
// index, stdlib-only; the descriptor-set/OpenAPI halves are paths to the
// service's own files and codegen, recorded here for discovery.
//
// Compatibility policy: before 1.0 there are no backward-compatibility
// obligations when the project's own generated clients are the consumers, so
// v1 may change freely as long as the clients are regenerated in the same
// change. The index is therefore cheap to serve and
// never negotiated: it is the truth of this binary, not of a version.

// kindRE is the event-kind grammar: dot-separated lowercase family
// and kind segments ("run.started", "item.plan.step.updated"). Kinds are
// published into the SSE "event:" field, the WS frames' "kind" and index
// type names, so one grammar keeps all three joinable.
var kindRE = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)+$`)

// ValidateKind reports whether kind matches the event-kind grammar
// (family.kind, dot-separated, lowercase alphanumeric segments).
func ValidateKind(kind string) error {
	if !kindRE.MatchString(kind) {
		return errors.New("realtime: event kind must be dot-separated lowercase family.kind segments like \"run.started\", got " + strconv.Quote(kind))
	}
	return nil
}

// topicPatternRE accepts exact slash-separated topics and the hub's two
// wildcard forms: a bare "*" or a trailing "prefix*".
var topicPatternRE = regexp.MustCompile(`^(\*|[a-z0-9][a-z0-9_\-]*(/[a-z0-9][a-z0-9_\-]*)*(/\*)?)$`)

// ValidateTopicPattern reports whether p is a well-formed subscription
// pattern ("*", "acct/42/run/7", "acct/42/*").
func ValidateTopicPattern(p string) error {
	if !topicPatternRE.MatchString(p) {
		return errors.New("realtime: topic pattern must be slash-separated lowercase segments with an optional trailing /*, got " + strconv.Quote(p))
	}
	return nil
}

// SchemaEvent describes one event kind carried by the realtime transports:
// its Hub kind, a subscription pattern it can arrive on, and its JSON
// Schema (draft 2020-12) for the event frame's "data".
type SchemaEvent struct {
	Kind         string          `json:"kind"`
	TopicPattern string          `json:"topic_pattern"`
	Schema       json.RawMessage `json:"schema"`
}

// SchemaIndex is the one discoverable document listing where everything this
// service serves its schema lives.
type SchemaIndex struct {
	// Service is the lowercase service name, e.g. "orders-api".
	Service string `json:"service"`
	// RealtimePrefix is where the transports are mounted ("/api/rt"), so
	// clients can build {prefix}/stream, {prefix}/ws, {prefix}/rpc/{spec}.
	RealtimePrefix string `json:"realtime_prefix"`
	// Events lists every event kind the hub may deliver.
	Events []SchemaEvent `json:"events"`
	// REST points at the service's OpenAPI 3.1 document, when it has one.
	REST *RESTSchema `json:"rest,omitempty"`
	// Connect points at the descriptor set and reflection, when mounted.
	Connect *ConnectSchema `json:"connect,omitempty"`
}

// RESTSchema locates the OpenAPI document (well-known path convention:
// /openapi.json under the service's HTTP root).
type RESTSchema struct {
	OpenAPIPath string `json:"openapi_path"`
}

// ConnectSchema locates the RPC surface: DescriptorSetPath is the
// buf-generated FileDescriptorSet the service serves (convention:
// /descriptor.pb), Reflection says gRPC server reflection is enabled so
// clients can also discover at runtime. RPCMethods lists the fully
// qualified plinth RPC method names this binary registered, so generated
// clients need no descriptor to call the standard profile.
type ConnectSchema struct {
	DescriptorSetPath string   `json:"descriptor_set_path,omitempty"`
	Reflection        bool     `json:"reflection,omitempty"`
	RPCMethods        []string `json:"rpc_methods,omitempty"`
}

// Schemas is the registry behind the index handler: add every event kind
// (and optionally the REST/Connect pointers) at startup, mount Handler().
type Schemas struct {
	idx SchemaIndex
	mu  sync.RWMutex
}

// NewSchemas starts an index for a service mounted at prefix.
func NewSchemas(service, prefix string) *Schemas {
	return &Schemas{idx: SchemaIndex{Service: service, RealtimePrefix: prefix}}
}

// AddEvent registers one event kind with its JSON Schema. It rejects bad
// kinds, malformed patterns, non-object schemas and JSON Schema that is
// not valid JSON — a broken index would generate broken clients, which is
// worse than a failed startup.
func (s *Schemas) AddEvent(kind, topicPattern string, schema json.RawMessage) error {
	if err := ValidateKind(kind); err != nil {
		return err
	}
	if err := ValidateTopicPattern(topicPattern); err != nil {
		return err
	}
	var probe map[string]any
	if err := json.Unmarshal(schema, &probe); err != nil {
		return errors.New("realtime: schema for " + kind + " must be a JSON object: " + err.Error())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.idx.Events {
		if e.Kind == kind {
			return errors.New("realtime: duplicate event kind " + kind)
		}
	}
	s.idx.Events = append(s.idx.Events, SchemaEvent{Kind: kind, TopicPattern: topicPattern, Schema: json.RawMessage(slices.Clone(schema))})
	return nil
}

// SetREST records where the OpenAPI 3.1 document is served.
func (s *Schemas) SetREST(openAPIPath string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idx.REST = &RESTSchema{OpenAPIPath: openAPIPath}
}

// SetConnect records the descriptor-set path and reflection flag, keeping
// any RPC method list already set (by [Register] or [Schemas.SetRPCMethods]):
// the two setters describe halves of the same surface and must be order
// independent.
func (s *Schemas) SetConnect(descriptorSetPath string, reflection bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx.Connect == nil {
		s.idx.Connect = &ConnectSchema{}
	}
	s.idx.Connect.DescriptorSetPath = descriptorSetPath
	s.idx.Connect.Reflection = reflection
}

// SetRPCMethods lists the fully qualified method names registered on the
// [RPC] handler (kept in sync by [Register]).
func (s *Schemas) SetRPCMethods(methods []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx.Connect == nil {
		s.idx.Connect = &ConnectSchema{}
	}
	s.idx.Connect.RPCMethods = slices.Clone(methods)
}

// Index returns the served index document.
func (s *Schemas) Index() SchemaIndex {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.idx
}

// Handler serves GET of the index as JSON (Cache-Control: no-store: it is
// the truth of this process, and a stale index generating stale clients
// defeats the point).
func (s *Schemas) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := jsonx.Marshal(s.Index())
		if err != nil {
			problem.WriteError(w, r, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})
}
