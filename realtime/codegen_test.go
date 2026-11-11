package realtime

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGenerateTypeScript(t *testing.T) {
	s := NewSchemas("studio-api", "/api/rt")
	s.SetREST("/openapi.json")
	s.SetConnect("/descriptor.pb", true)
	s.SetRPCMethods([]string{"studio.v1.RunService/StartRun", "studio.v1.RunService/GetRun"})
	if err := s.AddEvent("run.started", "acct/*", json.RawMessage(`{
		"type": "object",
		"properties": {
			"run_id": {"type": "string"},
			"attempt": {"type": "integer"},
			"status": {"type": "string", "enum": ["queued", "running"]},
			"tags": {"type": "array", "items": {"type": "string"}},
			"meta": {"type": "object", "properties": {"ok": {"type": "boolean"}}, "required": ["ok"]}
		},
		"required": ["run_id", "status"]
	}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEvent("item.plan.step.updated", "acct/42/run/7", json.RawMessage(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}

	tsBytes, err := s.GenerateTypeScript()
	if err != nil {
		t.Fatalf("GenerateTypeScript: %v", err)
	}
	ts := string(tsBytes)

	for _, want := range []string{
		`export const SERVICE = "studio-api" as const;`,
		`export const REALTIME_PREFIX = "/api/rt" as const;`,
		`export const OPENAPI_PATH = "/openapi.json" as const;`,
		`export const DESCRIPTOR_SET_PATH = "/descriptor.pb" as const;`,
		`export interface ItemPlanStepUpdatedEventData {`,
		`export interface RunStartedEventData {`,
		`attempt?: number;`,
		`meta?: { ok: boolean };`,
		`run_id: string;`,
		`status: "queued" | "running";`,
		`tags?: Array<string>;`,
		`"item.plan.step.updated": ItemPlanStepUpdatedEventData;`,
		`"run.started": RunStartedEventData;`,
		`export const RPC_METHODS = ["studio.v1.RunService/GetRun", "studio.v1.RunService/StartRun"] as const;`,
		`export async function callRPC<`,
	} {
		if !strings.Contains(ts, want) {
			t.Errorf("generated TypeScript missing %q:\n%s", want, ts)
		}
	}

	// Deterministic: a second call returns identical bytes.
	tsBytes2, err := s.GenerateTypeScript()
	if err != nil {
		t.Fatal(err)
	}
	if string(tsBytes2) != ts {
		t.Fatal("GenerateTypeScript output is not deterministic")
	}
}

func TestGenerateTypeScriptValidation(t *testing.T) {
	for _, tc := range []struct {
		name string
		idx  SchemaIndex
	}{
		{"empty service", SchemaIndex{RealtimePrefix: "/rt"}},
		{"bad prefix", SchemaIndex{Service: "svc", RealtimePrefix: "rt"}},
		{"trailing slash prefix", SchemaIndex{Service: "svc", RealtimePrefix: "/rt/"}},
		{"bad event kind", SchemaIndex{Service: "svc", RealtimePrefix: "/rt", Events: []SchemaEvent{{Kind: "bad", TopicPattern: "*", Schema: json.RawMessage(`{}`)}}}},
		{"bad topic pattern", SchemaIndex{Service: "svc", RealtimePrefix: "/rt", Events: []SchemaEvent{{Kind: "a.b", TopicPattern: "Bad/*", Schema: json.RawMessage(`{}`)}}}},
		{"bad schema json", SchemaIndex{Service: "svc", RealtimePrefix: "/rt", Events: []SchemaEvent{{Kind: "a.b", TopicPattern: "*", Schema: json.RawMessage(`[]`)}}}},
		{"duplicate event kind", SchemaIndex{Service: "svc", RealtimePrefix: "/rt", Events: []SchemaEvent{
			{Kind: "a.b", TopicPattern: "*", Schema: json.RawMessage(`{}`)},
			{Kind: "a.b", TopicPattern: "*", Schema: json.RawMessage(`{}`)},
		}}},
		{"bad rpc method", SchemaIndex{Service: "svc", RealtimePrefix: "/rt", Connect: &ConnectSchema{RPCMethods: []string{"noslash"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := GenerateTypeScript(tc.idx); err == nil {
				t.Errorf("%s: expected error, got nil", tc.name)
			}
		})
	}
}

// TestGenerateTypeScriptCLI backs tools/gen-client.sh when PLINTH_SCHEMA_IN is
// set, and self-tests the file-to-file round trip otherwise.
func TestGenerateTypeScriptCLI(t *testing.T) {
	inPath := os.Getenv("PLINTH_SCHEMA_IN")
	outPath := os.Getenv("PLINTH_TS_OUT")
	if inPath == "" {
		dir := t.TempDir()
		inPath = filepath.Join(dir, "schema.json")
		outPath = filepath.Join(dir, "client.ts")
		s := NewSchemas("cli-test", "/api/rt")
		if err := s.AddEvent("run.started", "acct/*", json.RawMessage(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}`)); err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(s.Index())
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(inPath, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(inPath)
	if err != nil {
		t.Fatalf("read %s: %v", inPath, err)
	}
	var idx SchemaIndex
	if err := json.Unmarshal(raw, &idx); err != nil {
		t.Fatalf("parse %s: %v", inPath, err)
	}
	ts, err := GenerateTypeScript(idx)
	if err != nil {
		t.Fatalf("codegen: %v", err)
	}
	if outPath != "" {
		if err := os.WriteFile(outPath, ts, 0o644); err != nil {
			t.Fatalf("write %s: %v", outPath, err)
		}
	}
}

func TestStandardRulesMatchValidators(t *testing.T) {
	spectralBytes, err := os.ReadFile("rules/spectral-realtime.yaml")
	if err != nil {
		t.Fatalf("read spectral-realtime.yaml: %v", err)
	}
	spectral := string(spectralBytes)
	for _, want := range []string{
		"tcp-backend-no-html-response",
		"tcp-schema-index-event-kind",
		"tcp-schema-index-topic-pattern",
		"tcp-schema-index-rpc-method",
		`^[a-z][a-z0-9]*(\\.[a-z][a-z0-9]*)+$`,
	} {
		if !strings.Contains(spectral, want) {
			t.Errorf("spectral-realtime.yaml missing %q", want)
		}
	}

	bufBytes, err := os.ReadFile("rules/buf.yaml")
	if err != nil {
		t.Fatalf("read buf.yaml: %v", err)
	}
	buf := string(bufBytes)
	for _, want := range []string{
		"version: v2",
		"STANDARD",
		"service_suffix: Service",
		"rpc_allow_same_request_response: false",
	} {
		if !strings.Contains(buf, want) {
			t.Errorf("buf.yaml missing %q", want)
		}
	}
}
