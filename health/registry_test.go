package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type pinger struct{ err error }

func (p pinger) PingContext(context.Context) error { return p.err }

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

func TestMountedProbes(t *testing.T) {
	g := NewRegistry()
	loaded := false
	schema := 3
	g.Add("db", DB(pinger{}))
	g.Add("config", ConfigLoaded(func() bool { return loaded }))
	g.Add("schema", SchemaAtLeast(4, func(context.Context) (int, error) { return schema, nil }))
	mux := http.NewServeMux()
	Mount(mux, g, map[string]string{"service": "x"})

	if w := get(mux, "/healthz"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	w := get(mux, "/readyz")
	var body struct {
		Status string
		Checks map[string]string
	}
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if w.Code != 503 || body.Checks["db"] != "ok" || body.Checks["config"] != "fail" || body.Checks["schema"] != "fail" {
		t.Fatal(w.Code, w.Body)
	}
	loaded, schema = true, 4
	if w := get(mux, "/readyz"); w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	schema = 5 // newer schema is fine
	if w := get(mux, "/readyz"); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := get(mux, "/version"); w.Code != 200 || !strings.Contains(w.Body.String(), `"service":"x"`) {
		t.Fatal(w.Body)
	}
}

func TestReadyHidesErrorsAndSchemaError(t *testing.T) {
	g := NewRegistry()
	g.Add("db", DB(pinger{errors.New("postgres://user:secret@host/db")}))
	w := get(g.Ready(), "/readyz")
	if w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
		t.Fatal(w.Code, w.Body)
	}
	g2 := NewRegistry()
	g2.Add("schema", SchemaAtLeast(1, func(context.Context) (int, error) { return 0, errors.New("boom") }))
	if w := get(g2.Ready(), "/readyz"); w.Code != 503 {
		t.Fatal(w.Code)
	}
	if w := get(NewRegistry().Ready(), "/readyz"); w.Code != 200 {
		t.Fatal("empty registry must be ready")
	}
}

func TestAddReplaces(t *testing.T) {
	g := NewRegistry()
	g.Add("a", func(context.Context) error { return errors.New("x") })
	g.Add("a", func(context.Context) error { return nil })
	if w := get(g.Ready(), "/"); w.Code != 200 || strings.Count(w.Body.String(), `"a"`) != 1 {
		t.Fatal(w.Body)
	}
}
