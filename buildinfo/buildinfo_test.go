package buildinfo

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// probe builds the testdata program under the given go tool flags and runs
// it, so the test observes what a real binary reports. go run does not stamp
// VCS information, which is why this builds a binary first.
func probe(t *testing.T, buildvcs bool, ldflags string) Info {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "probe")
	args := []string{"build", "-o", bin}
	if !buildvcs {
		args = append(args, "-buildvcs=false")
	}
	if ldflags != "" {
		args = append(args, "-ldflags", ldflags)
	}
	args = append(args, "./testdata/probe")
	if out, err := exec.Command("go", args...).CombinedOutput(); err != nil {
		t.Fatalf("go %v: %v\n%s", args, err, out)
	}
	out, err := exec.Command(bin).Output()
	if err != nil {
		t.Fatalf("run %s: %v", bin, err)
	}
	var info Info
	if err := json.Unmarshal(out, &info); err != nil {
		t.Fatalf("probe output %q: %v", out, err)
	}
	return info
}

func TestGetReportsThisToolchain(t *testing.T) {
	info := Get()
	if info.Revision == "" {
		t.Error("Revision is empty, want the VCS sha or Unknown")
	}
	if info.GoVersion != runtime.Version() {
		t.Errorf("GoVersion = %q, want %q", info.GoVersion, runtime.Version())
	}
}

func TestBuildWithVCSInfo(t *testing.T) {
	// This test requires VCS context (a .git directory) to embed revision info.
	// When running from a git archive checkout, revision is Unknown.
	info := probe(t, true, "")
	if info.Revision == Unknown || info.Revision == "" {
		t.Skipf("VCS info unavailable in test environment (Revision = %q); skipping VCS stamp check", info.Revision)
	}
	if len(info.Revision) < 7 {
		t.Errorf("Revision = %q, too short for a git sha", info.Revision)
	}
	if info.Time.IsZero() {
		t.Error("Time is zero; vcs.time was not read")
	}
	if info.GoVersion == "" {
		t.Error("GoVersion is empty")
	}
}

func TestBuildWithoutVCSInfo(t *testing.T) {
	info := probe(t, false, "")
	if info.Revision != Unknown {
		t.Errorf("Revision = %q, want %q without VCS stamps", info.Revision, Unknown)
	}
	if info.Modified {
		t.Error("Modified = true without VCS stamps")
	}
	if !info.Time.IsZero() {
		t.Errorf("Time = %v, want the zero time without VCS stamps", info.Time)
	}
	if info.GoVersion == "" {
		t.Error("GoVersion is empty")
	}
}

func TestLinkTimeOverride(t *testing.T) {
	const ldflags = "-X github.com/tcpsystems/plinth/buildinfo.Revision=0123456789abcdef" +
		" -X github.com/tcpsystems/plinth/buildinfo.Modified=true" +
		" -X github.com/tcpsystems/plinth/buildinfo.BuiltAt=2026-11-10T12:34:56Z"
	// buildvcs=false proves the values come from -X alone; the VCS build below
	// proves a non-empty override wins over embedded stamps.
	info := probe(t, false, ldflags)
	if info.Revision != "0123456789abcdef" {
		t.Errorf("Revision = %q, want the -X value", info.Revision)
	}
	if !info.Modified {
		t.Error("Modified = false, want the -X value true")
	}
	if got := info.Time.UTC().Format(time.RFC3339); got != "2026-11-10T12:34:56Z" {
		t.Errorf("Time = %q, want the -X value", got)
	}

	info = probe(t, true, ldflags)
	if info.Revision != "0123456789abcdef" {
		t.Errorf("Revision = %q, want the -X value to win over vcs.revision", info.Revision)
	}
}

func TestHandler(t *testing.T) {
	const service = "test-service"
	rec := httptest.NewRecorder()
	Handler(service).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/version", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body %q: %v", rec.Body, err)
	}
	want := []string{"service", "commit", "modified", "built_at", "go"}
	if len(body) != len(want) {
		t.Errorf("body has %d fields %v, want exactly %v", len(body), body, want)
	}
	for _, k := range want {
		if _, ok := body[k]; !ok {
			t.Errorf("body lacks %q", k)
		}
	}
	if body["service"] != service {
		t.Errorf("service = %v, want %q", body["service"], service)
	}
}

func TestHandlerRejectsOtherMethods(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler("test-service").ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/version", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
	if got := rec.Header().Get("Allow"); got != "GET, HEAD" {
		t.Errorf("Allow = %q, want %q", got, "GET, HEAD")
	}
}

func TestHandlerConcurrent(t *testing.T) {
	h := Handler("test-service")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/version", nil))
			if rec.Code != http.StatusOK {
				t.Errorf("status = %d, want %d", rec.Code, http.StatusOK)
			}
		}()
	}
	wg.Wait()
}
