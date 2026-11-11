package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func listen(t *testing.T) net.Listener {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func TestSeparateListenersAndCancel(t *testing.T) {
	pub, mesh := listen(t), listen(t)
	pubA, meshA := pub.Addr().String(), mesh.Addr().String()
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("public")) })
	mh := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("mesh")) })
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Config{PublicListener: pub, MeshListener: mesh, MeshHandler: mh}, h) }()
	for addr, want := range map[string]string{pubA: "public", meshA: "mesh"} {
		var body []byte
		for i := 0; i < 50; i++ {
			resp, err := http.Get("http://" + addr)
			if err == nil {
				body, _ = io.ReadAll(resp.Body)
				resp.Body.Close()
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if string(body) != want {
			t.Fatalf("%s: %q", addr, body)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := http.Get("http://" + pubA); err == nil {
		t.Fatal("still serving after shutdown")
	}
}

func TestSIGTERMDrainsInFlight(t *testing.T) {
	pub := listen(t)
	started, release := make(chan struct{}), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = w.Write([]byte("done"))
	})
	done := make(chan error, 1)
	go func() { done <- Run(t.Context(), Config{PublicListener: pub, DrainTimeout: 5 * time.Second}, h) }()
	got := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + pub.Addr().String())
		if err != nil {
			got <- err.Error()
			return
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		got <- string(b)
	}()
	<-started
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
		t.Fatal("Run returned before the in-flight request finished")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if g := <-got; g != "done" {
		t.Fatal(g)
	}
}

func TestDrainTimeoutReported(t *testing.T) {
	pub := listen(t)
	started := make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		time.Sleep(500 * time.Millisecond)
	})
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, Config{PublicListener: pub, DrainTimeout: 50 * time.Millisecond}, h) }()
	go func() { _, _ = http.Get("http://" + pub.Addr().String()) }()
	<-started
	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestConfigErrors(t *testing.T) {
	if err := Run(t.Context(), Config{PublicAddr: "127.0.0.1:0"}, nil); err == nil {
		t.Fatal("nil handler")
	}
	if err := Run(t.Context(), Config{}, http.NotFoundHandler()); err == nil {
		t.Fatal("no listeners")
	}
	busy := listen(t)
	defer busy.Close()
	if err := Run(t.Context(), Config{PublicAddr: "127.0.0.1:0", MeshAddr: busy.Addr().String()}, http.NotFoundHandler()); err == nil {
		t.Fatal("busy mesh port")
	}
}

func TestVersion(t *testing.T) {
	GitSHA, BuildTime = "abc123", "2026-11-07T00:00:00Z"
	defer func() { GitSHA, BuildTime = "", "" }()
	v := ReadVersion("hello", 4, map[string]string{"service": "1"})
	if v.Service != "hello" || v.GitSHA != "abc123" || v.BuildTime != "2026-11-07T00:00:00Z" || v.SchemaVersion != 4 || v.Arch != runtime.GOARCH || v.Contracts["service"] != "1" {
		t.Fatalf("%+v", v)
	}
	b, _ := json.Marshal(v)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	for _, k := range []string{"service", "git_sha", "build_time", "schema_version", "contracts", "arch"} {
		if _, ok := m[k]; !ok {
			t.Errorf("missing %s", k)
		}
	}
	GitSHA, BuildTime = "", ""
	if v := ReadVersion("x", 0, nil); v.GitSHA == "" || v.BuildTime == "" || v.Contracts == nil {
		t.Fatalf("%+v", v)
	}
}
