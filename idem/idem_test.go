package idem

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/internal/fakesql"
	"github.com/tcpsystems/plinth/problem"
)

func setup(o Options) (http.Handler, *int32) {
	var calls int32
	h := Middleware(o)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		b, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"n":` + string(rune('0'+atomic.LoadInt32(&calls))) + `,"echo":"` + string(b) + `"}`))
	}))
	return h, &calls
}

func post(h http.Handler, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/things", strings.NewReader(body))
	if key != "" {
		r.Header.Set(Header, key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func TestReplayAndReuse(t *testing.T) {
	h, calls := setup(Options{Store: NewMemoryStore()})
	a := post(h, "k1", "one")
	b := post(h, "k1", "one")
	if a.Code != 201 || b.Code != 201 || a.Body.String() != b.Body.String() || *calls != 1 {
		t.Fatal(a, b, *calls)
	}
	if a.Header().Get(ReplayHeader) != "" || b.Header().Get(ReplayHeader) != "true" || b.Header().Get("Content-Type") != "application/json" {
		t.Fatal(a.Header(), b.Header())
	}
	c := post(h, "k1", "two")
	var p problem.Problem
	_ = json.Unmarshal(c.Body.Bytes(), &p)
	if c.Code != 422 || p.Code != "idempotency_key_reuse" || *calls != 1 {
		t.Fatal(c.Code, c.Body, *calls)
	}
	if d := post(h, "k2", "one"); *calls != 2 || d.Header().Get(ReplayHeader) != "" {
		t.Fatal("new key should execute")
	}
}

func TestRequired(t *testing.T) {
	h, calls := setup(Options{Store: NewMemoryStore(), Required: true})
	w := post(h, "", "x")
	var p problem.Problem
	_ = json.Unmarshal(w.Body.Bytes(), &p)
	if w.Code != 400 || p.Code != "idempotency_key_required" || *calls != 0 {
		t.Fatal(w.Code, w.Body)
	}
	g := httptest.NewRecorder()
	h.ServeHTTP(g, httptest.NewRequest("GET", "/things", nil))
	if g.Code != 201 {
		t.Fatal("GET must not require a key")
	}
	opt, c2 := setup(Options{Store: NewMemoryStore()})
	post(opt, "", "x")
	post(opt, "", "x")
	if *c2 != 2 {
		t.Fatal("optional mode without key must pass through")
	}
}

func TestInvalidKeyAndBodyLimit(t *testing.T) {
	h, _ := setup(Options{Store: NewMemoryStore(), MaxBody: 4})
	if w := post(h, strings.Repeat("a", 256), "x"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := post(h, "bad key", "x"); w.Code != 400 {
		t.Fatal(w.Code)
	}
	if w := post(h, "k", "12345"); w.Code != 400 {
		t.Fatal(w.Code)
	}
}

func TestExpiryServerErrorAndScope(t *testing.T) {
	now := time.Unix(1000, 0)
	clock := func() time.Time { return now }
	h, calls := setup(Options{Store: NewMemoryStore(), Now: clock})
	post(h, "k", "x")
	now = now.Add(23 * time.Hour)
	post(h, "k", "x")
	if *calls != 1 {
		t.Fatal("replay within TTL")
	}
	now = now.Add(2 * time.Hour)
	post(h, "k", "y") // expired: different body is fine
	if *calls != 2 {
		t.Fatal("expired record must not replay")
	}

	var n int32
	fail := Middleware(Options{Store: NewMemoryStore()})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&n, 1)
		w.WriteHeader(500)
	}))
	post(fail, "k", "x")
	post(fail, "k", "x")
	if n != 2 {
		t.Fatal("5xx must not be stored")
	}

	who := ""
	sc, c3 := setup(Options{Store: NewMemoryStore(), Scope: func(*http.Request) string { return who }})
	who = "a"
	post(sc, "k", "x")
	who = "b"
	post(sc, "k", "x")
	if *c3 != 2 {
		t.Fatal("scope must separate callers")
	}
}

func TestConcurrentDuplicatesRunOnce(t *testing.T) {
	h, calls := setup(Options{Store: NewMemoryStore()})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); post(h, "same", "x") }()
	}
	wg.Wait()
	if *calls != 1 {
		t.Fatal(*calls)
	}
}

func TestSQLStore(t *testing.T) {
	var puts int
	var stored Record
	db := fakesql.Open(func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		switch {
		case strings.HasPrefix(q, "INSERT INTO idempotency_key"):
			puts++
			if puts == 2 {
				return nil, nil, 0, nil // conflict with live row
			}
			stored = Record{Key: a[0].(string), RequestHash: a[1].(string)}
			return nil, nil, 1, nil
		case strings.HasPrefix(q, "SELECT"):
			if stored.Key != a[0].(string) {
				return []string{"a"}, nil, 0, nil
			}
			return []string{"request_hash", "status", "content_type", "body", "expires_at"},
				[][]driver.Value{{stored.RequestHash, int64(201), "application/json", []byte("{}"), time.Unix(5, 0)}}, 0, nil
		case strings.HasPrefix(q, "DELETE"):
			return nil, nil, 3, nil
		}
		t.Fatal("unexpected", q)
		return nil, nil, 0, nil
	})
	s := SQLStore{DB: db}
	ctx := t.Context()
	now := time.Unix(1, 0)
	if r, err := s.Get(ctx, "k", now); r != nil || err != nil {
		t.Fatal(r, err)
	}
	if ok, err := s.Put(ctx, Record{Key: "k", RequestHash: "h", Status: 201, Body: []byte("{}"), ExpiresAt: now.Add(time.Hour)}, now); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if ok, _ := s.Put(ctx, Record{Key: "k"}, now); ok {
		t.Fatal("conflict must report not stored")
	}
	r, err := s.Get(ctx, "k", now)
	if err != nil || r == nil || r.Status != 201 || r.RequestHash != "h" || string(r.Body) != "{}" {
		t.Fatal(r, err)
	}
	if n, err := s.Purge(ctx, now); n != 3 || err != nil {
		t.Fatal(n, err)
	}
}
