package settings

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/internal/fakesql"
)

type src struct {
	loads int
	vals  map[string]string
}

func (s *src) Load(_ context.Context, k string) (Setting, error) {
	s.loads++
	v, ok := s.vals[k]
	if !ok {
		return Setting{}, ErrNotFound
	}
	return Setting{Key: k, Value: []byte(v), Version: int64(s.loads)}, nil
}

type lis struct{ ch chan string }

func (l lis) Listen(context.Context, string) (<-chan string, error) { return l.ch, nil }

func TestCacheTTLAndInvalidate(t *testing.T) {
	s := &src{vals: map[string]string{"a": `{"n":1}`}}
	l := New(s, time.Minute, nil)
	now := time.Unix(100, 0)
	l.now = func() time.Time { return now }
	var v struct{ N int }
	for i := 0; i < 3; i++ {
		if err := l.GetInto(t.Context(), "a", &v); err != nil || v.N != 1 {
			t.Fatal(err, v)
		}
	}
	if s.loads != 1 {
		t.Fatal("cache miss", s.loads)
	}
	now = now.Add(2 * time.Minute)
	_, _ = l.Get(t.Context(), "a")
	l.Invalidate("a")
	_, _ = l.Get(t.Context(), "a")
	if s.loads != 3 {
		t.Fatal(s.loads)
	}
	if _, err := l.Get(t.Context(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := l.GetInto(t.Context(), "a", new(string)); err == nil {
		t.Fatal("type mismatch must error")
	}
}

func TestNotifyInvalidates(t *testing.T) {
	s := &src{vals: map[string]string{"a": `1`, "b": `2`}}
	ch := make(chan string)
	l := New(s, time.Hour, lis{ch})
	_, _ = l.Get(t.Context(), "a")
	_, _ = l.Get(t.Context(), "b")
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error)
	go func() { done <- l.Notify(ctx, "setting_changed") }()
	ch <- "a"
	ch <- "a" // synchronizes: the first has been applied
	_, _ = l.Get(t.Context(), "b")
	if s.loads != 2 {
		t.Fatal("b must stay cached", s.loads)
	}
	_, _ = l.Get(t.Context(), "a")
	if s.loads != 3 {
		t.Fatal("a must reload", s.loads)
	}
	ch <- "" // flush all
	ch <- ""
	_, _ = l.Get(t.Context(), "b")
	if s.loads != 4 {
		t.Fatal(s.loads)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := New(s, 0, nil).Notify(t.Context(), "x"); err == nil {
		t.Fatal("nil listener")
	}
}

func TestSQLSource(t *testing.T) {
	db := fakesql.Open(func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		if a[0] == "yes" {
			return []string{"value", "version"}, [][]driver.Value{{[]byte(`{"x":true}`), int64(7)}}, 0, nil
		}
		return []string{"value", "version"}, nil, 0, nil
	})
	s, err := SQLSource{DB: db}.Load(t.Context(), "yes")
	if err != nil || s.Version != 7 || string(s.Value) != `{"x":true}` {
		t.Fatal(s, err)
	}
	if _, err := (SQLSource{DB: db}).Load(t.Context(), "no"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}

func TestBootstrap(t *testing.T) {
	got := Bootstrap(func(n string) string { return map[string]string{"A": "1", "B": "2"}[n] }, "A", "C")
	if got["A"] != "1" || got["C"] != "" || len(got) != 2 {
		t.Fatal(got)
	}
}
