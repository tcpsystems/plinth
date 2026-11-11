package leader_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/tcpsystems/plinth/keel/internal/fakesql"
	"github.com/tcpsystems/plinth/keel/leader"
)

// worker is the shape of a background job that must run in one replica at a
// time: each round it tries to lead, and the leader does the work in steps,
// renewing the lease before each step and stopping when it is lost.
type worker struct {
	m      *leader.Manager
	job    string
	ttl    time.Duration
	step   time.Duration
	rounds int
	did    []int // rounds whose work this worker did
}

// run performs rounds and returns the lock it still held, if any.
func (w *worker) run(ctx context.Context) (released bool, err error) {
	for round := 0; round < w.rounds; round++ {
		if round > 0 {
			select {
			case <-ctx.Done():
				return true, nil
			case <-time.After(w.step):
			}
		}
		ok, err := w.m.TryAcquire(ctx, w.job, w.ttl)
		if err != nil {
			return true, fmt.Errorf("try acquire: %w", err)
		}
		if !ok {
			continue
		}
		if err := w.workStep(ctx); err != nil {
			return true, err
		}
		if w.release(ctx) {
			return true, nil
		}
	}
	return true, nil
}

func (w *worker) release(ctx context.Context) bool {
	ok, err := w.m.Release(ctx, w.job)
	if err != nil {
		return false
	}
	return ok
}

func (w *worker) workStep(ctx context.Context) error {
	held, err := w.m.Renew(ctx, w.job)
	if err != nil {
		return fmt.Errorf("renew: %w", err)
	}
	if !held {
		return fmt.Errorf("lost the lock for %s; stopping", w.job)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(w.step):
	}
	w.did = append(w.did, len(w.did))
	return nil
}

// A set of workers sharing one job do each round's work once.
func TestWorkerExclusive(t *testing.T) {
	db := testDB(t)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	const rounds = 5
	var (
		mu      sync.Mutex
		done    = map[int]int{}
		wg      sync.WaitGroup
		workers = 3
	)
	for i := 0; i < workers; i++ {
		w := &worker{
			m:      newManager(t, db, newNamespace()),
			job:    "hourly-report",
			ttl:    time.Minute,
			step:   5 * time.Millisecond,
			rounds: rounds,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := w.run(ctx); err != nil {
				t.Errorf("worker: %v", err)
			}
			mu.Lock()
			for _, r := range w.did {
				done[r]++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	for round := 0; round < rounds; round++ {
		if n := done[round]; n > 1 {
			t.Errorf("round %d ran %d times; want at most once", round, n)
		}
	}
	if len(done) == 0 {
		t.Fatal("no round ran")
	}
}

// An interrupt stops the worker before its next round.
func TestWorkerStopsOnCancel(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	w := &worker{
		m:      newManager(t, db, newNamespace()),
		job:    "hourly-report",
		ttl:    time.Minute,
		step:   time.Hour, // the first round waits here
		rounds: 2,
	}
	cancel()
	if released, err := w.run(ctx); err != nil || !released {
		t.Fatalf("run = %v, %v; want true, nil", released, err)
	}
	if len(w.did) != 0 {
		t.Fatalf("did %d rounds after cancel; want 0", len(w.did))
	}
}

// The worker script runs against an in-memory database that answers the lock
// statements, so the acquire, renew and release path is exercised without a
// Postgres.
func TestWorker(t *testing.T) {
	var mu sync.Mutex
	held := map[int64]bool{}
	key := func(a []driver.Value) int64 { return a[0].(int64)<<32 | a[1].(int64)&0xffffffff }
	h := func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case strings.HasPrefix(q, "SELECT pg_try_advisory_lock"):
			k := key(a)
			ok := !held[k]
			held[k] = true
			return []string{"pg_try_advisory_lock"}, [][]driver.Value{{ok}}, 0, nil
		case strings.HasPrefix(q, "SELECT pg_advisory_unlock"):
			k := key(a)
			ok := held[k]
			delete(held, k)
			return []string{"pg_advisory_unlock"}, [][]driver.Value{{ok}}, 0, nil
		case strings.Contains(q, "FROM pg_locks"):
			return []string{"?column?"}, [][]driver.Value{{true}}, 0, nil
		}
		return nil, nil, 0, errors.New("unexpected statement " + q)
	}

	// Two managers with their own pools stand in for two replicas; the second
	// waits for the lock the first holds.
	ctx := context.Background()
	first := newFakeManager(t, h)
	if ok, err := first.TryAcquire(ctx, "nightly", time.Minute); err != nil || !ok {
		t.Fatalf("TryAcquire = %v, %v; want true, nil", ok, err)
	}
	second := newFakeManager(t, h)
	if ok, err := second.TryAcquire(ctx, "nightly", time.Minute); err != nil || ok {
		t.Fatalf("second TryAcquire = %v, %v; want false, nil", ok, err)
	}
	if held, err := first.Renew(ctx, "nightly"); err != nil || !held {
		t.Fatalf("Renew = %v, %v; want true, nil", held, err)
	}
	if released, err := first.Release(ctx, "nightly"); err != nil || !released {
		t.Fatalf("Release = %v, %v; want true, nil", released, err)
	}
	if ok, err := second.TryAcquire(ctx, "nightly", time.Minute); err != nil || !ok {
		t.Fatalf("TryAcquire after release = %v, %v; want true, nil", ok, err)
	}
	if released, err := second.Release(ctx, "nightly"); err != nil || !released {
		t.Fatalf("second Release = %v, %v; want true, nil", released, err)
	}
	w := &worker{m: first, job: "nightly", ttl: time.Minute, step: time.Millisecond, rounds: 2}
	if _, err := w.run(ctx); err != nil || len(w.did) != 1 {
		t.Fatalf("worker run = %v after %d rounds; want nil after 1", err, len(w.did))
	}
}

var fakeDrivers atomic.Int32

// newFakeManager returns a Manager over a pool backed by h, opened the way a
// service opens its pool.
func newFakeManager(t *testing.T, h fakesql.Handler) *leader.Manager {
	t.Helper()
	name := fmt.Sprintf("fake-%d", fakeDrivers.Add(1))
	fakesql.Register(name, h)
	db, err := leader.OpenSQL(name, "")
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	m := leader.New(db, leader.Config{Namespace: 7})
	t.Cleanup(func() { _ = m.Close(); _ = db.Close() })
	return m
}
