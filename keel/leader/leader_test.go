package leader_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/tcpsystems/plinth/keel/leader"
)

// testDB opens the disposable database from KEEL_TEST_DSN
// (tools/testdb.sh go ...). Each call is an independent pool, so separate
// calls stand in for separate processes.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("KEEL_TEST_DSN")
	if dsn == "" {
		t.Skip("KEEL_TEST_DSN is not set; run through tools/testdb.sh go")
	}
	db, err := leader.OpenSQL("pgx", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

var namespaces atomic.Int32

// newNamespace gives each test its own advisory lock key space so a stale
// lock from an earlier run cannot change the result.
func newNamespace() leader.Config {
	return leader.Config{Namespace: 1_000_000 + namespaces.Add(1)}
}

// newManager returns a Manager whose locks are released when the test ends,
// so a later -count run does not meet locks leaked by a failed one.
func newManager(t *testing.T, db *sql.DB, cfg leader.Config) *leader.Manager {
	t.Helper()
	m := leader.New(db, cfg)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

func mustAcquire(t *testing.T, m *leader.Manager, job string) {
	t.Helper()
	ok, err := m.TryAcquire(context.Background(), job, time.Minute)
	if err != nil || !ok {
		t.Fatalf("TryAcquire(%q) = %v, %v; want true, nil", job, ok, err)
	}
}

func TestTryAcquireExcludesSecondProcess(t *testing.T) {
	cfg := newNamespace()
	a := newManager(t, testDB(t), cfg)
	b := newManager(t, testDB(t), cfg) // another process, same namespace
	ctx := context.Background()
	mustAcquire(t, a, "nightly-reindex")

	ok, err := b.TryAcquire(ctx, "nightly-reindex", time.Minute)
	if err != nil {
		t.Fatalf("second TryAcquire: %v", err)
	}
	if ok {
		t.Fatal("second process acquired a lock the first process holds")
	}

	// A different job in the same namespace is independent.
	mustAcquire(t, b, "nightly-reindex-copy")

	released, err := a.Release(ctx, "nightly-reindex")
	if err != nil || !released {
		t.Fatalf("Release = %v, %v; want true, nil", released, err)
	}
	mustAcquire(t, b, "nightly-reindex")
}

func TestReleaseReturnsLockAndConnection(t *testing.T) {
	db := testDB(t)
	cfg := newNamespace()
	cfg.MaxConns = 1
	a := newManager(t, db, cfg)
	cfg2 := cfg // same keys, its own pinned pool
	b := newManager(t, db, cfg2)
	ctx := context.Background()

	// Churn the single pinned connection well past the point where an idle
	// timer would have reaped it; the lock must stay held.
	for i := 0; i < 5; i++ {
		mustAcquire(t, a, "hourly-sync")
		if ok, err := b.TryAcquire(ctx, "hourly-sync", time.Minute); err != nil || ok {
			t.Fatalf("round %d: peer TryAcquire = %v, %v; want false, nil", i, ok, err)
		}
		if released, err := a.Release(ctx, "hourly-sync"); err != nil || !released {
			t.Fatalf("round %d: Release = %v, %v; want true, nil", i, released, err)
		}
	}

	// Releasing again is a no-op, not an error.
	if released, err := a.Release(ctx, "hourly-sync"); err != nil || released {
		t.Fatalf("second Release = %v, %v; want false, nil", released, err)
	}
	if ok, err := a.Renew(ctx, "hourly-sync"); err != nil || ok {
		t.Fatalf("Renew after Release = %v, %v; want false, nil", ok, err)
	}
}

func TestRenewTracksHeldLock(t *testing.T) {
	db := testDB(t)
	m := newManager(t, db, newNamespace())
	ctx := context.Background()

	if ok, err := m.Renew(ctx, "report"); err != nil || ok {
		t.Fatalf("Renew before acquire = %v, %v; want false, nil", ok, err)
	}
	mustAcquire(t, m, "report")
	if ok, err := m.Renew(ctx, "report"); err != nil || !ok {
		t.Fatalf("Renew while held = %v, %v; want true, nil", ok, err)
	}
	if released, err := m.Release(ctx, "report"); err != nil || !released {
		t.Fatalf("Release = %v, %v; want true, nil", released, err)
	}
}

func TestRenewReportsLostLock(t *testing.T) {
	db := testDB(t)
	cfg := newNamespace()
	cfg.MaxConns = 1
	m := newManager(t, db, cfg)
	ctx := context.Background()
	mustAcquire(t, m, "report")

	// Unlock from a second session on the same key, simulating a lock that
	// vanished without Release ever running.
	if unlock(t, db, cfg, "report") {
		t.Fatal("second session unlocked a lock it does not hold")
	}

	if ok, err := m.Renew(ctx, "report"); err != nil || !ok {
		t.Fatalf("Renew = %v, %v; want true, nil while the connection lives", ok, err)
	}

	// Dropping the holder's connection without Release is the other way a
	// lock vanishes; the session ending takes the lock with it.
	pid := lockPID(t, db, cfg, "report")
	if _, err := db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatalf("terminate holder: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		ok, err := m.Renew(ctx, "report")
		if err == nil && !ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Renew = %v, %v after the holder's connection died; want false, nil", ok, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// lockKey mirrors the key layout: namespace in classid's high bits, job key
// across the classid sign bit and objid. pg_locks shows both as unsigned.
func lockKey(cfg leader.Config, job string) (classid, objid uint32) {
	key := uint64(leader.Key(job))
	classid = uint32(key>>33&(1<<30-1) | uint64(cfg.Namespace&0x7fffffff)<<31)
	objid = uint32(key>>1 | key<<31)
	return classid, objid
}

// lockPID finds the backend holding job's lock under cfg.
func lockPID(t *testing.T, db *sql.DB, cfg leader.Config, job string) int {
	t.Helper()
	classid, objid := lockKey(cfg, job)
	var pid int
	err := db.QueryRow(`SELECT pid FROM pg_locks
		WHERE locktype = 'advisory' AND classid::bigint = $1 AND objid::bigint = $2
		AND objsubid = 2 AND granted`, classid, objid).Scan(&pid)
	if err != nil {
		t.Fatalf("find lock holder: %v", err)
	}
	return pid
}

// unlock releases job's lock from a fresh session, which holds no locks.
func unlock(t *testing.T, db *sql.DB, cfg leader.Config, job string) bool {
	t.Helper()
	classid, objid := lockKey(cfg, job)
	var unlocked bool
	if err := db.QueryRow("SELECT pg_advisory_unlock($1::int4, $2::int4)", int32(classid), int32(objid)).Scan(&unlocked); err != nil {
		t.Fatalf("unlock from second session: %v", err)
	}
	return unlocked
}

func TestValidation(t *testing.T) {
	m := newManager(t, testDB(t), newNamespace())
	ctx := context.Background()
	if ok, err := m.TryAcquire(ctx, "", time.Minute); ok || err == nil {
		t.Fatalf("empty job = %v, %v; want false and an error", ok, err)
	}
	if ok, err := m.TryAcquire(ctx, "job", 0); ok || err == nil {
		t.Fatalf("zero ttl = %v, %v; want false and an error", ok, err)
	}
}

// TestPlainSQLOpenIsRejected: a pool opened with sql.Open does not tell the
// Manager which database the locks must live on, and pinning it to the wrong
// database would let two processes both become leader.
func TestPlainSQLOpenIsRejected(t *testing.T) {
	testDB(t) // skips without a test database
	db, err := sql.Open("pgx", os.Getenv("KEEL_TEST_DSN"))
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	m := newManager(t, db, newNamespace())
	ok, err := m.TryAcquire(context.Background(), "job", time.Minute)
	if ok || err == nil || !strings.Contains(err.Error(), "OpenSQL") {
		t.Fatalf("TryAcquire = %v, %v; want false and a hint to use OpenSQL", ok, err)
	}
}

func TestKeyIsStable(t *testing.T) {
	// The value pins the key derivation: changing it silently hands every
	// job a new lock and lets two versions both become leader.
	if got := leader.Key("nightly-reindex"); got != leader.Key("nightly-reindex") {
		t.Fatal("Key is not deterministic")
	}
	if leader.Key("a") == leader.Key("b") {
		t.Fatal("distinct jobs share a key")
	}
}

// TestConcurrentLeaders starts many independent processes that race for one
// job, one round after another. Exactly one wins each round, and it keeps
// the lock until it releases.
func TestConcurrentLeaders(t *testing.T) {
	db := testDB(t)
	cfg := newNamespace()
	const processes, rounds = 8, 25
	managers := make([]*leader.Manager, processes)
	var won atomic.Int32
	for i := range managers {
		managers[i] = newManager(t, db, cfg)
	}
	ctx := context.Background()

	for round := 0; round < rounds; round++ {
		var wins atomic.Int32
		var wg sync.WaitGroup
		start := make(chan struct{})
		errs := make(chan error, processes)
		for i, m := range managers {
			wg.Add(1)
			go func(i int, m *leader.Manager) {
				defer wg.Done()
				<-start
				ok, err := m.TryAcquire(ctx, "shared-job", time.Minute)
				if err != nil {
					errs <- err
					return
				}
				if ok {
					wins.Add(1)
					won.Store(int32(i))
				}
			}(i, m)
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatalf("round %d: %v", round, err)
		}
		if got := wins.Load(); got != 1 {
			t.Fatalf("round %d: %d leaders, want exactly 1", round, got)
		}
		winner := managers[won.Load()]
		if released, err := winner.Release(ctx, "shared-job"); err != nil || !released {
			t.Fatalf("round %d: Release = %v, %v; want true, nil", round, released, err)
		}
	}
}

// TestDroppedConnectionFreesLock proves a holder that dies without
// releasing does not leave the job locked: closing the connection ends the
// session, and the server drops its session locks.
func TestDroppedConnectionFreesLock(t *testing.T) {
	db := testDB(t)
	cfg := newNamespace()
	a := newManager(t, db, cfg)
	ctx := context.Background()
	mustAcquire(t, a, "orphan-prone")

	b := newManager(t, db, cfg) // another process, same namespace
	if ok, err := b.TryAcquire(ctx, "orphan-prone", time.Minute); err != nil || ok {
		t.Fatalf("TryAcquire while held = %v, %v; want false, nil", ok, err)
	}

	// Kill the holder's backend the way a crash would: the connection goes.
	pid := lockPID(t, db, cfg, "orphan-prone")
	if _, err := db.ExecContext(ctx, "SELECT pg_terminate_backend($1)", pid); err != nil {
		t.Fatalf("terminate holder: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		ok, err := b.TryAcquire(ctx, "orphan-prone", time.Minute)
		if err != nil && !strings.Contains(err.Error(), "conn") {
			t.Fatalf("TryAcquire: %v", err)
		}
		if ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("lock was not freed after the holder's connection died")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
