package jobs_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/tcpsystems/plinth/jobs"
)

// testDB opens the disposable database from KEEL_TEST_DSN (tools/testdb.sh go
// ...), applies jobs/schema.sql and returns the pool.
//
// Every call empties the table first: tests share one database under -race.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("KEEL_TEST_DSN")
	if dsn == "" {
		t.Fatal("KEEL_TEST_DSN is not set; run through tools/testdb.sh go")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	db.SetMaxOpenConns(32)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	t.Cleanup(func() { _ = db.Close() })
	schema, err := os.ReadFile("../schema.sql")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), string(schema)); err != nil {
		t.Fatalf("apply schema: %v", err)
	}
	if _, err := db.ExecContext(t.Context(), `TRUNCATE jobs`); err != nil {
		t.Fatalf("truncate jobs: %v", err)
	}
	return db
}

func count(t *testing.T, db *sql.DB, q string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(t.Context(), q, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return n
}

func stateOf(t *testing.T, db *sql.DB, id int64) (state string, attempts int, lastError sql.NullString, runAt, lockedUntil sql.NullTime) {
	t.Helper()
	err := db.QueryRowContext(t.Context(),
		`SELECT state, attempts, last_error, run_at, locked_until FROM jobs WHERE id = $1`, id).
		Scan(&state, &attempts, &lastError, &runAt, &lockedUntil)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func TestEnqueueCommitsAndRollsBackWithTransaction(t *testing.T) {
	db := testDB(t)

	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	kept, err := jobs.Enqueue(t.Context(), tx, jobs.Job{Kind: "send", Payload: []byte(`{"to":"a"}`), MaxAttempts: 3})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var kind, state string
	var attempts int
	if err := db.QueryRowContext(t.Context(), `SELECT kind, state, attempts FROM jobs WHERE id = $1`, kept).Scan(&kind, &state, &attempts); err != nil {
		t.Fatalf("committed job not visible: %v", err)
	}
	if kind != "send" || state != "queued" || attempts != 0 {
		t.Fatalf("kind=%q state=%q attempts=%d", kind, state, attempts)
	}

	tx, err = db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	dropped, err := jobs.Enqueue(t.Context(), tx, jobs.Job{Kind: "send"})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, `SELECT count(*) FROM jobs WHERE id = $1`, dropped); n != 0 {
		t.Fatalf("rolled back job is visible (%d rows)", n)
	}
	// The id of a rolled-back insert leaves a gap; that is expected of a
	// sequence and not a duplicate.
	if dropped <= kept {
		t.Fatalf("ids did not increase: kept=%d dropped=%d", kept, dropped)
	}
}

func TestEnqueueValidation(t *testing.T) {
	db := testDB(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := jobs.Enqueue(t.Context(), nil, jobs.Job{Kind: "k"}); err == nil {
		t.Error("nil transaction accepted")
	}
	if _, err := jobs.Enqueue(t.Context(), tx, jobs.Job{}); err == nil {
		t.Error("empty kind accepted")
	}
	if _, err := jobs.Enqueue(t.Context(), tx, jobs.Job{Kind: "k", Payload: []byte(`{bad`)}); err == nil {
		t.Error("invalid JSON accepted")
	}
}

func TestClaimDefaultsAndInvalidArguments(t *testing.T) {
	db := testDB(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := jobs.Enqueue(t.Context(), tx, jobs.Job{Kind: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if _, attempts, _, _, _ := stateOf(t, db, id); attempts != 0 {
		t.Fatalf("default max_attempts gave attempts=%d", attempts)
	}
	if n := count(t, db, `SELECT max_attempts FROM jobs WHERE id = $1`, id); n != jobs.DefaultMaxAttempts {
		t.Fatalf("max_attempts=%d, want %d", n, jobs.DefaultMaxAttempts)
	}
	for _, c := range []struct {
		name  string
		n     int
		lease time.Duration
	}{{"zero count", 0, time.Second}, {"negative count", -1, time.Second}, {"zero lease", 1, 0}, {"negative lease", 1, -time.Second}} {
		if _, err := jobs.Claim(t.Context(), db, c.n, c.lease); err == nil {
			t.Errorf("%s: claim accepted", c.name)
		}
	}
	if _, err := jobs.Claim(t.Context(), nil, 1, time.Second); err == nil {
		t.Error("nil database accepted")
	}
	if n := count(t, db, `SELECT count(*) FROM jobs WHERE state <> 'queued'`); n != 0 {
		t.Fatalf("an invalid claim changed %d rows", n)
	}
}

func TestClaimRespectsRunAt(t *testing.T) {
	db := testDB(t)
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	future, err := jobs.Enqueue(t.Context(), tx, jobs.Job{Kind: "later", RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := jobs.Claim(t.Context(), db, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("claimed a job that is not due: %+v", got)
	}
	if state, _, _, _, _ := stateOf(t, db, future); state != "queued" {
		t.Fatalf("state=%q", state)
	}
}

func TestConcurrentClaimersNeverShareAJob(t *testing.T) {
	db := testDB(t)
	const total = 60
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i++ {
		if _, err := jobs.Enqueue(t.Context(), tx, jobs.Job{Kind: "work"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	const claimers = 8
	var (
		mu   sync.Mutex
		seen = map[int64]int{}
		wg   sync.WaitGroup
	)
	start := make(chan struct{})
	errs := make(chan error, claimers)
	for i := 0; i < claimers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				got, err := jobs.Claim(context.Background(), db, 5, time.Minute)
				if err != nil {
					errs <- err
					return
				}
				if len(got) == 0 {
					return
				}
				mu.Lock()
				for _, j := range got {
					seen[j.ID]++
					if j.State != jobs.Running || j.Attempts != 1 {
						errs <- errors.New("claimed job has wrong state or attempts")
					}
				}
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(seen) != total {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d claimed %d times", id, n)
		}
	}
}

func TestCompleteMarksDone(t *testing.T) {
	db := testDB(t)
	got := enqueueAndClaim(t, db, time.Minute, jobs.Job{Kind: "k"})
	if err := jobs.Complete(t.Context(), db, got[0].ID); err != nil {
		t.Fatal(err)
	}
	state, attempts, _, _, lockedUntil := stateOf(t, db, got[0].ID)
	if state != "done" || attempts != 1 || lockedUntil.Valid {
		t.Fatalf("state=%q attempts=%d locked_until=%v", state, attempts, lockedUntil)
	}
	if again, err := jobs.Claim(t.Context(), db, 10, time.Minute); err != nil || len(again) != 0 {
		t.Fatalf("done job re-claimed: %+v %v", again, err)
	}
}

func TestFailReschedulesWithBackoffThenFails(t *testing.T) {
	db := testDB(t)
	got := enqueueAndClaim(t, db, time.Minute, jobs.Job{Kind: "flaky", MaxAttempts: 2}, jobs.Job{Kind: "bystander"})
	var j, bystander jobs.Job
	for _, c := range got {
		if c.Kind == "flaky" {
			j = c
		} else {
			bystander = c
		}
	}

	wait := time.Hour
	err := jobs.Fail(t.Context(), db, j.ID, errors.New("first attempt failed"), func(int) time.Duration { return wait })
	if err != nil {
		t.Fatal(err)
	}
	state, attempts, lastErr, runAt, lockedUntil := stateOf(t, db, j.ID)
	if state != "queued" || attempts != 1 || lastErr.String != "first attempt failed" || lockedUntil.Valid {
		t.Fatalf("after first failure: state=%q attempts=%d err=%q locked=%v", state, attempts, lastErr.String, lockedUntil)
	}
	if d := time.Until(runAt.Time); d < wait-5*time.Second {
		t.Fatalf("run_at only %v ahead, want about %v", d, wait)
	}
	if again, err := jobs.Claim(t.Context(), db, 10, time.Minute); err != nil || len(again) != 0 {
		t.Fatalf("rescheduled job claimed before backoff elapsed: %+v %v", again, err)
	}

	// Second attempt: make it due now and claim it again.
	if _, err := db.ExecContext(t.Context(), `UPDATE jobs SET run_at = now() WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}
	again, err := jobs.Claim(t.Context(), db, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].ID != j.ID || again[0].Attempts != 2 {
		t.Fatalf("second claim: %+v", again)
	}
	if err := jobs.Fail(t.Context(), db, j.ID, errors.New("second attempt failed"), func(int) time.Duration { return wait }); err != nil {
		t.Fatal(err)
	}
	state, attempts, lastErr, _, lockedUntil = stateOf(t, db, j.ID)
	if state != "failed" || attempts != 2 || lastErr.String != "second attempt failed" || lockedUntil.Valid {
		t.Fatalf("after exhausted attempts: state=%q attempts=%d err=%q", state, attempts, lastErr.String)
	}
	if n := count(t, db, `SELECT count(*) FROM jobs WHERE state = 'failed'`); n != 1 {
		t.Fatalf("failed jobs visible: %d", n)
	}
	if again, err := jobs.Claim(t.Context(), db, 10, time.Minute); err != nil || len(again) != 0 {
		t.Fatalf("failed job claimed: %+v %v", again, err)
	}

	// The failure of one job left the other claim alone.
	if state, _, _, _, _ := stateOf(t, db, bystander.ID); state != "running" {
		t.Fatalf("bystander state=%q", state)
	}
}

func TestExpiredLeaseIsClaimableAgain(t *testing.T) {
	db := testDB(t)
	got := enqueueAndClaim(t, db, 100*time.Millisecond, jobs.Job{Kind: "k"})
	// While the lease is live no one else may claim it.
	if again, err := jobs.Claim(t.Context(), db, 10, time.Minute); err != nil || len(again) != 0 {
		t.Fatalf("live lease claimed: %+v %v", again, err)
	}
	if _, _, _, _, lockedUntil := stateOf(t, db, got[0].ID); !lockedUntil.Valid {
		t.Fatal("claim did not set locked_until")
	}
	time.Sleep(250 * time.Millisecond)
	again, err := jobs.Claim(t.Context(), db, 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 1 || again[0].ID != got[0].ID || again[0].Attempts != 2 {
		t.Fatalf("expired lease claim: %+v", again)
	}
	if err := jobs.Complete(t.Context(), db, got[0].ID); err != nil {
		t.Fatal(err)
	}
	if state, _, _, _, _ := stateOf(t, db, got[0].ID); state != "done" {
		t.Fatalf("state=%q", state)
	}
}

func TestFailErrors(t *testing.T) {
	db := testDB(t)
	if err := jobs.Fail(t.Context(), db, 0, nil, nil); err == nil {
		t.Error("nil error accepted")
	}
	if err := jobs.Fail(t.Context(), db, 1<<62, errors.New("x"), nil); !errors.Is(err, sql.ErrNoRows) {
		t.Errorf("unknown id: %v", err)
	}
}

// enqueueAndClaim commits all jobs in one transaction, claims exactly as many
// as were enqueued with lease, and returns the claimed rows.
func enqueueAndClaim(t *testing.T, db *sql.DB, lease time.Duration, want ...jobs.Job) []jobs.Job {
	t.Helper()
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range want {
		if _, err := jobs.Enqueue(t.Context(), tx, j); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got, err := jobs.Claim(t.Context(), db, len(want), lease)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("claimed %d jobs, want %d", len(got), len(want))
	}
	return got
}
