package cron

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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

// schema applies the migration once per test binary.
var schemaOnce sync.Once

func schema(t *testing.T) *sql.DB {
	t.Helper()
	db := testDB(t)
	schemaOnce.Do(func() {
		migration, err := os.ReadFile("001_cron.sql")
		if err != nil {
			t.Fatalf("read migration: %v", err)
		}
		// Start from a database without the schema, as a fresh one is: the
		// migration must create it itself.
		ctx := context.Background()
		if _, err := db.ExecContext(ctx, "DROP SCHEMA IF EXISTS cron CASCADE"); err != nil {
			t.Fatalf("drop schema: %v", err)
		}
		if _, err := db.ExecContext(ctx, string(migration)); err != nil {
			t.Fatalf("apply migration: %v", err)
		}
	})
	return db
}

var scheduleSeq atomic.Int64

// scheduleID returns an ID no other test in the suite uses.
func scheduleID(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d", t.Name(), scheduleSeq.Add(1))
}

var namespaces atomic.Int32

// newLeaders returns a leader Manager with its own lock namespace, so a lock
// leaked by an earlier test cannot change the result.
func newLeaders(t *testing.T, db *sql.DB) *leader.Manager {
	t.Helper()
	return leadersIn(t, db, 3_000_000+namespaces.Add(1))
}

// leadersIn returns a leader Manager in the given namespace; Managers sharing
// one namespace contend for the same lock, as replicas of a service do.
func leadersIn(t *testing.T, db *sql.DB, namespace int32) *leader.Manager {
	t.Helper()
	m := leader.New(db, leader.Config{Namespace: namespace})
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// insertRun adds a pending due row.
func insertRun(t *testing.T, db *sql.DB, id string, due time.Time) {
	t.Helper()
	_, err := db.ExecContext(context.Background(),
		`INSERT INTO cron.schedule (schedule_id, next_due) VALUES ($1, $2)`, id, due)
	if err != nil {
		t.Fatalf("insert run: %v", err)
	}
}

type rowState struct {
	Status  string
	Owner   sql.NullString
	Attempt int
}

func readRow(t *testing.T, db *sql.DB, id string) rowState {
	t.Helper()
	var st rowState
	err := db.QueryRowContext(context.Background(),
		`SELECT status, owner, attempt FROM cron.schedule WHERE schedule_id = $1 AND next_due <= now()`, id).
		Scan(&st.Status, &st.Owner, &st.Attempt)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	return st
}

// countRows counts the runs recorded for a schedule.
func countRows(t *testing.T, db *sql.DB, id string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM cron.schedule WHERE schedule_id = $1`, id).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func readDue(t *testing.T, db *sql.DB, id string) time.Time {
	t.Helper()
	var due time.Time
	if err := db.QueryRowContext(context.Background(),
		`SELECT next_due FROM cron.schedule WHERE schedule_id = $1`, id).Scan(&due); err != nil {
		t.Fatalf("read due: %v", err)
	}
	return due
}

// readBackoff returns the backoff interval stored on the schedule's due row.
func readBackoff(t *testing.T, db *sql.DB, id string) time.Duration {
	t.Helper()
	var secs float64
	if err := db.QueryRowContext(context.Background(),
		`SELECT extract(epoch FROM backoff) FROM cron.schedule WHERE schedule_id = $1 AND next_due <= now()`, id).Scan(&secs); err != nil {
		t.Fatalf("read backoff: %v", err)
	}
	return time.Duration(secs * float64(time.Second))
}

// laterDue returns the next_due of the schedule's pending row; it fails when
// there is none.
func laterDue(t *testing.T, db *sql.DB, id string) time.Time {
	t.Helper()
	var due time.Time
	if err := db.QueryRowContext(context.Background(),
		`SELECT next_due FROM cron.schedule WHERE schedule_id = $1 AND status = 'pending'`, id).Scan(&due); err != nil {
		t.Fatalf("read pending due: %v", err)
	}
	return due
}

// waitFor polls cond until it holds or timeout passes.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// keepLive reports id's row as this scheduler's while its lease has not run
// out. Tests that run a claim directly have no Scheduler renewing it.
func keepLive(t *testing.T, db *sql.DB, id string) func() bool {
	return func() bool {
		var live bool
		if err := db.QueryRowContext(context.Background(),
			`SELECT status = 'running' AND lease_until IS NOT NULL AND lease_until > now() FROM cron.schedule WHERE schedule_id = $1`,
			id).Scan(&live); err != nil {
			t.Errorf("read lease: %v", err)
			return false
		}
		return live
	}
}

func newTestScheduler(t *testing.T, db *sql.DB, name string, jobs ...Job) *Scheduler {
	t.Helper()
	return newSchedulerAt(t, db, newLeaders(t, db), name, jobs...)
}

// newSchedulerAt builds a Scheduler over an existing leader Manager.
func newSchedulerAt(t *testing.T, db *sql.DB, m *leader.Manager, name string, jobs ...Job) *Scheduler {
	t.Helper()
	s, err := New(Config{DB: db, Leaders: m, Name: name, Jobs: jobs})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestOneSchedulerPerSlot races two replicas that share one Name and one
// leader namespace, as replicas of a service do: the leader lock lets one
// survey at a time, and the slot is claimed and run once.
func TestOneSchedulerPerSlot(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	insertRun(t, db, id, time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond))
	namespace := 3_000_000 + namespaces.Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var runs atomic.Int32
	// The run holds the lock across its execution and a while longer, so the
	// waiting replica is seen contending for it.
	job := Job{ID: id, Lease: 500 * time.Millisecond, Run: func(context.Context) error {
		runs.Add(1)
		time.Sleep(300 * time.Millisecond)
		return nil
	}}
	schedulers := []*Scheduler{
		newSchedulerAt(t, db, leadersIn(t, db, namespace), "cron", job),
		newSchedulerAt(t, db, leadersIn(t, db, namespace), "cron", job),
	}
	if schedulers[0].config.Owner == schedulers[1].config.Owner {
		t.Fatalf("both replicas own rows as %q; owners must differ per process", schedulers[0].config.Owner)
	}
	var wg sync.WaitGroup
	for _, s := range schedulers {
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.Run(ctx) }()
	}

	// Both replicas contend for the one lock named after their shared Name.
	key := uint64(leader.Key(leaderLockPrefix + schedulers[0].config.Name))
	classid := uint32(key>>33&(1<<30-1) | uint64(namespace&0x7fffffff)<<31)
	objid := uint32(key>>1 | key<<31)
	const probe = `SELECT count(*) FROM pg_locks
WHERE locktype = 'advisory' AND granted AND objsubid = 2 AND classid::bigint = $1 AND objid::bigint = $2`
	leaders := func() int {
		var n int
		if err := db.QueryRowContext(ctx, probe, classid, objid).Scan(&n); err != nil {
			t.Fatalf("probe leader lock: %v", err)
		}
		return n
	}
	// Exactly one replica holds the shared lock at every observation; the
	// other waits for it. Observing it repeatedly covers the whole run.
	waitFor(t, 15*time.Second, "one replica to hold the shared lock", func() bool {
		return leaders() == 1
	})
	waitFor(t, 15*time.Second, "the due run to succeed", func() bool {
		if n := leaders(); n > 1 {
			t.Fatalf("%d sessions hold the shared leader lock; want at most one", n)
		}
		return readRow(t, db, id).Status == string(StatusSucceeded)
	})
	if n := leaders(); n > 1 {
		cancel()
		wg.Wait()
		t.Fatalf("%d sessions hold the shared leader lock; want at most one", n)
	}
	cancel()
	wg.Wait()

	if got := runs.Load(); got != 1 {
		t.Fatalf("job ran %d times; want exactly 1", got)
	}
	if n := countRows(t, db, id); n != 1 {
		t.Fatalf("%d rows recorded; want 1", n)
	}
	if st := readRow(t, db, id); st.Attempt != 1 || st.Owner.Valid {
		t.Fatalf("row %+v; want attempt 1 with no owner", st)
	}
}

// TestReclaimAfterLease kills a scheduler mid-run and checks that the next
// leader reclaims the same row after its lease and runs it once.
func TestReclaimAfterLease(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	insertRun(t, db, id, time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond))

	// The first scheduler dies mid-run: its job blocks until its context
	// ends, so killing the scheduler leaves the row running with a lease
	// nothing renews.
	started := make(chan struct{})
	first := newTestScheduler(t, db, "cron", Job{ID: id, Lease: 400 * time.Millisecond,
		RenewInterval: 100 * time.Millisecond,
		Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}})
	kill, stopFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); _ = first.Run(kill) }()
	<-started
	stopFirst()
	<-firstDone

	st := readRow(t, db, id)
	if st.Status != string(StatusRunning) || !st.Owner.Valid || st.Attempt != 1 {
		t.Fatalf("interrupted row %+v; want running at attempt 1 under the dead scheduler", st)
	}
	// The row is not reclaimable while its lease is still live.
	if got := surveyDue(t, db); contains(got, id) {
		t.Fatalf("interrupted run %s surveyed before its lease expired", id)
	}

	var runs atomic.Int32
	second := newTestScheduler(t, db, "cron", Job{ID: id, Lease: 400 * time.Millisecond,
		Run: func(context.Context) error { runs.Add(1); return nil }})
	ctx, stopSecond := context.WithCancel(context.Background())
	defer stopSecond()
	secondDone := make(chan struct{})
	go func() { defer close(secondDone); _ = second.Run(ctx) }()

	waitFor(t, 15*time.Second, "the interrupted run to be reclaimed", func() bool {
		return readRow(t, db, id).Status == string(StatusSucceeded)
	})
	stopSecond()
	<-secondDone

	if got := runs.Load(); got != 1 {
		t.Fatalf("reclaimed run executed %d times; want 1", got)
	}
	if st := readRow(t, db, id); st.Attempt != 2 || st.Owner.Valid {
		t.Fatalf("row %+v after reclaim; want attempt 2 with no owner", st)
	}
	if n := countRows(t, db, id); n != 1 {
		t.Fatalf("%d rows recorded; the reclaim must reuse the due row", n)
	}
}

// TestLiveLeaseNotReclaimed: a running row with a live lease is not claimed,
// and becomes claimable only once the lease expires.
func TestLiveLeaseNotReclaimed(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	j := &Job{ID: id, Lease: time.Hour, Run: noop}
	s := newTestScheduler(t, db, "leader", *j)
	surveyDue(t, db) // proves the row is surveyed as due

	first, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if first.Attempt != 1 || first.ScheduleID != id || !first.Due.Equal(due) {
		t.Fatalf("claim = %+v", first)
	}
	if _, err := s.claim(context.Background(), j, dueRow{id, due}); !errors.Is(err, errNotDue) {
		t.Fatalf("second claim = %v; want errNotDue while the lease is live", err)
	}

	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET lease_until = now() - interval '1 second' WHERE schedule_id = $1`, id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	second, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim after expiry: %v", err)
	}
	if second.Attempt != 2 {
		t.Fatalf("reclaim attempt = %d; want 2 carried over", second.Attempt)
	}
}

// surveyDue lists the rows the scheduler's survey sees as due.
func surveyDue(t *testing.T, db *sql.DB) []string {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), SurveySQL, time.Now(), 1000)
	if err != nil {
		t.Fatalf("survey: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []string
	for rows.Next() {
		var r dueRow
		if err := rows.Scan(&r.ScheduleID, &r.Due); err != nil {
			t.Fatalf("survey: %v", err)
		}
		ids = append(ids, r.ScheduleID)
	}
	return ids
}

// TestBackoffThenDead: a failing run keeps its slot, waits out its backoff,
// and is marked dead at MaxAttempts.
func TestBackoffThenDead(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	// A due time whole minutes back: the retry gap picks the first slot past
	// the backoff from it.
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Minute)
	insertRun(t, db, id, due)
	var fails atomic.Int32
	failed := errors.New("job failed")
	j := &Job{ID: id, Every: time.Hour, MaxAttempts: 2, Backoff: time.Hour, Lease: time.Minute,
		Run: func(context.Context) error {
			fails.Add(1)
			return failed
		}}
	s := newTestScheduler(t, db, "leader", *j)
	s.report = func(error) {}
	// A survey runs only while its scheduler leads.
	if ok, err := s.leaders.TryAcquire(context.Background(), leaderLockPrefix+s.config.Name, time.Minute); !ok || err != nil {
		t.Fatalf("acquire leader lock: %v", err)
	}

	if err := s.survey(context.Background()); err != nil {
		t.Fatalf("survey: %v", err)
	}
	st := readRow(t, db, id)
	if st.Status != string(StatusFailed) || st.Owner.Valid || st.Attempt != 1 {
		t.Fatalf("after first failure %+v; want failed at attempt 1 with no owner", st)
	}
	// A failed recurring run lets the schedule carry on: the occurrence
	// after the retry is recorded, as recurring as this one. The claim stamped
	// the job's hour backoff on the row, so the next slot is one Every past
	// this due, covering the full retry.
	if got := readBackoff(t, db, id); got != time.Hour {
		t.Fatalf("row backoff %v after claim; want the job's %v", got, time.Hour)
	}
	checkSlot := func(next time.Time) {
		t.Helper()
		want := due.Add(time.Hour)
		if !next.Equal(want) {
			t.Fatalf("next slot %v; want %v, one every past the retry", next, want)
		}
		var every float64
		if err := db.QueryRowContext(context.Background(),
			`SELECT extract(epoch FROM every) FROM cron.schedule WHERE schedule_id = $1 AND next_due = $2`, id, want).Scan(&every); err != nil {
			t.Fatalf("read every: %v", err)
		}
		if int(every) != 3600 {
			t.Fatalf("next slot every %vs; want 3600s", every)
		}
	}
	checkSlot(laterDue(t, db, id))

	// The retry is held back: right after failing, only the row's backoff,
	// counted from when it failed, keeps it from being surveyed. The
	// schedule's next occurrence is an hour out, so it is not due either.
	if got := surveyDue(t, db); contains(got, id) {
		t.Fatalf("failed run %s surveyed inside its backoff", id)
	}
	// Inside the job's hour backoff it is still held, however long ago it
	// failed.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET updated_at = now() - interval '30 minutes' WHERE schedule_id = $1 AND status = 'failed'`, id); err != nil {
		t.Fatalf("pass default backoff: %v", err)
	}
	if got := surveyDue(t, db); contains(got, id) {
		t.Fatalf("failed run %s surveyed inside its backoff", id)
	}
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET updated_at = now() - interval '90 minutes' WHERE schedule_id = $1 AND status = 'failed'`, id); err != nil {
		t.Fatalf("pass backoff: %v", err)
	}
	if got := surveyDue(t, db); !contains(got, id) {
		t.Fatalf("failed run %s not surveyed after its backoff", id)
	}

	if err := s.survey(context.Background()); err != nil {
		t.Fatalf("survey: %v", err)
	}
	st = readRow(t, db, id)
	checkSlot(laterDue(t, db, id))
	if st.Status != string(StatusDead) || st.Attempt != 2 {
		t.Fatalf("after max attempts %+v; want dead at attempt 2", st)
	}
	if fails.Load() != 2 {
		t.Fatalf("job ran %d times; want 2", fails.Load())
	}
	// A dead run is never retried, however long it waits.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET updated_at = now() - interval '2 days' WHERE schedule_id = $1 AND status = 'dead'`, id); err != nil {
		t.Fatalf("pass backoff: %v", err)
	}
	if err := s.survey(context.Background()); err != nil {
		t.Fatalf("survey: %v", err)
	}
	if fails.Load() != 2 {
		t.Fatalf("job ran %d times; a dead run must not be retried", fails.Load())
	}
}

// TestFailedRunKeepsScheduleAlive: a failed run held back by its backoff
// does not strand its schedule when a retry slot already exists; the
// schedule carries on at its next occurrence.
func TestFailedRunKeepsScheduleAlive(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Hour).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	j := &Job{ID: id, Every: time.Hour, Backoff: time.Minute, MaxAttempts: 2, Lease: time.Minute,
		Run: func(context.Context) error { return errors.New("job failed") }}
	s := newTestScheduler(t, db, "leader", *j)
	run, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	s.runClaimed(context.Background(), j, run, keepLive(t, db, id))
	if st := readRow(t, db, id); st.Status != string(StatusFailed) {
		t.Fatalf("row %+v; want failed", st)
	}
	// The retry slot already exists while this run is still pending, so the
	// failure has to schedule the occurrence after it.
	var following int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM cron.schedule WHERE schedule_id = $1 AND next_due > $2`, id, due).Scan(&following); err != nil {
		t.Fatalf("count later slots: %v", err)
	}
	if following != 1 {
		t.Fatalf("%d slots after the failed run; want the occurrence after its retry", following)
	}
}

func noop(context.Context) error { return nil }

func contains(ids []string, id string) bool {
	for _, s := range ids {
		if s == id {
			return true
		}
	}
	return false
}

// TestSucceededRunNotClaimed: a succeeded row never matches a claim, even
// with an expired lease, so one slot is never run twice.
func TestSucceededRunNotClaimed(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	insertRun(t, db, id, time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond))
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET status = 'succeeded', lease_until = now() - interval '1 minute', owner = 'old' WHERE schedule_id = $1`, id); err != nil {
		t.Fatalf("mark succeeded: %v", err)
	}
	s := newTestScheduler(t, db, "leader", Job{ID: id, Run: noop})
	if _, err := s.claim(context.Background(), &Job{ID: id, Lease: time.Minute}, dueRow{id, readDue(t, db, id)}); !errors.Is(err, errNotDue) {
		t.Fatalf("claim of succeeded row = %v; want errNotDue", err)
	}
}

// TestLostLeaderCancelsRun: a job whose scheduler lost the leader lock is
// cancelled and records nothing of its own; the row keeps its lease until it
// expires, and the next leader reclaims it.
func TestLostLeaderCancelsRun(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	started := make(chan struct{})
	j := &Job{ID: id, Lease: time.Minute, RenewInterval: 20 * time.Millisecond,
		Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}}
	s := newTestScheduler(t, db, "cron", *j)
	var reported []error
	var mu sync.Mutex
	s.report = func(err error) { mu.Lock(); reported = append(reported, err); mu.Unlock() }
	name := leaderLockPrefix + s.config.Name
	if ok, err := s.leaders.TryAcquire(context.Background(), name, time.Minute); !ok || err != nil {
		t.Fatalf("acquire leader lock: %v", err)
	}
	run, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); s.execute(context.Background(), j, run) }()
	<-started
	if _, err := s.leaders.Release(context.Background(), name); err != nil {
		t.Fatalf("release leader lock: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("job was not cancelled after losing the leader lock")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 1 {
		t.Fatalf("reported %d errors; want 1 halt", len(reported))
	}
	// A lost leader lock renews as "not held" with no error, which the halt
	// must still report as a cause rather than a nil error.
	var jobErr *JobError
	if !errors.As(reported[0], &jobErr) {
		t.Fatalf("reported %v; want a *JobError", reported[0])
	}
	if !errors.Is(jobErr, errHalted) {
		t.Fatalf("reported cause %v; want errHalted", jobErr.Err)
	}
	if st := readRow(t, db, id); st.Status != string(StatusRunning) || st.Owner.String != s.config.Owner {
		t.Fatalf("row %+v; want left running under the lost leader", st)
	}
}

// TestLostLeaseCancelsRun: a job whose lease another leader took is
// cancelled and records nothing of its own, so it cannot overwrite the
// leader that reclaimed it.
func TestLostLeaseCancelsRun(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	var reported atomic.Int32
	started := make(chan struct{})
	j := &Job{ID: id, Lease: time.Minute, RenewInterval: 20 * time.Millisecond,
		Run: func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		}}
	s := newTestScheduler(t, db, "leader", *j)
	s.report = func(error) { reported.Add(1) }
	// The run renews only while its Scheduler leads.
	if ok, err := s.leaders.TryAcquire(context.Background(), leaderLockPrefix+s.config.Name, time.Minute); !ok || err != nil {
		t.Fatalf("acquire leader lock: %v", err)
	}
	run, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	done := make(chan struct{})
	keep := keepLive(t, db, id)
	go func() { defer close(done); s.runClaimed(context.Background(), j, run, keep) }()
	<-started
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET owner = 'other', lease_until = now() + interval '1 minute' WHERE schedule_id = $1`, id); err != nil {
		t.Fatalf("hand over lease: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("job was not cancelled after losing its lease")
	}
	if reported.Load() != 1 {
		t.Fatalf("reported %d errors; want 1 halt", reported.Load())
	}
	if st := readRow(t, db, id); st.Status != string(StatusRunning) || st.Owner.String != "other" {
		t.Fatalf("row %+v; want running under the new owner", st)
	}
}

// TestRenewKeepsLiveLease: a long run renews its lease, so another leader
// never sees it as reclaimable while it works.
func TestRenewKeepsLiveLease(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	working := make(chan struct{})
	finish := make(chan struct{})
	j := &Job{ID: id, Lease: 300 * time.Millisecond, RenewInterval: 50 * time.Millisecond,
		Run: func(context.Context) error {
			close(working)
			<-finish
			return nil
		}}
	s := newTestScheduler(t, db, "leader", *j)
	// The run renews only while its Scheduler leads.
	if ok, err := s.leaders.TryAcquire(context.Background(), leaderLockPrefix+s.config.Name, time.Minute); !ok || err != nil {
		t.Fatalf("acquire leader lock: %v", err)
	}
	run, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); s.runClaimed(context.Background(), j, run, keepLive(t, db, id)) }()
	<-working
	var expiry time.Time
	waitFor(t, 5*time.Second, "the lease to be renewed", func() bool {
		var e time.Time
		if err := db.QueryRowContext(context.Background(),
			`SELECT lease_until FROM cron.schedule WHERE schedule_id = $1`, id).Scan(&e); err != nil {
			t.Fatalf("read lease: %v", err)
		}
		if !expiry.IsZero() && e.After(expiry) {
			return true
		}
		expiry = e
		return false
	})
	if got := surveyDue(t, db); contains(got, id) {
		t.Fatalf("renewed run %s surveyed as reclaimable", id)
	}
	close(finish)
	<-done
	if st := readRow(t, db, id); st.Status != string(StatusSucceeded) {
		t.Fatalf("row %+v; want succeeded", st)
	}
}

// TestRunSchedulesNextOccurrence: a successful recurring run records one row
// for its next due time.
func TestRunSchedulesNextOccurrence(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	j := &Job{ID: id, Every: time.Hour, Lease: time.Minute, Run: noop}
	s := newTestScheduler(t, db, "leader", *j)
	// Claim directly: a survey round would run the job itself.
	run, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	s.runClaimed(context.Background(), j, run, keepLive(t, db, id))

	if st := readRow(t, db, id); st.Status != string(StatusSucceeded) {
		t.Fatalf("row %+v; want succeeded", st)
	}
	if n := countRows(t, db, id); n != 2 {
		t.Fatalf("%d rows; want this run plus one next occurrence", n)
	}
	var pending int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM cron.schedule WHERE schedule_id = $1 AND status = 'pending'`, id).Scan(&pending); err != nil {
		t.Fatalf("count pending: %v", err)
	}
	if pending != 1 {
		t.Fatalf("%d pending rows; want 1", pending)
	}
}

// TestSchemaOnePendingPerSchedule: the schema keeps one unclaimed pending run
// per schedule on its own.
func TestSchemaOnePendingPerSchedule(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	insertRun(t, db, id, time.Now().UTC())
	if _, err := db.ExecContext(context.Background(),
		`INSERT INTO cron.schedule (schedule_id, next_due) VALUES ($1, now() + interval '1 hour')`, id); err == nil {
		t.Fatal("schema accepted a second pending row for one schedule")
	}
}

func TestNewValidates(t *testing.T) {
	db := schema(t)
	noop := func(context.Context) error { return nil }
	valid := Config{DB: db, Leaders: newLeaders(t, db), Name: "cron", Jobs: []Job{{ID: "a", Run: noop, Every: time.Hour}}}
	s, err := New(valid)
	if err != nil {
		t.Fatalf("New(valid): %v", err)
	}
	j := s.config.Jobs[0]
	if j.MaxAttempts != DefaultMaxAttempts || j.Backoff != DefaultBackoff || j.Lease != DefaultLease ||
		j.RenewInterval != DefaultLease/defaultRenewDivisor {
		t.Fatalf("defaults not applied: %+v", j)
	}
	// The owner names this process, not just the service, so two replicas
	// never share one.
	other, err := New(valid)
	if err != nil {
		t.Fatalf("New(valid): %v", err)
	}
	if !strings.HasPrefix(s.config.Owner, "cron-") || len(s.config.Owner) <= len("cron-") {
		t.Fatalf("owner = %q; want the scheduler name and a per-process suffix", s.config.Owner)
	}
	if other.config.Owner == s.config.Owner {
		t.Fatalf("two schedulers share owner %q", s.config.Owner)
	}

	bad := map[string]Config{
		"no db":          {Leaders: valid.Leaders, Name: "cron"},
		"no leaders":     {DB: db, Name: "cron"},
		"no name":        {DB: db, Leaders: valid.Leaders},
		"empty id":       {DB: db, Leaders: valid.Leaders, Name: "cron", Jobs: []Job{{Run: noop}}},
		"no run":         {DB: db, Leaders: valid.Leaders, Name: "cron", Jobs: []Job{{ID: "a"}}},
		"duplicate id":   {DB: db, Leaders: valid.Leaders, Name: "cron", Jobs: []Job{{ID: "a", Run: noop}, {ID: "a", Run: noop}}},
		"renew too long": {DB: db, Leaders: valid.Leaders, Name: "cron", Jobs: []Job{{ID: "a", Run: noop, Lease: time.Second, RenewInterval: time.Second}}},
		"negative every": {DB: db, Leaders: valid.Leaders, Name: "cron", Jobs: []Job{{ID: "a", Run: noop, Every: -time.Second}}},
		"negative lease": {DB: db, Leaders: valid.Leaders, Name: "cron", Jobs: []Job{{ID: "a", Run: noop, Lease: -time.Second}}},
	}
	for name, cfg := range bad {
		if _, err := New(cfg); err == nil {
			t.Errorf("%s: New accepted an invalid config", name)
		}
	}
}

func TestJobErrorWrapsCause(t *testing.T) {
	cause := errors.New("boom")
	err := &JobError{ScheduleID: "s", Due: time.Unix(0, 0), Attempt: 2, Err: cause}
	if !errors.Is(err, cause) || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("JobError = %v; want it to wrap and print the cause", err)
	}
	if !strings.Contains(errHalted.Error(), "lease") {
		t.Fatalf("halt error = %v; want it to mention the lease", errHalted)
	}
}

// TestClaimPayload reads a real jsonb column: a claimed row carries its JSON
// text as is, and a NULL payload claims as nil.
func TestClaimPayload(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET payload = '{"job": "sync", "n": 2}'::jsonb WHERE schedule_id = $1`, id); err != nil {
		t.Fatalf("set payload: %v", err)
	}
	j := &Job{ID: id, Lease: time.Minute, Run: noop}
	s := newTestScheduler(t, db, "leader", *j)
	run, err := s.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	var want map[string]any
	if err := json.Unmarshal(run.Payload, &want); err != nil || want["job"] != "sync" || want["n"] != float64(2) {
		t.Fatalf("claimed payload %q; want the stored JSON object", run.Payload)
	}

	nullID := scheduleID(t)
	insertRun(t, db, nullID, due)
	j.ID = nullID
	run, err = s.claim(context.Background(), j, dueRow{nullID, due})
	if err != nil {
		t.Fatalf("claim NULL payload: %v", err)
	}
	if run.Payload != nil {
		t.Fatalf("NULL payload claimed as %q; want nil", run.Payload)
	}
}

// TestStaleOwnerFenced: two replicas of one service share a Name, and so the
// leader lock, but not an owner. Once the second has reclaimed the first's
// run, the first's renew and finish statements match no row.
func TestStaleOwnerFenced(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	namespace := 3_000_000 + namespaces.Add(1)
	j := &Job{ID: id, Lease: time.Minute, Run: noop}
	a := newSchedulerAt(t, db, leadersIn(t, db, namespace), "cron", *j)
	b := newSchedulerAt(t, db, leadersIn(t, db, namespace), "cron", *j)
	if a.config.Owner == b.config.Owner {
		t.Fatalf("replicas share owner %q", a.config.Owner)
	}

	if _, err := a.claim(context.Background(), j, dueRow{id, due}); err != nil {
		t.Fatalf("claim by A: %v", err)
	}
	// A's lease lapses and B, the new leader, reclaims the row.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET lease_until = now() - interval '1 second' WHERE schedule_id = $1`, id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	reclaimed, err := b.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("reclaim by B: %v", err)
	}
	if reclaimed.Attempt != 2 {
		t.Fatalf("reclaim attempt %d; want 2", reclaimed.Attempt)
	}

	exec := func(query string, args ...any) int64 {
		t.Helper()
		res, err := db.ExecContext(context.Background(), query, args...)
		if err != nil {
			t.Fatalf("exec: %v", err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			t.Fatalf("rows affected: %v", err)
		}
		return n
	}
	now := time.Now()
	if n := exec(renewSQL, id, due, now.Add(time.Minute), now, a.config.Owner); n != 0 {
		t.Fatalf("stale renew touched %d rows after reclamation; want 0", n)
	}
	if n := exec(finishSQL, id, due, StatusFailed, now, a.config.Owner); n != 0 {
		t.Fatalf("stale finish touched %d rows after reclamation; want 0", n)
	}
	if n := exec(finishSQL, id, due, StatusSucceeded, now, b.config.Owner); n != 1 {
		t.Fatalf("current owner's finish touched %d rows; want 1", n)
	}
}

// TestReclaimedRowNotFinished: a failure recorded after another scheduler
// reclaimed the run is neither reported nor allowed to schedule on.
func TestReclaimedRowNotFinished(t *testing.T) {
	db := schema(t)
	id := scheduleID(t)
	due := time.Now().Add(-time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, id, due)
	j := &Job{ID: id, Every: time.Hour, Lease: time.Minute, Run: noop}
	a := newTestScheduler(t, db, "leader", *j)
	run, err := a.claim(context.Background(), j, dueRow{id, due})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Another scheduler takes the row while A's run is in flight.
	if _, err := db.ExecContext(context.Background(),
		`UPDATE cron.schedule SET owner = 'other', lease_until = now() + interval '1 minute' WHERE schedule_id = $1`, id); err != nil {
		t.Fatalf("hand over row: %v", err)
	}
	var reported atomic.Int32
	a.report = func(error) { reported.Add(1) }
	a.finish(context.Background(), j, run, errors.New("failed"))

	if reported.Load() != 0 {
		t.Fatalf("reported %d errors for a reclaimed run; want 0", reported.Load())
	}
	if st := readRow(t, db, id); st.Status != string(StatusRunning) || st.Owner.String != "other" {
		t.Fatalf("row %+v; want it left to the owner that reclaimed it", st)
	}
	if n := countRows(t, db, id); n != 1 {
		t.Fatalf("%d rows; a reclaimed run must not schedule on", n)
	}
}

// TestLostLeaderStopsSurvey: a scheduler that loses the lock between due rows
// starts no further run.
func TestLostLeaderStopsSurvey(t *testing.T) {
	db := schema(t)
	first, second := scheduleID(t), scheduleID(t)
	base := time.Now().Add(-2 * time.Minute).UTC().Truncate(time.Microsecond)
	insertRun(t, db, first, base)
	insertRun(t, db, second, base.Add(time.Minute)) // surveyed after first
	var started atomic.Int32
	surveying := make(chan struct{})
	release := make(chan struct{})
	run := func(context.Context) error {
		if started.Add(1) == 1 {
			close(surveying)
		}
		<-release
		return nil
	}
	j1 := &Job{ID: first, Lease: time.Minute, Run: run}
	j2 := &Job{ID: second, Lease: time.Minute, Run: run}
	s := newSchedulerAt(t, db, newLeaders(t, db), "cron", *j1, *j2)
	name := leaderLockPrefix + s.config.Name
	if ok, err := s.leaders.TryAcquire(context.Background(), name, time.Minute); !ok || err != nil {
		t.Fatalf("acquire leader lock: %v", err)
	}
	done := make(chan error)
	go func() { done <- s.survey(context.Background()) }()
	select {
	case <-surveying:
	case <-time.After(5 * time.Second):
		t.Fatal("first run did not start")
	}
	if _, err := s.leaders.Release(context.Background(), name); err != nil {
		t.Fatalf("release leader lock: %v", err)
	}
	close(release)
	select {
	case err := <-done:
		if !errors.Is(err, errLockLost) {
			t.Fatalf("survey ended with %v; want errLockLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("survey kept running after leadership loss")
	}
	if st := readRow(t, db, second); st.Status != string(StatusPending) {
		t.Fatalf("second row %+v; want it left pending for the next leader", st)
	}
}
