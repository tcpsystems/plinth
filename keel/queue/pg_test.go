package queue_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/tcpsystems/plinth/keel/queue"
)

var tables struct {
	sync.Mutex
	n int
}

func nextTableName() string {
	tables.Lock()
	defer tables.Unlock()
	tables.n++
	return fmt.Sprintf("job_%d_%d", os.Getpid(), tables.n)
}

// testDB opens the disposable database from KEEL_TEST_DSN
// (tools/testdb.sh go ...).
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("KEEL_TEST_DSN")
	if dsn == "" {
		t.Skip("KEEL_TEST_DSN is not set; run through tools/testdb.sh go")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	// Enough connections for the concurrency tests, which claim in parallel.
	db.SetMaxOpenConns(64)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newTable creates a job table with the Default schema under a unique name
// and returns it as a Schema, so tests never share rows through -count runs.
func newTable(t *testing.T, db *sql.DB) queue.Schema {
	t.Helper()
	name := nextTableName()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE `+name+` (
		id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		priority    integer     NOT NULL DEFAULT 100,
		next_due    timestamptz NOT NULL DEFAULT now(),
		state       text        NOT NULL DEFAULT 'pending',
		lease_epoch bigint      NOT NULL DEFAULT 0
	)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.ExecContext(ctx, `CREATE INDEX ON `+name+` (state, priority, next_due, id)`); err != nil {
		t.Fatalf("create index: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+name); err != nil {
			t.Logf("drop %s: %v", name, err)
		}
	})
	// Default names the columns; take them so the table's schema is complete.
	s := queue.Default
	s.Table = name
	return s
}

func newQueue(t *testing.T, db *sql.DB, s queue.Schema) *queue.Queue {
	t.Helper()
	q, err := queue.NewWithSchema(db, s)
	if err != nil {
		t.Fatalf("NewWithSchema: %v", err)
	}
	return q
}

// addDue adds n due rows at priority p and returns their ids in insert order.
// next_due is in the past and distinct per row, so claim order is
// deterministic.
func addDue(t *testing.T, db *sql.DB, s queue.Schema, p, n int) []int64 {
	t.Helper()
	rows, err := db.Query(`INSERT INTO `+s.Table+` (priority, next_due)
		SELECT $1, now() - make_interval(secs => g) FROM generate_series(1, $2) AS g
		RETURNING id`, p, n)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("insert: %v", err)
	}
	return ids
}

// insertNotDue adds a row that is not due for an hour and returns its id.
func insertNotDue(t *testing.T, db *sql.DB, s queue.Schema, p int) int64 {
	t.Helper()
	var id int64
	err := db.QueryRow(`INSERT INTO `+s.Table+` (priority, next_due)
		VALUES ($1, now() + interval '1 hour') RETURNING id`, p).Scan(&id)
	if err != nil {
		t.Fatalf("insert not due: %v", err)
	}
	return id
}

type rowState struct {
	state string
	epoch int64
}

func readRow(t *testing.T, db *sql.DB, s queue.Schema, id int64) rowState {
	t.Helper()
	var r rowState
	state, epoch, table, key := s.State, s.Epoch, s.Table, s.ID
	query := `SELECT t.` + state + `, t.` + epoch + ` FROM ` + table + ` AS t WHERE t.` + key + ` = $1`
	err := db.QueryRow(query, id).
		Scan(&r.state, &r.epoch)
	if err != nil {
		t.Fatalf("read row %d: %v", id, err)
	}
	return r
}

func ids(cs []queue.Claimed) []int64 {
	out := make([]int64, len(cs))
	for i, c := range cs {
		out[i] = c.ID
	}
	return out
}

// sameSet reports whether a and b hold the same ids, each once up to order.
func sameSet(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[int64]int, len(a))
	for _, id := range a {
		seen[id]++
	}
	for _, id := range b {
		seen[id]--
		if seen[id] < 0 {
			return false
		}
	}
	return true
}

func TestClaimBatchOrdersAndClaims(t *testing.T) {
	db := testDB(t)
	s := newTable(t, db)
	q := newQueue(t, db, s)
	ctx := context.Background()

	low := addDue(t, db, s, 20, 3)
	high := addDue(t, db, s, 5, 3)
	notDue := insertNotDue(t, db, s, 1)

	// addDue inserts next_due now()-1s, now()-2s, ... so the last id of each
	// group is the earliest due and sorts first. ClaimN compares as sets: the
	// ids it takes are ordered, the order it returns them in is not.
	got := ids(mustClaimN(t, q, ctx, 5))
	want := append(append([]int64{}, high...), low[1], low[2])
	if !sameSet(got, want) {
		t.Fatalf("ClaimN(5) = %v; want the priority 5 rows and the two earliest priority 20 rows, i.e. %v", got, want)
	}

	// The next claim takes what is left, and never the row that is not due.
	got = ids(mustClaimN(t, q, ctx, 5))
	if !sameSet(got, low[:1]) {
		t.Fatalf("second ClaimN = %v; want only %v", got, low[:1])
	}
	if st := readRow(t, db, s, notDue); st.state != "pending" {
		t.Fatalf("not-due row state = %q; want pending", st.state)
	}
	if got := mustClaimN(t, q, ctx, 5); len(got) != 0 {
		t.Fatalf("ClaimN with nothing due = %v; want empty", got)
	}
}

func TestClaimBatchBumpsEpochAndCapsBatch(t *testing.T) {
	db := testDB(t)
	s := newTable(t, db)
	q := newQueue(t, db, s)
	ctx := context.Background()
	addDue(t, db, s, 100, 3)

	got := mustClaimN(t, q, ctx, 2)
	if len(got) != 2 {
		t.Fatalf("ClaimN(2) returned %d rows; want 2", len(got))
	}
	for _, c := range got {
		if c.Epoch != 1 {
			t.Errorf("row %d epoch = %d; want 1 after the first claim", c.ID, c.Epoch)
		}
		if st := readRow(t, db, s, c.ID); st.state != "claimed" || st.epoch != 1 {
			t.Errorf("row %d stored as %+v; want claimed epoch 1", c.ID, st)
		}
	}

	// n below 1 claims a single row; n above MaxBatch is clamped, not an error.
	if got := mustClaimN(t, q, ctx, 0); len(got) != 1 {
		t.Fatalf("ClaimN(0) returned %d rows; want 1", len(got))
	}
	addDue(t, db, s, 100, queue.MaxBatch+5)
	if got := mustClaimN(t, q, ctx, queue.MaxBatch+5); len(got) != queue.MaxBatch {
		t.Fatalf("ClaimN(MaxBatch+5) returned %d rows; want %d", len(got), queue.MaxBatch)
	}
}

// TestClaimBatchConcurrentDisjoint runs many claimers on one table: every
// claimer gets rows no other claimer got in the same round, each row is
// claimed exactly once, and every row carries the epoch its single claim set.
func TestClaimBatchConcurrentDisjoint(t *testing.T) {
	db := testDB(t)
	s := newTable(t, db)
	q := newQueue(t, db, s)
	ctx := context.Background()
	const claimers, per = 8, 64
	addDue(t, db, s, 1, claimers*per)

	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make(chan error, claimers)
	for range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			total := 0
			for total < per {
				// Mix batch sizes so the paths for a full and a short batch
				// both run under contention.
				n := min(1+total%7, per-total)
				got, err := q.ClaimN(ctx, n)
				if err != nil {
					errs <- err
					return
				}
				if len(got) == 0 {
					errs <- fmt.Errorf("claim ran dry after %d of %d rows", total, per)
					return
				}
				mu.Lock()
				for _, c := range got {
					seen[c.ID]++
				}
				mu.Unlock()
				total += len(got)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if len(seen) != claimers*per {
		t.Fatalf("%d distinct rows claimed; want %d", len(seen), claimers*per)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("row %d claimed %d times; want once", id, n)
		}
	}
	for id := range seen {
		if st := readRow(t, db, s, id); st != (rowState{"claimed", 1}) {
			t.Fatalf("row %d stored as %+v; want claimed at epoch 1", id, st)
		}
	}
}

func mustClaimN(t *testing.T, q *queue.Queue, ctx context.Context, n int) []queue.Claimed {
	t.Helper()
	got, err := q.ClaimN(ctx, n)
	if err != nil {
		t.Fatalf("ClaimN(%d): %v", n, err)
	}
	return got
}

func TestStaleOwnerCannotSettle(t *testing.T) {
	db := testDB(t)
	s := newTable(t, db)
	q := newQueue(t, db, s)
	ctx := context.Background()
	addDue(t, db, s, 1, 1)

	old := mustClaimN(t, q, ctx, 1)[0]
	// The old owner loses its lease: the row goes back to pending and a
	// second owner claims it, as a reaper plus a new claim would.
	if err := q.Fail(ctx, old.ID, old.Epoch, 0); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	fresh := mustClaimN(t, q, ctx, 1)[0]
	if fresh.ID != old.ID || fresh.Epoch <= old.Epoch {
		t.Fatalf("reclaim = %+v; want row %d with an epoch above %d", fresh, old.ID, old.Epoch)
	}

	// The stale owner's Settle and Fail are refused and change nothing.
	if err := q.Settle(ctx, old.ID, old.Epoch); !errors.Is(err, queue.ErrStale) {
		t.Fatalf("stale Settle = %v; want ErrStale", err)
	}
	if err := q.Fail(ctx, old.ID, old.Epoch, time.Minute); !errors.Is(err, queue.ErrStale) {
		t.Fatalf("stale Fail = %v; want ErrStale", err)
	}
	if st := readRow(t, db, s, old.ID); st != (rowState{"claimed", fresh.Epoch}) {
		t.Fatalf("row after stale attempts = %+v; want claimed epoch %d", st, fresh.Epoch)
	}

	// The current owner settles; the row is done and both leases are dead.
	if err := q.Settle(ctx, fresh.ID, fresh.Epoch); err != nil {
		t.Fatalf("current Settle: %v", err)
	}
	if st := readRow(t, db, s, old.ID); st.state != "done" {
		t.Fatalf("row after Settle = %+v; want done", st)
	}
	if err := q.Settle(ctx, fresh.ID, fresh.Epoch); !errors.Is(err, queue.ErrStale) {
		t.Fatalf("repeated Settle = %v; want ErrStale", err)
	}
	if err := q.Settle(ctx, old.ID, old.Epoch); !errors.Is(err, queue.ErrStale) {
		t.Fatalf("stale Settle after done = %v; want ErrStale", err)
	}
}

func TestFailBackoffDelaysReclaim(t *testing.T) {
	db := testDB(t)
	s := newTable(t, db)
	q := newQueue(t, db, s)
	ctx := context.Background()
	addDue(t, db, s, 1, 1)

	c := mustClaimN(t, q, ctx, 1)[0]
	if err := q.Fail(ctx, c.ID, c.Epoch, time.Hour); err != nil {
		t.Fatalf("Fail: %v", err)
	}
	if st := readRow(t, db, s, c.ID); st.state != "pending" || st.epoch != c.Epoch+1 {
		t.Fatalf("after Fail = %+v; want pending epoch %d", st, c.Epoch+1)
	}
	if got := mustClaimN(t, q, ctx, 1); len(got) != 0 {
		t.Fatalf("row in backoff was claimed: %v", got)
	}
	if err := q.Fail(ctx, c.ID, c.Epoch, time.Hour); !errors.Is(err, queue.ErrStale) {
		t.Fatalf("repeated Fail = %v; want ErrStale", err)
	}
}

func TestSettleOnlyOwnersRow(t *testing.T) {
	db := testDB(t)
	s := newTable(t, db)
	q := newQueue(t, db, s)
	ctx := context.Background()
	ids := addDue(t, db, s, 1, 2)

	got := mustClaimN(t, q, ctx, 2)
	// ClaimN returns its rows in no defined order; pick them by id.
	byID := map[int64]queue.Claimed{}
	for _, c := range got {
		byID[c.ID] = c
	}
	if err := q.Settle(ctx, byID[ids[0]].ID, byID[ids[0]].Epoch); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	// The other row is untouched.
	if st := readRow(t, db, s, ids[1]); st != (rowState{"claimed", byID[ids[1]].Epoch}) {
		t.Fatalf("sibling row = %+v; want still claimed", st)
	}
	// An unknown epoch on the right row is stale, not an error.
	if err := q.Settle(ctx, ids[1], byID[ids[1]].Epoch+10); !errors.Is(err, queue.ErrStale) {
		t.Fatalf("Settle with a future epoch = %v; want ErrStale", err)
	}
	if st := readRow(t, db, s, ids[1]); st.state != "claimed" {
		t.Fatalf("row after stale Settle = %+v; want claimed", st)
	}
}

func TestCustomSchema(t *testing.T) {
	db := testDB(t)
	s := queue.Schema{Table: "jobs_alt", ID: "key", Priority: "prio", NextDue: "run_at",
		State: "phase", Epoch: "fence", Pending: "waiting", Claimed: "busy"}
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `DROP TABLE IF EXISTS jobs_alt; CREATE TABLE jobs_alt (
		key bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, prio int, run_at timestamptz,
		phase text NOT NULL DEFAULT 'waiting', fence bigint NOT NULL DEFAULT 0)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS jobs_alt`) })
	if _, err := db.ExecContext(ctx, `INSERT INTO jobs_alt (prio, run_at) VALUES (1, now() - interval '1 minute')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	q := newQueue(t, db, s)
	c := mustClaimN(t, q, ctx, 3)[0]
	if err := q.Settle(ctx, c.ID, c.Epoch); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	var phase string
	if err := db.QueryRowContext(ctx, `SELECT j.phase FROM jobs_alt AS j WHERE j.key = $1`, c.ID).Scan(&phase); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(phase, "done") {
		t.Fatalf("phase = %q; want done", phase)
	}
}
