package queue_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/riverqueue/river"

	"github.com/tcpsystems/plinth/keel/queue"
)

type workArgs struct {
	N   int    `json:"n"`
	Key string `json:"key" river:"unique"`
}

func (workArgs) Kind() string { return "test.work" }

// riverSchema creates a private schema for River's tables so tests share
// neither jobs nor leaders, and returns its name.
func riverSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	name := nextTableName() + "_river"
	if _, err := db.Exec(`CREATE SCHEMA ` + name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DROP SCHEMA IF EXISTS ` + name + ` CASCADE`); err != nil {
			t.Logf("drop schema %s: %v", name, err)
		}
	})
	if err := queue.Migrate(context.Background(), db, name); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return name
}

// startClient builds and starts a client, stopping it when the test ends.
func startClient(t *testing.T, cfg queue.Config) *queue.Client {
	t.Helper()
	if cfg.FetchPollInterval == 0 {
		cfg.FetchPollInterval = 50 * time.Millisecond
		cfg.FetchCooldown = 20 * time.Millisecond
	}
	c, err := queue.NewClient(cfg)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := c.Stop(ctx); err != nil {
			t.Logf("stop: %v", err)
		}
	})
	return c
}

func waitFor(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func count(t *testing.T, q interface {
	QueryRow(string, ...any) *sql.Row
}, query string, args ...any) int {
	t.Helper()
	var n int
	if err := q.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func TestInsertTxRollsBackWithTheTransaction(t *testing.T) {
	db := testDB(t)
	schema := riverSchema(t, db)
	c, err := queue.NewClient(queue.Config{DB: db, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	jobs := fmt.Sprintf(`SELECT count(*) FROM %s.river_job`, schema)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.InsertTx(ctx, tx, workArgs{N: 1, Key: "a"}, nil); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if n := count(t, tx, jobs); n != 1 {
		t.Fatalf("job invisible inside its own transaction: %d", n)
	}
	if n := count(t, db, jobs); n != 0 {
		t.Fatalf("uncommitted job visible to other sessions: %d", n)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, jobs); n != 0 {
		t.Fatalf("rolled-back job survived: %d", n)
	}

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.InsertTx(ctx, tx, workArgs{N: 2, Key: "b"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if n := count(t, db, jobs); n != 1 {
		t.Fatalf("committed job missing: %d", n)
	}
	if _, err := c.InsertTx(ctx, nil, workArgs{}, nil); err == nil {
		t.Error("nil transaction accepted")
	}
}

func TestInsertManyIdempotencyKeys(t *testing.T) {
	db := testDB(t)
	schema := riverSchema(t, db)
	c, err := queue.NewClient(queue.Config{DB: db, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	batch := func(keys ...string) []river.InsertManyParams {
		var ps []river.InsertManyParams
		for i, k := range keys {
			ps = append(ps, river.InsertManyParams{Args: workArgs{N: i, Key: k}, InsertOpts: queue.Idempotent(nil)})
		}
		return ps
	}
	res, err := c.InsertMany(ctx, batch("a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if res[0].UniqueSkippedAsDuplicate || res[1].UniqueSkippedAsDuplicate {
		t.Fatal("first insert of a key was skipped")
	}

	// A retry of the whole batch, as a caller would after a lost response,
	// adds nothing, including inside a caller transaction.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err = c.InsertManyTx(ctx, tx, batch("a", "b", "c"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !res[0].UniqueSkippedAsDuplicate || !res[1].UniqueSkippedAsDuplicate || res[2].UniqueSkippedAsDuplicate {
		t.Fatalf("retry results = %v %v %v", res[0].UniqueSkippedAsDuplicate, res[1].UniqueSkippedAsDuplicate, res[2].UniqueSkippedAsDuplicate)
	}
	if n := count(t, db, fmt.Sprintf(`SELECT count(*) FROM %s.river_job`, schema)); n != 3 {
		t.Fatalf("%d jobs, want 3 distinct keys", n)
	}
}

func TestTwoClientsClaimEachJobOnce(t *testing.T) {
	const jobs = 60
	db1, db2 := testDB(t), testDB(t)
	schema := riverSchema(t, db1)

	var mu sync.Mutex
	runs := map[int][]string{} // job number -> clients that ran it
	byClient := map[string]int{}
	newWorkers := func(name string) *river.Workers {
		w := river.NewWorkers()
		river.AddWorker(w, river.WorkFunc(func(_ context.Context, j *river.Job[workArgs]) error {
			time.Sleep(25 * time.Millisecond)
			mu.Lock()
			runs[j.Args.N] = append(runs[j.Args.N], name)
			byClient[name]++
			mu.Unlock()
			return nil
		}))
		return w
	}
	qs := map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 2}}

	var ps []river.InsertManyParams
	for i := 0; i < jobs; i++ {
		ps = append(ps, river.InsertManyParams{Args: workArgs{N: i, Key: fmt.Sprint(i)}})
	}
	ins, err := queue.NewClient(queue.Config{DB: db1, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ins.InsertMany(context.Background(), ps); err != nil {
		t.Fatal(err)
	}

	startClient(t, queue.Config{DB: db1, Schema: schema, Workers: newWorkers("a"), Queues: qs})
	startClient(t, queue.Config{DB: db2, Schema: schema, Workers: newWorkers("b"), Queues: qs})

	waitFor(t, 30*time.Second, "all jobs to run", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(runs) == jobs
	})
	mu.Lock()
	defer mu.Unlock()
	for n, who := range runs {
		if len(who) != 1 {
			t.Errorf("job %d ran %d times (%v)", n, len(who), who)
		}
	}
	if byClient["a"] == 0 || byClient["b"] == 0 {
		t.Errorf("work was not shared: %v", byClient)
	}
}

func TestKilledWorkersJobIsRescuedAfterItsLease(t *testing.T) {
	const lease = time.Second
	db1, db2 := testDB(t), testDB(t)
	schema := riverSchema(t, db1)

	// Client a claims the job and never finishes it, as a process that died
	// mid-job would. It cannot lead, so it cannot rescue its own job.
	claimed := make(chan struct{}, 4)
	hang := make(chan struct{})
	aw := river.NewWorkers()
	river.AddWorker(aw, river.WorkFunc(func(_ context.Context, _ *river.Job[workArgs]) error {
		claimed <- struct{}{}
		<-hang // ignores cancellation on purpose
		return nil
	}))
	// One slot, held by the hung job: a is dead weight and cannot take the retry.
	a := startClient(t, queue.Config{DB: db1, Schema: schema, Workers: aw, Lease: lease, NoLeader: true,
		Queues: map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 1}}})
	t.Cleanup(func() { close(hang) }) // runs before a's Stop, which waits for the worker

	if _, err := a.InsertMany(context.Background(), []river.InsertManyParams{{Args: workArgs{N: 1, Key: "k"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-claimed:
	case <-time.After(10 * time.Second):
		t.Fatal("client a never claimed the job")
	}
	state := func() (st string, attempt int, by int) {
		err := db1.QueryRow(fmt.Sprintf(`SELECT state::text, attempt, cardinality(attempted_by) FROM %s.river_job`, schema)).Scan(&st, &attempt, &by)
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	if st, _, _ := state(); st != "running" {
		t.Fatalf("job is %s while its worker holds it", st)
	}

	// Client b arrives after the lease ran out. Its leader finds the stale
	// running job, schedules a retry and works it.
	time.Sleep(lease + 300*time.Millisecond)
	var ran atomic.Int32
	bw := river.NewWorkers()
	river.AddWorker(bw, river.WorkFunc(func(_ context.Context, _ *river.Job[workArgs]) error {
		ran.Add(1)
		return nil
	}))
	startClient(t, queue.Config{DB: db2, Schema: schema, Workers: bw, Lease: lease, BackoffBase: 10 * time.Millisecond, BackoffMax: time.Second})

	waitFor(t, 45*time.Second, "the rescued job to complete", func() bool {
		st, _, _ := state()
		return st == "completed"
	})
	if _, attempt, by := state(); attempt != 2 || by != 2 || ran.Load() != 1 {
		t.Fatalf("attempt=%d claimed by %d clients, rescue worker ran %d times; want 2, 2, 1", attempt, by, ran.Load())
	}
}

func TestJobIsDeadAfterMaxAttempts(t *testing.T) {
	db := testDB(t)
	schema := riverSchema(t, db)
	var runs atomic.Int32
	w := river.NewWorkers()
	river.AddWorker(w, river.WorkFunc(func(_ context.Context, _ *river.Job[workArgs]) error {
		runs.Add(1)
		return fmt.Errorf("always failing")
	}))
	c := startClient(t, queue.Config{
		DB: db, Schema: schema, Workers: w, MaxAttempts: 2,
		BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond,
	})
	if _, err := c.InsertMany(context.Background(), []river.InsertManyParams{{Args: workArgs{N: 1, Key: "x"}}}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 45*time.Second, "the job to be dead", func() bool {
		return count(t, db, fmt.Sprintf(`SELECT count(*) FROM %s.river_job WHERE state = $1`, schema), string(queue.Dead)) == 1
	})
	if n := runs.Load(); n != 2 {
		t.Fatalf("ran %d times, want exactly MaxAttempts=2", n)
	}
	// Dead jobs stay dead.
	time.Sleep(300 * time.Millisecond)
	if n := runs.Load(); n != 2 {
		t.Fatalf("dead job ran again: %d runs", n)
	}
}

type tickArgs struct{}

func (tickArgs) Kind() string { return "test.tick" }

func TestPeriodicJobsRunWithOneLeader(t *testing.T) {
	db1, db2 := testDB(t), testDB(t)
	schema := riverSchema(t, db1)
	var ticks atomic.Int32
	newCfg := func(db *sql.DB) queue.Config {
		w := river.NewWorkers()
		river.AddWorker(w, river.WorkFunc(func(_ context.Context, _ *river.Job[tickArgs]) error {
			ticks.Add(1)
			return nil
		}))
		return queue.Config{
			DB: db, Schema: schema, Workers: w,
			Periodic: []*river.PeriodicJob{queue.PeriodicJob(queue.Periodic{
				ID: "tick", Schedule: queue.Every(500 * time.Millisecond), RunOnStart: true,
				Make: func() (river.JobArgs, *river.InsertOpts) { return tickArgs{}, nil },
			})},
		}
	}
	startClient(t, newCfg(db1))
	startClient(t, newCfg(db2))

	waitFor(t, 30*time.Second, "three periodic runs", func() bool { return ticks.Load() >= 3 })

	// One leader row exists, so both clients cannot be inserting ticks.
	if n := count(t, db1, fmt.Sprintf(`SELECT count(*) FROM %s.river_leader`, schema)); n != 1 {
		t.Fatalf("%d leader rows, want exactly 1", n)
	}
}

func TestAddPeriodicValidates(t *testing.T) {
	c, err := queue.NewClient(queue.Config{DB: &sql.DB{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AddPeriodic(queue.Periodic{ID: "x"}); err == nil {
		t.Error("incomplete periodic job accepted")
	}
}
