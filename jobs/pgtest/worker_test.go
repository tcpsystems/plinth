package jobs_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/jobs"
)

// TestTwoWorkersProcessJobsExactlyOnce enqueues 50 jobs and lets two
// independent Worker loops run against the same table until every job has
// been handled. The leased claims must keep the jobs apart: each job is
// handled once, completes, and its attempt count never exceeds one.
func TestTwoWorkersProcessJobsExactlyOnce(t *testing.T) {
	db := testDB(t)
	const nJobs = 50

	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin enqueue transaction: %v", err)
	}
	ids := make([]int64, 0, nJobs)
	for i := range nJobs {
		id, err := jobs.Enqueue(ctx, tx, jobs.Job{Kind: "pgwork", MaxAttempts: 3})
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("enqueue job %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit enqueues: %v", err)
	}

	var mu sync.Mutex
	runs := make(map[int64]int, nJobs)
	handled := make(chan int64, nJobs+1)

	newWorker := func() *jobs.Worker {
		w := jobs.NewWorker(&jobs.SQLStore{DB: db}, jobs.Options{
			Batch:        10,
			Concurrency:  4,
			PollInterval: 10 * time.Millisecond,
			Lease:        time.Minute,
		})
		w.Register("pgwork", func(_ context.Context, j jobs.Job) error {
			mu.Lock()
			runs[j.ID]++
			mu.Unlock()
			select {
			case handled <- j.ID:
			default:
			}
			return nil
		})
		return w
	}

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w1, w2 := newWorker(), newWorker()
	errCh := make(chan error, 2)
	go func() { errCh <- w1.Run(runCtx) }()
	go func() { errCh <- w2.Run(runCtx) }()

	// Bounded wait: stop once all 50 distinct jobs have been handled, not
	// after a fixed sleep.
	seen := make(map[int64]bool, nJobs)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	for len(seen) < nJobs {
		select {
		case id := <-handled:
			seen[id] = true
		case <-timeout.C:
			t.Fatalf("only %d of %d jobs handled in 30s", len(seen), nJobs)
		}
	}

	cancel()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("Run returned %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("worker did not stop after cancellation")
		}
	}

	// Every claim consumed an attempt, so exactly-once handling must show as
	// attempts == 1 on every row; no job may sit failed or unhandled.
	if got := count(t, db, `SELECT count(*) FROM jobs WHERE state = 'done'`); got != nJobs {
		t.Errorf("done jobs %d, want %d", got, nJobs)
	}
	if got := count(t, db, `SELECT count(*) FROM jobs WHERE state <> 'done'`); got != 0 {
		t.Errorf("jobs not done: %d", got)
	}
	if got := count(t, db, `SELECT COALESCE(SUM(attempts), 0)::int FROM jobs`); got != nJobs {
		t.Errorf("total attempts %d, want %d (no job claimed twice)", got, nJobs)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if runs[id] != 1 {
			t.Errorf("job %d handled %d times, want exactly once", id, runs[id])
		}
	}
	t.Logf("both loops drained; handler ran %d times across the two workers", len(ids))
}

// TestSlowHandlersProcessJobsExactlyOnce runs a batch that takes longer in
// total than its lease, while every individual handler stays inside the
// lease. A poll that claimed the whole batch at once would leave the later
// jobs queued behind the semaphore until their leases expired, and the
// store would hand them out again: both duplicate attempts and duplicate
// runs. Claiming only the free slots keeps every live claim inside its
// lease.
func TestSlowHandlersProcessJobsExactlyOnce(t *testing.T) {
	db := testDB(t)
	const nJobs = 10
	const handlerTime = 300 * time.Millisecond
	const lease = time.Second // five serial waves take 1.5s, longer than this

	ctx := t.Context()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin enqueue transaction: %v", err)
	}
	ids := make([]int64, 0, nJobs)
	for i := range nJobs {
		id, err := jobs.Enqueue(ctx, tx, jobs.Job{Kind: "pgslow", MaxAttempts: 3})
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("enqueue job %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit enqueues: %v", err)
	}

	var mu sync.Mutex
	runs := make(map[int64]int, nJobs)
	handled := make(chan int64, nJobs+1)

	w := jobs.NewWorker(&jobs.SQLStore{DB: db}, jobs.Options{
		Batch:        nJobs,
		Concurrency:  2,
		PollInterval: 10 * time.Millisecond,
		Lease:        lease,
	})
	w.Register("pgslow", func(_ context.Context, j jobs.Job) error {
		time.Sleep(handlerTime)
		mu.Lock()
		runs[j.ID]++
		mu.Unlock()
		select {
		case handled <- j.ID:
		default:
		}
		return nil
	})

	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(runCtx) }()

	seen := make(map[int64]bool, nJobs)
	timeout := time.NewTimer(30 * time.Second)
	defer timeout.Stop()
	for len(seen) < nJobs {
		select {
		case id := <-handled:
			seen[id] = true
		case <-timeout.C:
			t.Fatalf("only %d of %d jobs handled in 30s", len(seen), nJobs)
		}
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}

	if got := count(t, db, `SELECT count(*) FROM jobs WHERE state = 'done'`); got != nJobs {
		t.Errorf("done jobs %d, want %d", got, nJobs)
	}
	if got := count(t, db, `SELECT count(*) FROM jobs WHERE state <> 'done'`); got != 0 {
		t.Errorf("jobs not done: %d", got)
	}
	if got := count(t, db, `SELECT COALESCE(SUM(attempts), 0)::int FROM jobs`); got != nJobs {
		t.Errorf("total attempts %d, want %d (no job claimed twice)", got, nJobs)
	}
	mu.Lock()
	defer mu.Unlock()
	for _, id := range ids {
		if runs[id] != 1 {
			t.Errorf("job %d handled %d times, want exactly once", id, runs[id])
		}
	}
}
