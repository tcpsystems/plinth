package queue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/keel/queue"
)

// newJobs creates a uniquely named job table from JobsDDL and returns Jobs
// over it.
func newJobs(t *testing.T, db *sql.DB, cfg queue.JobsConfig) *queue.Jobs {
	t.Helper()
	cfg.Table = "qjob_" + nextTableName()
	ddl, err := queue.JobsDDL(cfg.Table)
	if err != nil {
		t.Fatalf("JobsDDL: %v", err)
	}
	if _, err := db.Exec(ddl); err != nil {
		t.Fatalf("create job table: %v", err)
	}
	t.Cleanup(func() {
		if _, err := db.Exec(`DROP TABLE IF EXISTS ` + cfg.Table); err != nil {
			t.Logf("drop %s: %v", cfg.Table, err)
		}
	})
	j, err := queue.NewJobs(db, cfg)
	if err != nil {
		t.Fatalf("NewJobs: %v", err)
	}
	tableOf[j] = cfg.Table
	return j
}

var tableOf = map[*queue.Jobs]string{}

func enqueue(t *testing.T, j *queue.Jobs, n queue.NewJob) queue.Job {
	t.Helper()
	job, _, err := j.Enqueue(context.Background(), n)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return job
}

// expireLease makes a running job's lease already expired.
func expireLease(t *testing.T, db *sql.DB, j *queue.Jobs, id int64) {
	t.Helper()
	if _, err := db.Exec(`UPDATE `+tableOf[j]+` SET lease_expires_at = now() - interval '1 minute' WHERE id = $1`, id); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func TestConcurrentClaim(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{})
	ctx := context.Background()
	const total, workers = 300, 16
	for i := 0; i < total; i++ {
		enqueue(t, j, queue.NewJob{Kind: "work", Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))})
	}

	var (
		mu     sync.Mutex
		seen   = map[int64]int{}
		wg     sync.WaitGroup
		errsCh = make(chan error, workers)
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := j.Claim(ctx, 7, queue.Filter{})
				if err != nil {
					errsCh <- err
					return
				}
				if len(jobs) == 0 {
					return
				}
				for _, job := range jobs {
					mu.Lock()
					seen[job.ID]++
					mu.Unlock()
					if err := j.Complete(ctx, job.ID, job.Epoch, json.RawMessage(`{"ok":true}`)); err != nil {
						errsCh <- err
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errsCh)
	for err := range errsCh {
		t.Fatalf("worker: %v", err)
	}
	if len(seen) != total {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("job %d claimed %d times", id, n)
		}
	}
	d, err := j.Depth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if d.Done != total || d.Ready != 0 || d.Running != 0 {
		t.Errorf("depth = %+v, want %d done", d, total)
	}
}

func TestEnqueueIdempotencyKey(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{})
	ctx := context.Background()

	var wg sync.WaitGroup
	var mu sync.Mutex
	created, ids := 0, map[int64]bool{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, c, err := j.Enqueue(ctx, queue.NewJob{Kind: "email", IdempotencyKey: "order-1"})
			if err != nil {
				t.Errorf("Enqueue: %v", err)
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if c {
				created++
			}
			ids[job.ID] = true
		}()
	}
	wg.Wait()
	if created != 1 || len(ids) != 1 {
		t.Fatalf("created %d rows, %d distinct ids; want exactly one", created, len(ids))
	}
	// Same key under another kind is a different job; no key never dedupes.
	other, c, err := j.Enqueue(ctx, queue.NewJob{Kind: "sms", IdempotencyKey: "order-1"})
	if err != nil || !c {
		t.Fatalf("other kind: created=%v err=%v", c, err)
	}
	if other.IdempotencyKey != "order-1" {
		t.Errorf("IdempotencyKey = %q", other.IdempotencyKey)
	}
	a, b := enqueue(t, j, queue.NewJob{Kind: "x"}), enqueue(t, j, queue.NewJob{Kind: "x"})
	if a.ID == b.ID {
		t.Error("jobs without a key were deduplicated")
	}
}

func TestEnqueueRejectsBadInput(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{})
	ctx := context.Background()
	if _, _, err := j.Enqueue(ctx, queue.NewJob{}); err == nil {
		t.Error("empty kind accepted")
	}
	if _, _, err := j.Enqueue(ctx, queue.NewJob{Kind: "x", Payload: json.RawMessage(`{bad`)}); err == nil {
		t.Error("invalid payload accepted")
	}
}

func TestClaimFilterAndRunAt(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{})
	ctx := context.Background()
	enqueue(t, j, queue.NewJob{Kind: "a"})
	enqueue(t, j, queue.NewJob{Kind: "b"})
	enqueue(t, j, queue.NewJob{Kind: "a", RunAt: time.Now().Add(time.Hour)})

	got, err := j.Claim(ctx, 10, queue.Filter{Kinds: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Kind != "a" || got[0].State != queue.StateRunning || got[0].Attempts != 1 {
		t.Fatalf("claim a = %+v", got)
	}
	if got[0].LeaseExpiresAt.IsZero() {
		t.Error("claimed job has no lease expiry")
	}
	rest, err := j.Claim(ctx, 10, queue.Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rest) != 1 || rest[0].Kind != "b" {
		t.Fatalf("claim rest = %+v, want only kind b (the other a is not due)", rest)
	}
}

func TestLeaseExpiryReclaimAndFencing(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{})
	ctx := context.Background()
	enqueue(t, j, queue.NewJob{Kind: "k"})

	first, err := j.Claim(ctx, 1, queue.Filter{})
	if err != nil || len(first) != 1 {
		t.Fatalf("first claim: %v %v", first, err)
	}
	// Live lease: nobody else may claim it.
	if again, _ := j.Claim(ctx, 1, queue.Filter{}); len(again) != 0 {
		t.Fatalf("claimed a job under a live lease: %+v", again)
	}
	expireLease(t, db, j, first[0].ID)
	second, err := j.Claim(ctx, 1, queue.Filter{})
	if err != nil || len(second) != 1 {
		t.Fatalf("reclaim: %v %v", second, err)
	}
	if second[0].Attempts != 2 || second[0].Epoch <= first[0].Epoch {
		t.Errorf("reclaim attempts=%d epoch=%d (was %d)", second[0].Attempts, second[0].Epoch, first[0].Epoch)
	}
	// The first owner lost the lease and must not act on the job.
	if err := j.Complete(ctx, first[0].ID, first[0].Epoch, nil); !errors.Is(err, queue.ErrStale) {
		t.Errorf("stale Complete = %v, want ErrStale", err)
	}
	if err := j.Heartbeat(ctx, first[0].ID, first[0].Epoch); !errors.Is(err, queue.ErrStale) {
		t.Errorf("stale Heartbeat = %v, want ErrStale", err)
	}
	if _, err := j.Fail(ctx, first[0].ID, first[0].Epoch, errors.New("late")); !errors.Is(err, queue.ErrStale) {
		t.Errorf("stale Fail = %v, want ErrStale", err)
	}
	if err := j.Complete(ctx, second[0].ID, second[0].Epoch, json.RawMessage(`"fine"`)); err != nil {
		t.Fatalf("Complete by owner: %v", err)
	}
	if err := j.Complete(ctx, second[0].ID, second[0].Epoch, nil); !errors.Is(err, queue.ErrStale) {
		t.Errorf("double Complete = %v, want ErrStale", err)
	}
}

func TestHeartbeatExtendsLease(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{LeaseTTL: time.Hour})
	ctx := context.Background()
	enqueue(t, j, queue.NewJob{Kind: "k"})
	got, err := j.Claim(ctx, 1, queue.Filter{})
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	expireLease(t, db, j, got[0].ID)
	if err := j.Heartbeat(ctx, got[0].ID, got[0].Epoch); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if again, _ := j.Claim(ctx, 1, queue.Filter{}); len(again) != 0 {
		t.Fatalf("job reclaimed after a heartbeat: %+v", again)
	}
	stuck, err := j.Stuck(ctx, 0, 10)
	if err != nil || len(stuck) != 0 {
		t.Errorf("Stuck after heartbeat = %v, %v", stuck, err)
	}
}

func TestRetryBackoffDoubles(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{BackoffBase: time.Minute, BackoffMax: time.Hour})
	ctx := context.Background()
	enqueue(t, j, queue.NewJob{Kind: "k", MaxAttempts: 10})

	for attempt, want := range []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute} {
		got, err := j.Claim(ctx, 1, queue.Filter{})
		if err != nil || len(got) != 1 {
			t.Fatalf("attempt %d claim: %v %v", attempt+1, got, err)
		}
		state, err := j.Fail(ctx, got[0].ID, got[0].Epoch, errors.New("boom"))
		if err != nil || state != queue.StatePending {
			t.Fatalf("Fail = %q, %v", state, err)
		}
		var delay float64
		if err := db.QueryRow(`SELECT extract(epoch FROM run_at - now()) FROM `+tableOf[j]+` WHERE id = $1`, got[0].ID).Scan(&delay); err != nil {
			t.Fatal(err)
		}
		if d := time.Duration(delay * float64(time.Second)); d < want-5*time.Second || d > want {
			t.Errorf("attempt %d: retry in %v, want about %v", attempt+1, d, want)
		}
		// Not claimable until the backoff passes.
		if again, _ := j.Claim(ctx, 1, queue.Filter{}); len(again) != 0 {
			t.Fatalf("attempt %d: claimed during backoff", attempt+1)
		}
		if _, err := db.Exec(`UPDATE `+tableOf[j]+` SET run_at = now() WHERE id = $1`, got[0].ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBackoffCapped(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{BackoffBase: time.Minute, BackoffMax: 3 * time.Minute})
	ctx := context.Background()
	job := enqueue(t, j, queue.NewJob{Kind: "k", MaxAttempts: 100})
	if _, err := db.Exec(`UPDATE `+tableOf[j]+` SET attempts = 60 WHERE id = $1`, job.ID); err != nil {
		t.Fatal(err)
	}
	got, err := j.Claim(ctx, 1, queue.Filter{})
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	if _, err := j.Fail(ctx, got[0].ID, got[0].Epoch, errors.New("boom")); err != nil {
		t.Fatalf("Fail with large attempts: %v", err)
	}
	var delay float64
	if err := db.QueryRow(`SELECT extract(epoch FROM run_at - now()) FROM `+tableOf[j]+` WHERE id = $1`, job.ID).Scan(&delay); err != nil {
		t.Fatal(err)
	}
	if delay > 180 || delay < 170 {
		t.Errorf("delay = %vs, want capped at 180s", delay)
	}
}

func TestDeadLetter(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{BackoffBase: time.Millisecond, BackoffMax: time.Millisecond})
	ctx := context.Background()
	enqueue(t, j, queue.NewJob{Kind: "k", MaxAttempts: 3})

	var state string
	for attempt := 1; attempt <= 3; attempt++ {
		var got []queue.Job
		var err error
		for deadline := time.Now().Add(5 * time.Second); len(got) == 0 && time.Now().Before(deadline); {
			if got, err = j.Claim(ctx, 1, queue.Filter{}); err != nil {
				t.Fatal(err)
			}
			if len(got) == 0 {
				time.Sleep(5 * time.Millisecond)
			}
		}
		if len(got) != 1 || got[0].Attempts != attempt {
			t.Fatalf("attempt %d: claim = %+v", attempt, got)
		}
		var ferr error
		if state, ferr = j.Fail(ctx, got[0].ID, got[0].Epoch, fmt.Errorf("failure %d", attempt)); ferr != nil {
			t.Fatal(ferr)
		}
	}
	if state != queue.StateDead {
		t.Fatalf("state after max attempts = %q, want dead", state)
	}
	if got, _ := j.Claim(ctx, 1, queue.Filter{}); len(got) != 0 {
		t.Fatalf("dead job was claimed: %+v", got)
	}
	dead, err := j.DeadLetters(ctx, 10)
	if err != nil || len(dead) != 1 {
		t.Fatalf("DeadLetters = %v, %v", dead, err)
	}
	if dead[0].LastError != "failure 3" || dead[0].Attempts != 3 || dead[0].FinishedAt.IsZero() {
		t.Errorf("dead job = %+v", dead[0])
	}
	d, _ := j.Depth(ctx)
	if d.Dead != 1 {
		t.Errorf("depth dead = %d", d.Dead)
	}

	// Requeue gives it a fresh set of attempts.
	if err := j.Requeue(ctx, dead[0].ID); err != nil {
		t.Fatalf("Requeue: %v", err)
	}
	if err := j.Requeue(ctx, dead[0].ID); !errors.Is(err, queue.ErrStale) {
		t.Errorf("second Requeue = %v, want ErrStale", err)
	}
	got, err := j.Claim(ctx, 1, queue.Filter{})
	if err != nil || len(got) != 1 || got[0].Attempts != 1 {
		t.Fatalf("claim after requeue = %+v, %v", got, err)
	}
}

func TestExpiredFinalAttemptIsDeadLettered(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{})
	ctx := context.Background()
	enqueue(t, j, queue.NewJob{Kind: "k", MaxAttempts: 1})
	got, err := j.Claim(ctx, 1, queue.Filter{})
	if err != nil || len(got) != 1 {
		t.Fatalf("claim: %v %v", got, err)
	}
	expireLease(t, db, j, got[0].ID)
	stuck, err := j.Stuck(ctx, 0, 10)
	if err != nil || len(stuck) != 1 || stuck[0].ID != got[0].ID {
		t.Fatalf("Stuck = %v, %v", stuck, err)
	}
	if again, err := j.Claim(ctx, 1, queue.Filter{}); err != nil || len(again) != 0 {
		t.Fatalf("claim after final attempt = %v, %v", again, err)
	}
	dead, err := j.DeadLetters(ctx, 10)
	if err != nil || len(dead) != 1 {
		t.Fatalf("DeadLetters = %v, %v", dead, err)
	}
	if err := j.Complete(ctx, got[0].ID, got[0].Epoch, nil); !errors.Is(err, queue.ErrStale) {
		t.Errorf("Complete after dead-lettering = %v, want ErrStale", err)
	}
}

func TestAdminDepthAndThroughput(t *testing.T) {
	db := testDB(t)
	j := newJobs(t, db, queue.JobsConfig{})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		enqueue(t, j, queue.NewJob{Kind: "k"})
	}
	enqueue(t, j, queue.NewJob{Kind: "k", RunAt: time.Now().Add(time.Hour)})
	got, err := j.Claim(ctx, 2, queue.Filter{})
	if err != nil || len(got) != 2 {
		t.Fatalf("claim: %v %v", got, err)
	}
	if err := j.Complete(ctx, got[0].ID, got[0].Epoch, json.RawMessage(`{"v":1}`)); err != nil {
		t.Fatal(err)
	}
	d, err := j.Depth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if want := (queue.Depth{Ready: 1, Delayed: 1, Running: 1, Done: 1}); d != want {
		t.Errorf("depth = %+v, want %+v", d, want)
	}
	tp, err := j.Throughput(ctx, time.Minute)
	if err != nil || tp.Done != 1 || tp.Dead != 0 || tp.PerSecond <= 0 {
		t.Errorf("throughput = %+v, %v", tp, err)
	}
	if _, err := j.Throughput(ctx, 0); err == nil {
		t.Error("zero window accepted")
	}
	done, _ := j.DeadLetters(ctx, 10)
	if len(done) != 0 {
		t.Errorf("dead letters = %v", done)
	}
}
