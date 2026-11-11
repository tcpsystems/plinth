package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/tcpsystems/plinth/jobs"
)

// fakeStore is an in-memory stand-in for jobs.Store that models the
// behaviour the Worker depends on: Claim hands out due queued jobs and
// running jobs whose lease has expired, sorted by (RunAt, ID) like the SQL
// does; attempts increase per claim; Fail reschedules with the caller's
// backoff until attempts run out, then parks the job as failed; Complete
// marks it done. Every call is recorded with the current (fake) clock so
// tests can watch retry delays and scheduling without ever sleeping for
// real.
type fakeStore struct {
	mu      sync.Mutex
	nextID  int64
	rows    []*jobs.Job
	claims  []claimEvent
	dones   []settleEvent
	fails   []failEvent
	doneSig chan int64
}

type claimEvent struct {
	at  time.Time
	ids []int64
}

type settleEvent struct {
	at time.Time
	id int64
}

type failEvent struct {
	at       time.Time
	id       int64
	err      error
	delay    time.Duration
	terminal bool
}

type fakeSnapshot struct {
	claims []claimEvent
	dones  []settleEvent
	fails  []failEvent
	rows   map[int64]jobs.Job
}

func newFakeStore() *fakeStore {
	return &fakeStore{doneSig: make(chan int64, 16)}
}

// addJob enqueues a job due now and returns its id. maxAttempts of zero
// means the store default.
func (s *fakeStore) addJob(kind string, maxAttempts int, payload json.RawMessage) int64 {
	return s.addDueAt(kind, maxAttempts, time.Now(), payload)
}

// addDueAt enqueues a job first claimable at runAt.
func (s *fakeStore) addDueAt(kind string, maxAttempts int, runAt time.Time, payload json.RawMessage) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	if maxAttempts == 0 {
		maxAttempts = jobs.DefaultMaxAttempts
	}
	now := time.Now()
	s.rows = append(s.rows, &jobs.Job{
		ID:          s.nextID,
		Kind:        kind,
		Payload:     payload,
		State:       jobs.Queued,
		MaxAttempts: maxAttempts,
		RunAt:       runAt,
		CreatedAt:   now,
		UpdatedAt:   now,
	})
	return s.nextID
}

// add marshals payload and enqueues it with the default attempts.
func (s *fakeStore) add(t *testing.T, kind string, payload any) int64 {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload for %s: %v", kind, err)
	}
	return s.addJob(kind, 0, body)
}

func (s *fakeStore) Claim(_ context.Context, n int, lease time.Duration) ([]jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var due []*jobs.Job
	for _, r := range s.rows {
		if r.State == jobs.Queued && !r.RunAt.After(now) ||
			r.State == jobs.Running && !r.LockedUntil.After(now) {
			due = append(due, r)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if !due[i].RunAt.Equal(due[j].RunAt) {
			return due[i].RunAt.Before(due[j].RunAt)
		}
		return due[i].ID < due[j].ID
	})
	if len(due) > n {
		due = due[:n]
	}
	out := make([]jobs.Job, 0, len(due))
	ids := make([]int64, 0, len(due))
	for _, r := range due {
		r.State = jobs.Running
		r.Attempts++
		r.LockedUntil = now.Add(lease)
		r.UpdatedAt = now
		out = append(out, *r)
		ids = append(ids, r.ID)
	}
	s.claims = append(s.claims, claimEvent{at: now, ids: ids})
	return out, nil
}

func (s *fakeStore) Complete(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rowLocked(id)
	if r == nil {
		return fmt.Errorf("fake store: no job %d", id)
	}
	now := time.Now()
	r.State = jobs.Done
	r.LockedUntil = time.Time{}
	r.UpdatedAt = now
	s.dones = append(s.dones, settleEvent{at: now, id: id})
	select {
	case s.doneSig <- id:
	default:
	}
	return nil
}

func (s *fakeStore) Fail(_ context.Context, id int64, msg error, backoff func(attempt int) time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := s.rowLocked(id)
	if r == nil {
		return fmt.Errorf("fake store: no job %d", id)
	}
	if msg == nil {
		return errors.New("fake store: fail needs the attempt error")
	}
	if backoff == nil {
		backoff = jobs.DefaultBackoff
	}
	now := time.Now()
	ev := failEvent{at: now, id: id, err: msg}
	if r.Attempts >= r.MaxAttempts {
		r.State = jobs.Failed
		ev.terminal = true
	} else {
		delay := backoff(r.Attempts)
		if delay < 0 {
			delay = 0
		}
		ev.delay = delay
		r.State = jobs.Queued
		r.RunAt = now.Add(delay)
	}
	r.LastError = msg.Error()
	r.LockedUntil = time.Time{}
	r.UpdatedAt = now
	s.fails = append(s.fails, ev)
	return nil
}

func (s *fakeStore) rowLocked(id int64) *jobs.Job {
	for _, r := range s.rows {
		if r.ID == id {
			return r
		}
	}
	return nil
}

func (s *fakeStore) snapshot() fakeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := fakeSnapshot{
		claims: append([]claimEvent{}, s.claims...),
		dones:  append([]settleEvent{}, s.dones...),
		fails:  append([]failEvent{}, s.fails...),
		rows:   make(map[int64]jobs.Job, len(s.rows)),
	}
	for _, r := range s.rows {
		snap.rows[r.ID] = *r
	}
	return snap
}

// claimedIDs returns every job id handed out by any Claim call.
func (snap fakeSnapshot) claimedIDs() []int64 {
	var ids []int64
	for _, c := range snap.claims {
		ids = append(ids, c.ids...)
	}
	return ids
}

func (snap fakeSnapshot) doneIDs() []int64 {
	var ids []int64
	for _, d := range snap.dones {
		ids = append(ids, d.id)
	}
	return ids
}

func (snap fakeSnapshot) failIDs() []int64 {
	var ids []int64
	for _, f := range snap.fails {
		ids = append(ids, f.id)
	}
	return ids
}

// runWorker starts w.Run in a goroutine and returns the channel that
// receives its error. The caller must cancel ctx and drain errCh before
// the synctest bubble closes.
func runWorker(ctx context.Context, w *jobs.Worker) chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx) }()
	return errCh
}

func stopWorker(t *testing.T, cancel context.CancelFunc, errCh chan error) {
	t.Helper()
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
}

func TestWorkerCompletesJobWithinLeaseDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		store := newFakeStore()
		id := store.add(t, "greet", map[string]string{"name": "world"})
		type obs struct {
			at        time.Time
			remaining time.Duration
			deadline  bool
		}
		obsCh := make(chan obs, 1)
		w := jobs.NewWorker(store, jobs.Options{
			PollInterval: 50 * time.Millisecond,
			Lease:        10 * time.Second,
		})
		w.Register("greet", func(ctx context.Context, j jobs.Job) error {
			deadline, ok := ctx.Deadline()
			obsCh <- obs{at: time.Now(), remaining: time.Until(deadline), deadline: ok}
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		o := <-obsCh
		synctest.Wait()
		if !o.deadline {
			t.Fatal("handler context has no deadline, want lease-bounded")
		}
		if o.remaining != 10*time.Second {
			t.Errorf("handler deadline in %v, want the full lease 10s", o.remaining)
		}
		snap := store.snapshot()
		if got := snap.doneIDs(); len(got) != 1 || got[0] != id {
			t.Errorf("done %v, want exactly [%d]", got, id)
		}
		if len(snap.dones) == 1 && !snap.dones[0].at.Equal(start) {
			t.Errorf("complete at %v, want bubble start %v", snap.dones[0].at, start)
		}
		stopWorker(t, cancel, errCh)
	})
}

func TestWorkerRetriesWithBackoffWithoutSleeping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		store := newFakeStore()
		var mu sync.Mutex
		var starts []time.Time
		attempt := 0
		id := store.addJob("flaky", 3, nil)
		w := jobs.NewWorker(store, jobs.Options{
			PollInterval: 50 * time.Millisecond,
			Lease:        10 * time.Second,
		})
		w.Register("flaky", func(_ context.Context, _ jobs.Job) error {
			mu.Lock()
			attempt++
			starts = append(starts, time.Now())
			mu.Unlock()
			if attempt < 3 {
				return errors.New("boom")
			}
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		// Fake clock: the whole 1s + 2s backoff schedule runs in no real time.
		time.Sleep(3500 * time.Millisecond)
		synctest.Wait()

		mu.Lock()
		got := append([]time.Time{}, starts...)
		mu.Unlock()
		want := []time.Time{start, start.Add(time.Second), start.Add(3 * time.Second)}
		if len(got) != len(want) {
			t.Fatalf("attempt starts %v, want %v", got, want)
		}
		for i := range want {
			if !got[i].Equal(want[i]) {
				t.Errorf("attempt %d started at %v, want %v", i+1, got[i].Sub(start), want[i].Sub(start))
			}
		}
		snap := store.snapshot()
		if len(snap.fails) != 2 {
			t.Fatalf("fail events %d, want 2", len(snap.fails))
		}
		// DefaultBackoff: attempt 1 waits 1s, attempt 2 waits 2s.
		if snap.fails[0].delay != time.Second || snap.fails[1].delay != 2*time.Second {
			t.Errorf("retry delays %v, %v, want 1s, 2s", snap.fails[0].delay, snap.fails[1].delay)
		}
		if snap.fails[0].terminal || snap.fails[1].terminal {
			t.Error("retries marked terminal before attempts were spent")
		}
		if ids := snap.doneIDs(); len(ids) != 1 || ids[0] != id {
			t.Errorf("done %v, want exactly [%d]", ids, id)
		}
		if got := snap.claimedIDs(); len(got) != 3 {
			t.Errorf("job claimed %d times, want 3", len(got))
		}
		stopWorker(t, cancel, errCh)
	})
}

func TestWorkerRecoversHandlerPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		store := newFakeStore()
		var mu sync.Mutex
		attempt := 0
		id := store.addJob("panicky", 3, nil)
		w := jobs.NewWorker(store, jobs.Options{
			PollInterval: 50 * time.Millisecond,
			Lease:        10 * time.Second,
		})
		w.Register("panicky", func(_ context.Context, _ jobs.Job) error {
			mu.Lock()
			attempt++
			n := attempt
			mu.Unlock()
			if n == 1 {
				panic("kaboom")
			}
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		time.Sleep(1200 * time.Millisecond)
		synctest.Wait()

		snap := store.snapshot()
		if len(snap.fails) != 1 {
			t.Fatalf("fail events %d, want 1", len(snap.fails))
		}
		f := snap.fails[0]
		if !strings.Contains(f.err.Error(), "handler panic") || !strings.Contains(f.err.Error(), "kaboom") {
			t.Errorf("fail error %q, want it to name the panic and its value", f.err)
		}
		if f.terminal || f.delay != time.Second {
			t.Errorf("fail terminal=%v delay=%v, want a retry in 1s", f.terminal, f.delay)
		}
		// The loop survived the panic: the retry ran and succeeded.
		if ids := snap.doneIDs(); len(ids) != 1 || ids[0] != id {
			t.Errorf("done %v, want exactly [%d] after the panic", ids, id)
		}
		if got := snap.dones[0].at; !got.Equal(start.Add(time.Second)) {
			t.Errorf("retry completed at %v, want +1s", got.Sub(start))
		}
		stopWorker(t, cancel, errCh)
	})
}

func TestWorkerFailsUnknownKind(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore()
		id := store.addJob("mystery", 2, nil)
		w := jobs.NewWorker(store, jobs.Options{
			PollInterval: 50 * time.Millisecond,
			Lease:        10 * time.Second,
		})
		// No handler is registered for "mystery".
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		time.Sleep(1200 * time.Millisecond)
		synctest.Wait()

		snap := store.snapshot()
		if len(snap.fails) != 2 {
			t.Fatalf("fail events %d, want 2 (retry then terminal)", len(snap.fails))
		}
		for _, f := range snap.fails {
			if !strings.Contains(f.err.Error(), `no handler registered for kind "mystery"`) {
				t.Errorf("fail error %q, want a clear unknown-kind message", f.err)
			}
		}
		if !snap.fails[1].terminal {
			t.Error("second failure not terminal although MaxAttempts was 2")
		}
		if snap.rows[id].State != jobs.Failed {
			t.Errorf("job state %q, want failed", snap.rows[id].State)
		}
		if !strings.Contains(snap.rows[id].LastError, "no handler") {
			t.Errorf("last_error %q, want the unknown-kind reason kept", snap.rows[id].LastError)
		}
		if ids := snap.doneIDs(); len(ids) != 0 {
			t.Errorf("done %v, want none", ids)
		}
		stopWorker(t, cancel, errCh)
	})
}

func TestWorkerDrainsGracefully(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore()
		id := store.add(t, "slow", nil)
		started := make(chan int64, 1)
		ctxErrSeen := make(chan error, 1)
		w := jobs.NewWorker(store, jobs.Options{
			PollInterval: 50 * time.Millisecond,
			Lease:        10 * time.Second,
		})
		w.Register("slow", func(ctx context.Context, j jobs.Job) error {
			started <- j.ID
			time.Sleep(300 * time.Millisecond)
			ctxErrSeen <- ctx.Err()
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		<-started
		cancel()
		// Run must not return until the handler finished its 300ms of work.
		if err := <-errCh; err != nil {
			t.Fatalf("Run returned %v, want nil", err)
		}
		if err := <-ctxErrSeen; !errors.Is(err, context.Canceled) {
			t.Errorf("handler saw ctx.Err()=%v while draining, want cancellation", err)
		}
		snap := store.snapshot()
		if ids := snap.doneIDs(); len(ids) != 1 || ids[0] != id {
			t.Errorf("done %v, want exactly [%d] settled on shutdown", ids, id)
		}
		if ids := snap.failIDs(); len(ids) != 0 {
			t.Errorf("fails %v, want none: a drained handler completes", ids)
		}
		if got := snap.claimedIDs(); len(got) != 1 {
			t.Errorf("claimed %v, want the job claimed once", got)
		}
	})
}

func TestWorkerClaimsNoMoreThanFreeSlots(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore()
		ids := []int64{
			store.addJob("hold", 3, nil),
			store.addJob("hold", 3, nil),
			store.addJob("hold", 3, nil),
		}
		started := make(chan int64, len(ids))
		release := make(chan struct{})
		// Batch is three but there is one slot: only one job may be claimed
		// at a time, so the other two stay claimable instead of ageing on a
		// lease behind the semaphore.
		w := jobs.NewWorker(store, jobs.Options{
			Batch:        3,
			Concurrency:  1,
			PollInterval: 50 * time.Millisecond,
			Lease:        10 * time.Second,
		})
		w.Register("hold", func(_ context.Context, j jobs.Job) error {
			started <- j.ID
			<-release
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		first := <-started
		synctest.Wait()

		snap := store.snapshot()
		if got := snap.claimedIDs(); len(got) != 1 || got[0] != first {
			t.Fatalf("claims %v, want only the running job [%d]", got, first)
		}
		for _, id := range ids {
			if id == first {
				continue
			}
			if snap.rows[id].State != jobs.Queued || snap.rows[id].Attempts != 0 {
				t.Errorf("job %d state=%q attempts=%d, want queued and unclaimed",
					id, snap.rows[id].State, snap.rows[id].Attempts)
			}
		}
		if len(snap.fails) != 0 {
			t.Fatalf("fails %v, want none while the first job runs", snap.failIDs())
		}

		close(release)
		for range ids {
			<-store.doneSig
		}
		synctest.Wait()

		snap = store.snapshot()
		if got := snap.doneIDs(); len(got) != len(ids) {
			t.Errorf("done %v, want all %d jobs", got, ids)
		}
		for _, id := range ids {
			if snap.rows[id].Attempts != 1 {
				t.Errorf("job %d attempts %d, want 1", id, snap.rows[id].Attempts)
			}
		}
		stopWorker(t, cancel, errCh)
	})
}

// TestWorkerSlowBatchRunsEachJobOnce is the regression test for a poll
// claiming more jobs than there are handler slots. The batch needs more
// total handler time than the lease, while every single handler fits inside
// it: with the batch queued behind the semaphore the later jobs' leases
// would expire before their handlers started, letting a second claim run
// them again. Claiming only the free slots prevents that.
func TestWorkerSlowBatchRunsEachJobOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store := newFakeStore()
		const jobCount = 6
		ids := make([]int64, jobCount)
		for i := range ids {
			ids[i] = store.addJob("slow", 3, nil)
		}
		var mu sync.Mutex
		runs := make(map[int64]int, jobCount)
		w := jobs.NewWorker(store, jobs.Options{
			Batch:        jobCount,
			Concurrency:  2,
			PollInterval: 10 * time.Millisecond,
			// Three waves of 40ms handlers take 120ms in total, longer than
			// this lease, yet each handler is comfortably inside it.
			Lease: 100 * time.Millisecond,
		})
		w.Register("slow", func(_ context.Context, j jobs.Job) error {
			time.Sleep(40 * time.Millisecond)
			mu.Lock()
			runs[j.ID]++
			mu.Unlock()
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		time.Sleep(time.Second)
		synctest.Wait()

		mu.Lock()
		gotRuns := make(map[int64]int, len(runs))
		for id, n := range runs {
			gotRuns[id] = n
		}
		mu.Unlock()
		snap := store.snapshot()
		for _, id := range ids {
			if gotRuns[id] != 1 {
				t.Errorf("job %d ran %d times, want exactly once", id, gotRuns[id])
			}
			if snap.rows[id].Attempts != 1 {
				t.Errorf("job %d attempts %d, want 1 (no duplicate claim)", id, snap.rows[id].Attempts)
			}
			if snap.rows[id].State != jobs.Done {
				t.Errorf("job %d state %q, want done", id, snap.rows[id].State)
			}
		}
		if got := snap.claimedIDs(); len(got) != jobCount {
			t.Errorf("claims %v, want one per job (%d)", got, jobCount)
		}
		if len(snap.fails) != 0 {
			t.Errorf("fails %v, want none", snap.failIDs())
		}
		stopWorker(t, cancel, errCh)
	})
}

// cancelOnClaimStore cancels the worker's context inside Claim and then
// returns one job, so the claim is in flight exactly when cancellation is
// already observable and a handler slot is free.
type cancelOnClaimStore struct {
	mu     sync.Mutex
	cancel context.CancelFunc
	job    jobs.Job
	once   bool
	fails  []failEvent
}

func (s *cancelOnClaimStore) Claim(_ context.Context, _ int, lease time.Duration) ([]jobs.Job, error) {
	s.mu.Lock()
	if s.once {
		s.mu.Unlock()
		return nil, nil
	}
	s.once = true
	s.mu.Unlock()
	s.cancel()
	j := s.job
	j.LockedUntil = time.Now().Add(lease)
	return []jobs.Job{j}, nil
}

func (s *cancelOnClaimStore) Complete(context.Context, int64) error { return nil }

func (s *cancelOnClaimStore) Fail(_ context.Context, id int64, err error, _ func(int) time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails = append(s.fails, failEvent{at: time.Now(), id: id, err: err})
	return nil
}

func (s *cancelOnClaimStore) failCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.fails)
}

// TestWorkerCancellationBeatsAFreeSlot checks the race between a free
// semaphore slot and a cancelled context: a handler slot being available
// must not start a job claimed at shutdown. The loop is repeated because the
// defect was a select race that only some interleavings hit.
func TestWorkerCancellationBeatsAFreeSlot(t *testing.T) {
	const rounds = 64
	for round := range rounds {
		ctx, cancel := context.WithCancel(context.Background())
		store := &cancelOnClaimStore{
			cancel: cancel,
			job:    jobs.Job{ID: int64(round + 1), Kind: "late", MaxAttempts: 3},
		}
		started := make(chan struct{}, 1)
		w := jobs.NewWorker(store, jobs.Options{
			Concurrency:  1,
			PollInterval: time.Millisecond,
			Lease:        time.Second,
		})
		w.Register("late", func(context.Context, jobs.Job) error {
			started <- struct{}{}
			return nil
		})
		if err := w.Run(ctx); err != nil {
			t.Fatalf("round %d: Run returned %v, want nil", round, err)
		}
		select {
		case <-started:
			t.Fatalf("round %d: handler started after cancellation with a free slot", round)
		default:
		}
		if got := store.failCount(); got != 1 {
			t.Fatalf("round %d: failed settlements %d, want the abandoned claim recorded", round, got)
		}
	}
}

// expiredLeaseStore returns one job whose lease already ran out.
type expiredLeaseStore struct {
	mu       sync.Mutex
	job      jobs.Job
	once     bool
	fails    []failEvent
	failedCh chan struct{}
}

func newExpiredLeaseStore(j jobs.Job) *expiredLeaseStore {
	return &expiredLeaseStore{job: j, failedCh: make(chan struct{}, 1)}
}

func (s *expiredLeaseStore) Claim(context.Context, int, time.Duration) ([]jobs.Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.once {
		return nil, nil
	}
	s.once = true
	j := s.job
	j.LockedUntil = time.Now().Add(-time.Second)
	return []jobs.Job{j}, nil
}

func (s *expiredLeaseStore) Complete(context.Context, int64) error { return nil }

func (s *expiredLeaseStore) Fail(_ context.Context, id int64, err error, _ func(int) time.Duration) error {
	s.mu.Lock()
	s.fails = append(s.fails, failEvent{at: time.Now(), id: id, err: err})
	s.mu.Unlock()
	select {
	case s.failedCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *expiredLeaseStore) firstFail() failEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fails[0]
}

func TestWorkerRefusesExpiredLease(t *testing.T) {
	store := newExpiredLeaseStore(jobs.Job{ID: 7, Kind: "late", MaxAttempts: 3})
	started := make(chan struct{}, 1)
	w := jobs.NewWorker(store, jobs.Options{
		Concurrency:  1,
		PollInterval: time.Millisecond,
		Lease:        time.Second,
	})
	w.Register("late", func(context.Context, jobs.Job) error {
		started <- struct{}{}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := runWorker(ctx, w)

	select {
	case <-store.failedCh:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("worker never settled the expired claim")
	}
	cancel()
	if err := <-errCh; err != nil {
		t.Fatalf("Run returned %v, want nil", err)
	}
	select {
	case <-started:
		t.Fatal("handler started for a job whose lease had expired")
	default:
	}
	f := store.firstFail()
	if !strings.Contains(f.err.Error(), "lease") || !strings.Contains(f.err.Error(), "expired") {
		t.Errorf("expired job failed with %q, want a lease-expired error", f.err)
	}
}

func TestWorkerRespectsConcurrency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		store := newFakeStore()
		var ids []int64
		for range 5 {
			ids = append(ids, store.addJob("tick", 3, nil))
		}
		var mu sync.Mutex
		inflight, peak := 0, 0
		w := jobs.NewWorker(store, jobs.Options{
			Batch:        10,
			Concurrency:  2,
			PollInterval: 50 * time.Millisecond,
			Lease:        10 * time.Second,
		})
		w.Register("tick", func(_ context.Context, _ jobs.Job) error {
			mu.Lock()
			inflight++
			if inflight > peak {
				peak = inflight
			}
			mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			mu.Lock()
			inflight--
			mu.Unlock()
			return nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		time.Sleep(350 * time.Millisecond)
		synctest.Wait()

		snap := store.snapshot()
		mu.Lock()
		gotPeak := peak
		mu.Unlock()
		if gotPeak != 2 {
			t.Errorf("peak concurrent handlers %d, want 2", gotPeak)
		}
		if got := snap.doneIDs(); len(got) != len(ids) {
			t.Fatalf("done %v, want all 5 jobs", got)
		}
		// Waves of two: completions land at +100ms, +200ms and +300ms.
		var offsets []time.Duration
		for _, d := range snap.dones {
			offsets = append(offsets, d.at.Sub(start))
		}
		sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
		want := []time.Duration{100 * time.Millisecond, 100 * time.Millisecond,
			200 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond}
		for i := range want {
			if offsets[i] != want[i] {
				t.Fatalf("completion times %v, want %v", offsets, want)
			}
		}
		stopWorker(t, cancel, errCh)
	})
}

func TestWorkerLeaseBoundsHandlerContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		store := newFakeStore()
		store.addJob("longrun", 1, nil)
		ends := make(chan time.Time, 4)
		w := jobs.NewWorker(store, jobs.Options{
			PollInterval: 60 * time.Millisecond,
			Lease:        100 * time.Millisecond,
		})
		w.Register("longrun", func(ctx context.Context, _ jobs.Job) error {
			select {
			case <-ctx.Done():
			case <-time.After(500 * time.Millisecond):
			}
			ends <- time.Now()
			return ctx.Err()
		})
		ctx, cancel := context.WithCancel(context.Background())
		errCh := runWorker(ctx, w)

		time.Sleep(200 * time.Millisecond)
		synctest.Wait()

		// The handler gave up at the lease deadline, not after its 500ms:
		// the per-job context is bounded so a stalled handler stops before
		// its lease can be taken by another claimer.
		select {
		case at := <-ends:
			if !at.Equal(start.Add(100 * time.Millisecond)) {
				t.Errorf("handler returned at %v, want the lease deadline +100ms", at.Sub(start))
			}
		default:
			t.Fatal("handler never returned at the lease deadline")
		}
		snap := store.snapshot()
		if len(snap.fails) != 1 || !snap.fails[0].terminal {
			t.Fatalf("fails %+v, want one terminal failure", snap.fails)
		}
		if !errors.Is(snap.fails[0].err, context.DeadlineExceeded) {
			t.Errorf("fail error %q, want the lease deadline error", snap.fails[0].err)
		}
		if got := snap.claimedIDs(); len(got) != 1 {
			t.Errorf("claimed %v, want a single claim (MaxAttempts 1)", got)
		}
		stopWorker(t, cancel, errCh)
	})
}

// ExampleWorker runs a Worker against the in-memory store used by these
// tests: register handlers, start Run in a goroutine, and stop it by
// cancelling the context once the work is done.
func ExampleWorker() {
	store := newFakeStore()
	// The second job is due after the first so it is claimed on a later
	// poll: with one handler slot the printed order is then fixed.
	store.addJob("greet", 3, json.RawMessage(`{"name":"world"}`))
	store.addDueAt("count", 3, time.Now().Add(50*time.Millisecond), json.RawMessage(`{"n":3}`))

	w := jobs.NewWorker(store, jobs.Options{
		PollInterval: 5 * time.Millisecond,
		Lease:        5 * time.Second,
		Concurrency:  1,
	})
	w.Register("greet", func(_ context.Context, j jobs.Job) error {
		var p struct{ Name string }
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return err
		}
		fmt.Printf("greeted %s\n", p.Name)
		return nil
	})
	w.Register("count", func(_ context.Context, j jobs.Job) error {
		var p struct{ N int }
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return err
		}
		fmt.Printf("counted %d\n", p.N)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- w.Run(ctx) }()

	// Both jobs settle, then shut the loop down gracefully.
	<-store.doneSig
	<-store.doneSig
	cancel()
	if err := <-errCh; err != nil {
		fmt.Println("worker:", err)
	}

	// Output:
	// greeted world
	// counted 3
}
