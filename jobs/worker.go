package jobs

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"
)

// Handler processes one claimed job. The context is the job's own context:
// it is cancelled when the worker's Run context is cancelled and at the
// claim's lease deadline (Job.LockedUntil), so a handler that outlives its
// lease is stopped before another claimer can take the job. Returning nil
// marks the job done; returning an error runs it through the store's retry
// backoff until its attempts are spent.
type Handler func(ctx context.Context, j Job) error

// Store is the job store a Worker drives: the three settle functions of
// this package bound to a database. It is an interface so the worker loop
// can be tested with a fake store, without a database.
//
// Claim must set Job.LockedUntil: the worker anchors each handler's deadline
// to it and refuses to start a job whose lease has already run out.
type Store interface {
	Claim(ctx context.Context, n int, lease time.Duration) ([]Job, error)
	Complete(ctx context.Context, id int64) error
	Fail(ctx context.Context, id int64, msg error, backoff func(attempt int) time.Duration) error
}

// SQLStore implements Store with the package-level Claim, Complete and Fail
// functions against a *sql.DB. This is the store a Worker uses in
// production.
type SQLStore struct {
	DB *sql.DB
}

// Claim claims up to n jobs with a lease on db.
func (s *SQLStore) Claim(ctx context.Context, n int, lease time.Duration) ([]Job, error) {
	return Claim(ctx, s.DB, n, lease)
}

// Complete marks the job done in db.
func (s *SQLStore) Complete(ctx context.Context, id int64) error {
	return Complete(ctx, s.DB, id)
}

// Fail records a failed attempt of the job in db.
func (s *SQLStore) Fail(ctx context.Context, id int64, msg error, backoff func(attempt int) time.Duration) error {
	return Fail(ctx, s.DB, id, msg, backoff)
}

// Default worker tuning. Every value can be overridden in Options.
const (
	// DefaultBatch is how many jobs one poll claims.
	DefaultBatch = 10
	// DefaultPollInterval is how long an idle worker waits before claiming
	// again.
	DefaultPollInterval = time.Second
	// DefaultLease is the lease given to each claimed job. Set it above the
	// longest legitimate handler; a job that outlives its lease can be
	// claimed again while the first handler is still running.
	DefaultLease = time.Minute
	// DefaultConcurrency is how many handlers one worker runs at once.
	DefaultConcurrency = 1
)

// Options configures a Worker. The zero value is valid: every field falls
// back to its Default constant.
type Options struct {
	// Batch is how many jobs one poll claims. Zero or less means
	// DefaultBatch.
	Batch int
	// PollInterval is how long an idle worker waits before the next claim.
	// Zero or less means DefaultPollInterval.
	PollInterval time.Duration
	// Lease is the lease requested for each claimed job. Each handler's
	// context is bounded by the claimed row's LockedUntil. Zero or less
	// means DefaultLease.
	Lease time.Duration
	// Backoff computes the retry delay after attempt n for a failed job,
	// passed unchanged to the store's Fail. Nil means DefaultBackoff.
	Backoff func(attempt int) time.Duration
	// Concurrency is how many handlers run at once. Zero or less means
	// DefaultConcurrency.
	Concurrency int
	// Logger receives the worker's events. Nil discards them.
	Logger *slog.Logger
}

// Worker claims jobs from a Store and runs registered handlers for them,
// completing or failing every job it claims. Create it with NewWorker and
// register handlers with Register before starting Run.
type Worker struct {
	store    Store
	batch    int
	poll     time.Duration
	lease    time.Duration
	backoff  func(attempt int) time.Duration
	logger   *slog.Logger
	sem      chan struct{} // one token per handler slot
	released chan struct{} // signals that a handler slot became free
	mu       sync.Mutex    // guards handlers, running
	handlers map[string]Handler
	running  bool
}

// NewWorker returns a Worker settling jobs through store. A nil store
// panics: the worker is useless without one. Options normalise to defaults
// as documented on each field.
func NewWorker(store Store, opts Options) *Worker {
	if store == nil {
		panic("jobs: NewWorker needs a store")
	}
	w := &Worker{
		store:    store,
		batch:    opts.Batch,
		poll:     opts.PollInterval,
		lease:    opts.Lease,
		backoff:  opts.Backoff,
		handlers: make(map[string]Handler),
		logger:   opts.Logger,
	}
	if w.batch <= 0 {
		w.batch = DefaultBatch
	}
	if w.poll <= 0 {
		w.poll = DefaultPollInterval
	}
	if w.lease <= 0 {
		w.lease = DefaultLease
	}
	if w.backoff == nil {
		w.backoff = DefaultBackoff
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = DefaultConcurrency
	}
	w.sem = make(chan struct{}, opts.Concurrency)
	w.released = make(chan struct{}, 1)
	if w.logger == nil {
		w.logger = slog.New(slog.DiscardHandler)
	}
	return w
}

// Register binds kind to h. Handlers are looked up per claimed job, so a
// kind with no handler stays in the store and fails with a clear error
// until a handler is registered for it. Register replaces an earlier
// handler for the same kind, and it may run concurrently with Run. A nil
// handler unregisters the kind; an empty kind is ignored with a warning.
func (w *Worker) Register(kind string, h Handler) {
	if kind == "" {
		w.logger.Warn("jobs: worker ignoring a registration with an empty kind")
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if h == nil {
		delete(w.handlers, kind)
		return
	}
	w.handlers[kind] = h
}

// Run polls the store, dispatches each claimed job to its handler and
// settles the job with Complete or Fail until the context is cancelled. Each
// poll claims at most the free handler slots, so no claimed job waits behind
// the semaphore while its lease ages. On cancellation it stops claiming,
// waits for the running handlers and returns nil; their jobs are settled
// with a detached context, so a graceful shutdown never abandons a claim
// mid-flight. A job claimed in the same poll as the cancellation is
// abandoned, not started.
//
// A second Run on the same Worker returns an error. Store errors are
// logged, not returned: an unreachable store is a pause, and the loop
// retries after the next poll. A handler panic is recovered and settled
// with Fail like any other handler error.
func (w *Worker) Run(ctx context.Context) error {
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("jobs: worker is already running")
	}
	w.running = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
	}()

	var wg sync.WaitGroup
	for ctx.Err() == nil {
		free := cap(w.sem) - len(w.sem)
		if free == 0 {
			// Every handler slot is busy. A further claim would sit behind
			// the semaphore while its lease aged, so wait for a slot release
			// or the next poll instead.
			w.waitForSlot(ctx)
			continue
		}
		n := w.batch
		if free < n {
			n = free
		}
		// Reserve the slots before claiming. Slots are only acquired here,
		// so len(sem) cannot grow underneath the claim and the number of
		// claimed jobs matches the number of handlers that can start.
		for i := 0; i < n; i++ {
			w.sem <- struct{}{}
		}
		claimed, err := w.store.Claim(ctx, n, w.lease)
		if err != nil {
			for i := 0; i < n; i++ {
				<-w.sem
			}
			if ctx.Err() != nil {
				break
			}
			w.logger.Error("jobs: worker claim failed", "error", err)
			claimed = nil
		} else if len(claimed) < n {
			// Fewer jobs were due than the slots reserved for them.
			for i := len(claimed); i < n; i++ {
				<-w.sem
			}
		}
		for _, j := range claimed {
			wg.Add(1)
			go w.start(ctx, &wg, j)
		}
		if len(claimed) == 0 {
			timer := time.NewTimer(w.poll)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
	}
	// Drain: every handler finishes and settles its job. Their per-job
	// contexts are cancelled, so settle calls use a detached context.
	wg.Wait()
	return nil
}

// start runs one claimed job in a handler slot that Run already reserved.
// A free slot does not outrank cancellation: when the worker's context is
// already cancelled the job is abandoned instead of started. It is also not
// started when its lease ran out between the claim and this goroutine, since
// another claimer may own it by now.
func (w *Worker) start(ctx context.Context, wg *sync.WaitGroup, j Job) {
	defer wg.Done()
	defer func() {
		<-w.sem
		select {
		case w.released <- struct{}{}:
		default:
		}
	}()
	if ctx.Err() != nil {
		w.abandon(ctx, j)
		return
	}
	if !j.LockedUntil.After(time.Now()) {
		w.expire(ctx, j)
		return
	}
	w.settle(ctx, j)
}

// waitForSlot blocks until a handler releases its slot, the poll interval
// passes or ctx is cancelled. The release wakes the loop immediately, so a
// freed slot is used without waiting for the next poll.
func (w *Worker) waitForSlot(ctx context.Context) {
	timer := time.NewTimer(w.poll)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-w.released:
	case <-timer.C:
	}
}

// handle looks up the handler for j and calls it with a context bounded by
// the worker's Run context and the claim's lease deadline. The deadline is
// Job.LockedUntil, not now plus the lease, so time spent queued before the
// handler started is not granted again. The named return carries a
// recovered panic as an error.
func (w *Worker) handle(parent context.Context, j Job) (err error) {
	jctx, cancel := context.WithDeadline(parent, j.LockedUntil)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("jobs: handler panicked",
				"kind", j.Kind, "job", j.ID, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("jobs: handler panic for kind %q on job %d: %v", j.Kind, j.ID, r)
		}
	}()

	w.mu.Lock()
	h := w.handlers[j.Kind]
	w.mu.Unlock()
	if h == nil {
		return fmt.Errorf("jobs: no handler registered for kind %q (job %d)", j.Kind, j.ID)
	}
	return h(jctx, j)
}

// settle runs one claimed job and settles it with the store. The settle
// context is detached from parent so a shutdown cancellation still records
// the outcome; if even that write fails the lease expires and the job
// becomes claimable again.
func (w *Worker) settle(parent context.Context, j Job) {
	err := w.handle(parent, j)
	ctx := context.WithoutCancel(parent)
	if err == nil {
		if cerr := w.store.Complete(ctx, j.ID); cerr != nil {
			w.logger.Error("jobs: worker complete failed", "job", j.ID, "error", cerr)
			return
		}
		w.logger.Info("jobs: job done", "kind", j.Kind, "job", j.ID, "attempts", j.Attempts)
		return
	}
	if ferr := w.store.Fail(ctx, j.ID, err, w.backoff); ferr != nil {
		w.logger.Error("jobs: worker fail record failed", "job", j.ID, "error", ferr)
		return
	}
	w.logger.Warn("jobs: job attempt failed", "kind", j.Kind, "job", j.ID, "attempts", j.Attempts, "error", err)
}

// abandon settles a job that was claimed but never started because the
// worker is shutting down. The attempt the claim consumed is recorded with
// a clear error so the job is visible and retries after backoff.
func (w *Worker) abandon(parent context.Context, j Job) {
	ctx := context.WithoutCancel(parent)
	err := fmt.Errorf("jobs: worker stopped before kind %q job %d started", j.Kind, j.ID)
	if ferr := w.store.Fail(ctx, j.ID, err, w.backoff); ferr != nil {
		w.logger.Error("jobs: worker fail record failed", "job", j.ID, "error", ferr)
		return
	}
	w.logger.Warn("jobs: job abandoned on shutdown", "kind", j.Kind, "job", j.ID)
}

// expire settles a job whose lease ran out before its handler could start.
// Starting it would run alongside a second claimer, so the attempt is
// recorded as failed and the job retries after backoff.
func (w *Worker) expire(parent context.Context, j Job) {
	ctx := context.WithoutCancel(parent)
	err := fmt.Errorf("jobs: lease for kind %q job %d expired before the handler started", j.Kind, j.ID)
	if ferr := w.store.Fail(ctx, j.ID, err, w.backoff); ferr != nil {
		w.logger.Error("jobs: worker fail record failed", "job", j.ID, "error", ferr)
		return
	}
	w.logger.Warn("jobs: job lease expired before start", "kind", j.Kind, "job", j.ID)
}
