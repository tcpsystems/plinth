// Package cron runs scheduled jobs once per due slot across replicas of a
// service.
//
// Every run is one row in cron.schedule keyed on (schedule_id, next_due). A
// Scheduler that holds the leader lock (see package leader) surveys due rows
// and claims each with a compare-and-set that writes its owner and a lease.
// The claim keeps next_due, so the claimed row itself is the record that the
// slot ran: the same next_due is never scheduled twice while the row exists
// in a non-terminal state, and a successful recurring job schedules the next
// next_due only after this one succeeded.
//
// A run interrupted by leader loss (the process died, or lost the leader
// lock) keeps its row running with a lease nothing renews. After lease_until
// passes, the next leader reclaims the same row, including its next_due and
// attempt count, and runs it again. Every statement that finishes or renews
// a run matches on owner, so a leader that lost a run cannot overwrite the
// leader that reclaimed it, and a job whose lease lapses is cancelled.
//
// Owner identifies one Scheduler instance, not one service: it defaults to
// Name plus random per-process bytes, so two replicas that share a Name (and
// therefore the leader lock) still never match each other's rows. Set Owner
// to a stable per-replica identity when rows should name it recognisably.
//
// A failed run is retried by the same next_due after the job's backoff,
// up to MaxAttempts; then the row is marked dead.
package cron

import (
	"context"
	crand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/tcpsystems/plinth/keel/leader"
)

// Status is the state of a scheduled run.
type Status string

const (
	// StatusPending is a due row waiting to be claimed.
	StatusPending Status = "pending"
	// StatusRunning is a claimed row whose lease has not yet been renewed
	// to completion.
	StatusRunning Status = "running"
	// StatusSucceeded is a run that finished without error.
	StatusSucceeded Status = "succeeded"
	// StatusFailed is a run that failed and is retried after its backoff.
	StatusFailed Status = "failed"
	// StatusDead is a run that failed MaxAttempts times and is not retried.
	StatusDead Status = "dead"
)

// Defaults for a Job field left zero.
const (
	DefaultMaxAttempts  = 3
	DefaultBackoff      = time.Minute
	DefaultLease        = 2 * time.Minute
	defaultRenewDivisor = 4 // RenewInterval defaults to Lease/4
)

const (
	leaderLockPrefix = "cron:"
	pollInterval     = time.Second
)

// Job is one scheduled job.
type Job struct {
	// ID names the schedule; it is stable across deployments.
	ID string
	// Run performs the job. It must return promptly when ctx is cancelled:
	// the Scheduler cancels it when the lease or the leader lock is lost.
	Run func(ctx context.Context) error
	// Every is the schedule interval. A successful run schedules its next
	// next_due at the time it finished plus Every. Zero runs the rows a
	// schedule already has without creating new ones.
	Every time.Duration
	// MaxAttempts is the attempt count after which a failing run is marked
	// dead. Zero means DefaultMaxAttempts.
	MaxAttempts int
	// Backoff is the delay before a failed run is retried. Zero means
	// DefaultBackoff.
	Backoff time.Duration
	// Lease bounds how long a claimed run may go without a renewal before
	// another leader may reclaim it; a job renews it every RenewInterval.
	// Reclaiming a stalled but living run duplicates it, so jobs should be
	// idempotent. Zero means DefaultLease.
	Lease time.Duration
	// RenewInterval is how often a running job renews its lease and checks
	// the leader lock. Zero means Lease/4 and must be shorter than Lease.
	RenewInterval time.Duration
}

// Config configures a Scheduler.
type Config struct {
	// DB is the service's pool. Open it with leader.OpenSQL so the leader
	// lock can pin its connection.
	DB *sql.DB
	// Leaders elects the leader for the whole Scheduler: one lock,
	// leaderLockPrefix + Name, held while the Scheduler runs, so one
	// replica surveys all jobs at a time. Required.
	Leaders *leader.Manager
	// Name identifies this Scheduler among others sharing Leaders; it is
	// the advisory lock key. Required.
	Name string
	// Jobs are the scheduled jobs; IDs must be unique. A Scheduler only
	// surveys rows of its own jobs.
	Jobs []Job
	// Owner is recorded as the owner of claimed rows and fences renewals and
	// outcomes. It must be unique to this Scheduler instance; empty means
	// Name plus random bytes chosen by New, which is unique per process.
	Owner string
}

// JobError is reported to the error reporter when a run fails or is halted.
type JobError struct {
	ScheduleID string
	Due        time.Time
	Attempt    int
	Err        error
}

// Error formats the scheduled job failure with schedule ID, due time, and attempt.
func (e *JobError) Error() string {
	return fmt.Sprintf("cron: %s due %s attempt %d: %v", e.ScheduleID, e.Due.UTC().Format(time.RFC3339), e.Attempt, e.Err)
}

// Unwrap returns the job's error.
func (e *JobError) Unwrap() error { return e.Err }

// Scheduler claims and runs scheduled jobs on whichever replica holds the
// leader lock.
type Scheduler struct {
	db      querier
	leaders *leader.Manager
	config  Config
	report  func(error)
	now     func() time.Time
}

// newScheduler builds a Scheduler over a pooled database and leader locks.
func newScheduler(db querier, leaders *leader.Manager, config Config) *Scheduler {
	return &Scheduler{db: db, leaders: leaders, config: config, report: func(error) {}, now: time.Now}
}

// New validates config and returns a Scheduler. Nothing runs until Run is
// called.
func New(config Config) (*Scheduler, error) {
	if config.DB == nil {
		return nil, errors.New("cron: DB is required")
	}
	if config.Leaders == nil {
		return nil, errors.New("cron: Leaders is required")
	}
	if config.Name == "" {
		return nil, errors.New("cron: Name is required")
	}
	if config.Owner == "" {
		config.Owner = newOwner(config.Name)
	}
	seen := make(map[string]bool, len(config.Jobs))
	for i, j := range config.Jobs {
		if j.ID == "" {
			return nil, errors.New("cron: job ID is empty")
		}
		if j.Run == nil {
			return nil, fmt.Errorf("cron: job %q has no Run", j.ID)
		}
		if j.Every < 0 || j.Backoff < 0 || j.Lease < 0 || j.RenewInterval < 0 {
			return nil, fmt.Errorf("cron: job %q has a negative interval", j.ID)
		}
		if seen[j.ID] {
			return nil, fmt.Errorf("cron: duplicate job %q", j.ID)
		}
		seen[j.ID] = true
		if j.MaxAttempts == 0 {
			j.MaxAttempts = DefaultMaxAttempts
		}
		if j.Backoff == 0 {
			j.Backoff = DefaultBackoff
		}
		if j.Lease == 0 {
			j.Lease = DefaultLease
		}
		if j.RenewInterval == 0 {
			j.RenewInterval = j.Lease / defaultRenewDivisor
		}
		// Intervals are stored as whole milliseconds; anything shorter would
		// round to zero on the row.
		if j.Every%time.Millisecond != 0 || j.Backoff%time.Millisecond != 0 {
			return nil, fmt.Errorf("cron: job %q intervals must be whole milliseconds", j.ID)
		}
		if j.RenewInterval >= j.Lease {
			return nil, fmt.Errorf("cron: job %q renew interval %s must be shorter than lease %s", j.ID, j.RenewInterval, j.Lease)
		}
		config.Jobs[i] = j
	}
	return newScheduler(config.DB, config.Leaders, config), nil
}

// newOwner returns a Scheduler's default owner: name plus random bytes, unique
// to this process, so replicas sharing a Name never fence each other in.
func newOwner(name string) string {
	var b [8]byte
	_, _ = crand.Read(b[:]) // crypto/rand.Read never fails
	return name + "-" + hex.EncodeToString(b[:])
}

// SetErrorReporter sets where job failures and halted runs are reported;
// the default discards them. It never stops the Scheduler, which keeps
// running after a job fails. Call it before Run.
func (s *Scheduler) SetErrorReporter(report func(error)) {
	if report == nil {
		report = func(error) {}
	}
	s.report = report
}

// Run surveys due rows and runs jobs while this replica holds the leader
// lock, until ctx is cancelled. Losing the leader lock cancels any running
// job; the interrupted rows stay leased until they expire, then another
// leader reclaims them.
//
// Every replica calls Run, so those without the lock wait here and try
// again; Run ends only when ctx is cancelled or a lock call fails.
func (s *Scheduler) Run(ctx context.Context) error {
	for {
		if err := s.leadUntil(ctx); err != nil || ctx.Err() != nil {
			return err
		}
		if !s.wait(ctx) {
			return nil
		}
	}
}

// leadUntil acquires the leader lock and surveys until it is lost or ctx
// ends.
func (s *Scheduler) leadUntil(ctx context.Context) error {
	name := leaderLockPrefix + s.config.Name
	var ttl time.Duration
	for _, j := range s.config.Jobs {
		// The lock outlives the longest lease, so a renewal that stalls is
		// noticed by the lease running out, not only by losing the lock.
		ttl = max(ttl, 2*j.Lease)
	}
	if ttl == 0 {
		ttl = 2 * DefaultLease
	}
	for {
		ok, err := s.leaders.TryAcquire(ctx, name, ttl)
		if err != nil {
			return fmt.Errorf("cron: acquire leader lock: %w", err)
		}
		if ok {
			break
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(pollInterval):
		}
	}
	s.lead(ctx, name)
	// A round cut short by losing the lock is the successor's to survey.
	// The rows this Scheduler claimed stay leased whatever happens here:
	// next_due is unchanged, so a successor reclaims them after their lease
	// expires.
	release := context.WithoutCancel(ctx)
	if _, err := s.leaders.Release(release, name); err != nil {
		s.report(fmt.Errorf("cron: release leader lock: %w", err))
	}
	return nil
}

// lead surveys until ctx is cancelled or the leader lock is lost.
func (s *Scheduler) lead(ctx context.Context, name string) {
	for {
		held, err := s.leadHeld(ctx)
		if err == nil && held {
			err = s.survey(ctx)
		}
		// A survey ended by ctx is not a failure, whether by cancellation
		// or by the leader check that stops a round after leadership loss.
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, errLockLost) {
			s.report(fmt.Errorf("cron: survey: %w", err))
		} else if !held {
			// The lock moved to another replica; it owns every reclaim now.
			return
		}
		if !s.wait(ctx) {
			return
		}
	}
}

// wait pauses between survey rounds, returning false when ctx ends.
func (s *Scheduler) wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(pollInterval):
		return true
	}
}

// leadHeld reports whether this Scheduler still holds the leader lock.
func (s *Scheduler) leadHeld(ctx context.Context) (bool, error) {
	return s.leaders.Renew(ctx, leaderLockPrefix+s.config.Name)
}
