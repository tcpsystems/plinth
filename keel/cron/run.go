package cron

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// errNotDue means a due row was claimed or finished by another scheduler
// between the survey and the claim.
var errNotDue = errors.New("cron: row is not due")

// errHalted ends a run once it no longer owns its lease.
var errHalted = errors.New("cron: lease or leader lock lost")

// errLockLost ends a survey once this Scheduler no longer leads, so no run
// starts under a leader that has moved on.
var errLockLost = errors.New("cron: leader lock lost")

// surveyLimit bounds the rows one survey reads.
const surveyLimit = 256

// maxAttempts is the job's MaxAttempts, or DefaultMaxAttempts for a Job
// built without New.
func maxAttempts(j *Job) int {
	if j.MaxAttempts > 0 {
		return j.MaxAttempts
	}
	return DefaultMaxAttempts
}

// job returns the job a schedule runs, or nil when this Scheduler does not
// know the schedule.
func (s *Scheduler) job(scheduleID string) *Job {
	for i := range s.config.Jobs {
		if s.config.Jobs[i].ID == scheduleID {
			return &s.config.Jobs[i]
		}
	}
	return nil
}

// survey claims and runs every due row of this Scheduler's jobs once. A row
// that this survey claims is not claimed again by the next one: the job's
// outcome is written before the next round, and only an interrupted run is
// left claimable, after its lease expires.
func (s *Scheduler) survey(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, SurveySQL, s.now(), surveyLimit)
	if err != nil {
		return fmt.Errorf("survey: %w", err)
	}
	var due []dueRow
	for rows.Next() {
		var r dueRow
		if err := rows.Scan(&r.ScheduleID, &r.Due); err != nil {
			_ = rows.Close()
			return fmt.Errorf("survey: %w", err)
		}
		if s.job(r.ScheduleID) != nil {
			due = append(due, r)
		}
	}
	err = rows.Err()
	// Close the survey before claiming: the pool can be small enough that
	// holding this cursor would leave the claim waiting for a connection.
	if cerr := rows.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("survey: %w", err)
	}
	for _, r := range due {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A leader that lost the lock mid-survey must not start more runs:
		// they would run unobserved until the lease lapsed.
		held, err := s.leadStillHeld(ctx)
		if err != nil {
			return fmt.Errorf("survey: %w", err)
		}
		if !held {
			return fmt.Errorf("survey: %w", errLockLost)
		}
		run, err := s.claim(ctx, s.job(r.ScheduleID), r)
		if errors.Is(err, errNotDue) {
			continue
		}
		if err != nil {
			s.report(err)
			continue
		}
		s.execute(ctx, s.job(run.ScheduleID), run)
	}
	return nil
}

// claim takes one due row. The claim commits before the job starts, so every
// leader sees the lease while the job runs.
func (s *Scheduler) claim(ctx context.Context, j *Job, row dueRow) (Run, error) {
	now := s.now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Run{}, fmt.Errorf("claim %s due %s: %w", row.ScheduleID, formatTime(row.Due), err)
	}
	defer func() { _ = tx.Rollback() }()
	run := Run{ScheduleID: row.ScheduleID, Due: row.Due}
	var payload RawBytes
	err = tx.QueryRowContext(ctx, claimSQL, row.ScheduleID, row.Due, s.config.Owner,
		now.Add(j.Lease), now, maxAttempts(j), intervalMillis(j.Backoff), intervalMillis(j.Every)).
		Scan(&run.Attempt, &run.MaxAttempt, &payload)
	if errors.Is(err, sql.ErrNoRows) {
		return Run{}, errNotDue
	}
	if err != nil {
		return Run{}, fmt.Errorf("claim %s due %s: %w", row.ScheduleID, formatTime(row.Due), err)
	}
	run.Payload = decodePayload(&payload)
	if err := tx.Commit(); err != nil {
		return Run{}, fmt.Errorf("claim %s due %s: %w", row.ScheduleID, formatTime(row.Due), err)
	}
	return run, nil
}

// execute runs the job under its lease. While it runs, a watcher renews the
// lease and checks the leader lock every RenewInterval; when either is gone
// it cancels the job and this scheduler records nothing more for the run, so
// the successor that reclaims it owns its outcome.
func (s *Scheduler) execute(ctx context.Context, j *Job, run Run) {
	s.runClaimed(ctx, j, run, func() bool { return true })
}

// runClaimed runs a claimed row and records its outcome unless keep reports
// that another scheduler took the row meanwhile.
func (s *Scheduler) runClaimed(ctx context.Context, j *Job, run Run, keep func() bool) {
	jobCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var lost atomic.Bool
	var haltErr atomic.Pointer[error]
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		t := time.NewTicker(renewInterval(j))
		defer t.Stop()
		for {
			select {
			case <-jobCtx.Done():
				return
			case <-t.C:
			}
			owned, err := s.ownsLease(jobCtx, j, run)
			if err == nil && !owned {
				err = errHalted
			}
			if err != nil {
				// A renewal that cannot be confirmed ends the run too: the
				// lease may already have lapsed, and another leader may own
				// the row. The cause is always the renewal's, never nil.
				haltErr.Store(&err)
				lost.Store(true)
				cancel()
				return
			}
		}
	}()
	err := j.Run(jobCtx)
	// Stop the watcher first: a check after the job ended would report a
	// lost leader before the outcome is written.
	cancel()
	<-watchDone
	reclaimed := !keep()

	switch {
	case reclaimed && !lost.Load():
		// The row belongs to another scheduler now; its outcome is theirs.
	case lost.Load():
		// A run that ended by itself before the lease was found lost is
		// still the previous owner's to report as halted; so is a lost lead.
		// The halt is reported by its cause, never as a nil error.
		if e := haltErr.Load(); e != nil {
			err = *e
		} else if err == nil || jobCtx.Err() != nil {
			err = errHalted
		}
		s.report(&JobError{ScheduleID: run.ScheduleID, Due: run.Due, Attempt: run.Attempt, Err: err})
	case err != nil:
		s.finish(ctx, j, run, err)
	default:
		s.succeed(ctx, j, run)
	}
}

// leadStillHeld reports whether this Scheduler still holds the leader lock; a
// lock that cannot be confirmed counts as lost, as it does for a run. It
// returns the reason when the lock is gone.
func (s *Scheduler) leadStillHeld(ctx context.Context) (bool, error) {
	held, err := s.leadHeld(ctx)
	return err == nil && held, err
}

// intervalMillis renders a duration as whole milliseconds, the unit the SQL
// interval parameters take; zero renders as 0. Durations below a millisecond
// are rejected by New.
func intervalMillis(d time.Duration) int64 {
	return d.Milliseconds()
}

// renewInterval is how often a run renews its lease: the job's
// RenewInterval, or Lease/4 (DefaultLease/4 for a Job built without New).
func renewInterval(j *Job) time.Duration {
	if j.RenewInterval > 0 {
		return j.RenewInterval
	}
	if j.Lease > 0 {
		return j.Lease / defaultRenewDivisor
	}
	return DefaultLease / defaultRenewDivisor
}

// ownsLease renews the run's lease and reports whether it is still this
// scheduler's to keep running: the lead is held and the row still names this
// owner. It returns no error only when the row belongs to another owner.
func (s *Scheduler) ownsLease(ctx context.Context, j *Job, run Run) (bool, error) {
	// The renewal is what excludes a duplicate run, so it starts even after
	// ctx ended; only the leader check is skipped once the lead is gone.
	base := context.WithoutCancel(ctx)
	held, err := s.leadHeld(base)
	if err != nil {
		return false, fmt.Errorf("cron: renew %s due %s: leader lock: %w", run.ScheduleID, formatTime(run.Due), err)
	}
	if !held {
		return false, nil
	}
	now := s.now()
	res, err := s.db.ExecContext(base, renewSQL, run.ScheduleID, run.Due, now.Add(j.Lease), now, s.config.Owner)
	if err != nil {
		return false, fmt.Errorf("cron: renew %s due %s: %w", run.ScheduleID, formatTime(run.Due), err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("cron: renew %s due %s: %w", run.ScheduleID, formatTime(run.Due), err)
	}
	return n == 1, nil
}

// succeed records a successful run and schedules the next occurrence of a
// recurring job.
func (s *Scheduler) succeed(ctx context.Context, j *Job, run Run) {
	now := s.now()
	res, err := s.db.ExecContext(ctx, finishSQL, run.ScheduleID, run.Due, StatusSucceeded, now, s.config.Owner)
	if err != nil {
		s.report(fmt.Errorf("cron: finish %s due %s: %w", run.ScheduleID, formatTime(run.Due), err))
		return
	}
	if n, _ := res.RowsAffected(); n != 1 {
		// Another leader reclaimed the run; scheduling on belongs to it.
		return
	}
	s.schedule(ctx, j, run, now.Add(j.Every))
}

// schedule adds the row for a later run of the schedule unless it already
// exists. A one-off job (Every zero) only ever has the rows it was given.
func (s *Scheduler) schedule(ctx context.Context, j *Job, run Run, next time.Time) {
	if j.Every <= 0 {
		return
	}
	_, err := s.db.ExecContext(ctx, scheduleNextSQL, run.ScheduleID, next, run.MaxAttempt,
		j.Backoff.Milliseconds(), j.Every.Milliseconds())
	if err != nil {
		s.report(fmt.Errorf("cron: schedule %s due %s: %w", run.ScheduleID, formatTime(next), err))
	}
}

// finish records a failed run. Below MaxAttempts the row keeps its next_due,
// so the same slot is retried once its backoff has passed; at MaxAttempts it
// is dead, and only its schedule carries on.
func (s *Scheduler) finish(ctx context.Context, j *Job, run Run, cause error) {
	dead := run.Attempt >= run.MaxAttempt
	status := StatusFailed
	if dead {
		status = StatusDead
	}
	now := s.now()
	res, err := s.db.ExecContext(ctx, finishSQL, run.ScheduleID, run.Due, status, now, s.config.Owner)
	if err != nil {
		s.report(fmt.Errorf("cron: finish %s due %s: %w", run.ScheduleID, formatTime(run.Due), err))
		return
	}
	if n, _ := res.RowsAffected(); n != 1 {
		// Another leader reclaimed the run; reporting and scheduling on
		// belong to it.
		return
	}
	s.report(&JobError{ScheduleID: run.ScheduleID, Due: run.Due, Attempt: run.Attempt, Err: cause})
	if j.Every <= 0 {
		return
	}
	// A failed row rejoins its schedule: the next occurrence may already be
	// claimed (one pending row per schedule), and the schedule keeps running
	// after it, whether or not this slot is retried. The gap covers the
	// backoff the earlier attempts took, stopping at the first slot not yet
	// used. While this run's own retry is still pending, this is a no-op.
	s.schedule(ctx, j, run, nextSlot(run.Due, now, j.Every, time.Duration(run.Attempt)*j.Backoff))
}

// nextSlot returns the first grid point, spacing every from due, that is
// past due by at least lag.
func nextSlot(due, now time.Time, every, lag time.Duration) time.Time {
	lag = max(lag, now.Sub(due)+1)
	return due.Add((lag + every - 1) / every * every)
}

// formatTime renders a due time for error text.
func formatTime(t time.Time) string {
	return t.UTC().Format(time.RFC3339Nano)
}
