// Package jobs is a durable job store on PostgreSQL. A job is enqueued in the
// caller's transaction, so it commits or rolls back with the business write
// that scheduled it. Claimers take due jobs with FOR UPDATE SKIP LOCKED under a
// lease, so two claimers never run the same job at once; a lease that runs out
// is claimable again. On failure a job is retried with backoff until its
// attempts are spent and then stays visible in the failed state with its last
// error.
//
// The SQL schema is in schema.sql. The package uses database/sql only, with a
// driver injected by the caller. Worker (worker.go) is the poll loop that
// claims jobs, runs the handler registered for each kind and settles every
// job with Complete or Fail; the service owns the worker and its lifetime.
package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// State is a job's lifecycle state.
type State string

// Job states.
const (
	Queued  State = "queued"  // claimable once run_at has passed
	Running State = "running" // claimed, lease held until LockedUntil
	Done    State = "done"    // finished successfully
	Failed  State = "failed"  // attempts exhausted; LastError is kept
)

// DefaultMaxAttempts is the max_attempts used when a Job does not set one.
const DefaultMaxAttempts = 10

// Job is one row of the jobs table.
type Job struct {
	ID          int64
	Kind        string          // required; names the handler
	Payload     json.RawMessage // optional, must be valid JSON
	State       State
	Attempts    int // claims so far; Claim increments it
	MaxAttempts int
	RunAt       time.Time // earliest claim time; backoff moves it later
	LockedUntil time.Time // end of the current running lease, zero when unleased
	LastError   string    // error of the most recent failed attempt
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

const columns = `id, kind, payload, state, attempts, max_attempts, run_at, locked_until, last_error, created_at, updated_at`

// Enqueue inserts j using tx and returns its id. It never commits: the job
// commits with the caller's business write, or rolls back with it.
//
// Kind is required. Payload is optional and must be valid JSON. MaxAttempts
// defaults to DefaultMaxAttempts; a zero RunAt means now on the database clock.
func Enqueue(ctx context.Context, tx *sql.Tx, j Job) (int64, error) {
	if tx == nil {
		return 0, errors.New("jobs: nil transaction")
	}
	if j.Kind == "" {
		return 0, errors.New("jobs: kind is required")
	}
	var payload any
	if len(j.Payload) > 0 {
		if !json.Valid(j.Payload) {
			return 0, errors.New("jobs: payload must be valid JSON")
		}
		payload = []byte(j.Payload)
	}
	maxAttempts := j.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = DefaultMaxAttempts
	}
	var runAt any
	if !j.RunAt.IsZero() {
		runAt = j.RunAt
	}
	var id int64
	err := tx.QueryRowContext(ctx,
		`INSERT INTO jobs (kind, payload, max_attempts, run_at)
		 VALUES ($1, COALESCE($2::jsonb, '{}'::jsonb), $3, COALESCE($4::timestamptz, now()))
		 RETURNING id`,
		j.Kind, payload, maxAttempts, runAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("jobs: enqueue: %w", err)
	}
	return id, nil
}

// Claim takes up to n due jobs for lease and marks them running. A job is due
// when it is queued and RunAt has passed, or when it is running and its lease
// has expired. Rows are picked with FOR UPDATE SKIP LOCKED, so concurrent
// claimers never receive the same job. Each claimed job's Attempts is
// incremented.
//
// Claim returns no error when fewer than n jobs are due. n and lease must be
// positive.
func Claim(ctx context.Context, db *sql.DB, n int, lease time.Duration) ([]Job, error) {
	if db == nil {
		return nil, errors.New("jobs: nil database")
	}
	if n <= 0 {
		return nil, errors.New("jobs: claim count must be positive")
	}
	if lease <= 0 {
		return nil, errors.New("jobs: lease must be positive")
	}
	// The lease travels as a microsecond count so it needs no parsing of a
	// textual interval.
	rows, err := db.QueryContext(ctx,
		`WITH due AS (
		     SELECT id FROM jobs
		     WHERE (state = 'queued' AND run_at <= now())
		        OR (state = 'running' AND locked_until <= now())
		     ORDER BY run_at, id
		     LIMIT $1
		     FOR UPDATE SKIP LOCKED
		 )
		 UPDATE jobs SET
		     state = 'running',
		     attempts = attempts + 1,
		     locked_until = now() + make_interval(secs => $2::double precision / 1000000),
		     updated_at = now()
		 WHERE id IN (SELECT id FROM due)
		 RETURNING `+columns,
		n, strconv.FormatInt(lease.Microseconds(), 10))
	if err != nil {
		return nil, fmt.Errorf("jobs: claim: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("jobs: claim: %w", err)
		}
		out = append(out, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("jobs: claim: %w", err)
	}
	return out, nil
}

// Complete marks job id done and clears its lease.
func Complete(ctx context.Context, db *sql.DB, id int64) error {
	return execUpdate(ctx, db, "complete",
		`UPDATE jobs SET state = 'done', locked_until = NULL, updated_at = now() WHERE id = $1`, id)
}

// Fail records a failed attempt of job id. msg is kept as last_error. While
// Attempts is below MaxAttempts the job returns to queued with RunAt moved
// later by backoff(Attempts); at MaxAttempts it becomes failed and stays
// visible with last_error. A nil backoff uses DefaultBackoff.
func Fail(ctx context.Context, db *sql.DB, id int64, msg error, backoff func(attempt int) time.Duration) error {
	if msg == nil {
		return errors.New("jobs: fail needs the attempt error")
	}
	if backoff == nil {
		backoff = DefaultBackoff
	}
	attempt, maxAttempts, err := readAttempts(ctx, db, id)
	if err != nil {
		return err
	}
	if attempt >= maxAttempts {
		return execUpdate(ctx, db, "fail",
			`UPDATE jobs SET state = 'failed', last_error = $2, locked_until = NULL, updated_at = now()
			 WHERE id = $1 AND state = 'running' AND attempts = $3`, id, msg.Error(), attempt)
	}
	// The backoff function is caller code: compute it before the write, and
	// keep the row lock only for the update itself.
	delay := backoff(attempt)
	if delay < 0 {
		delay = 0
	}
	return execUpdate(ctx, db, "fail",
		`UPDATE jobs SET state = 'queued', last_error = $2,
		     run_at = now() + make_interval(secs => $3::double precision / 1000000),
		     locked_until = NULL, updated_at = now()
		 WHERE id = $1 AND state = 'running' AND attempts = $4`, id, msg.Error(), delay.Microseconds(), attempt)
}

// DefaultBackoff is exponential: 1s, 2s, 4s, ... capped at 5 minutes.
func DefaultBackoff(attempt int) time.Duration {
	return ExponentialBackoff(time.Second, 5*time.Minute)(attempt)
}

// ExponentialBackoff returns a backoff that doubles base for each further
// attempt (attempt 1 waits base) and never exceeds limit. Non-positive base
// or limit fall back to DefaultBackoff's values.
func ExponentialBackoff(base, limit time.Duration) func(attempt int) time.Duration {
	if base <= 0 {
		base = time.Second
	}
	if limit <= 0 {
		limit = 5 * time.Minute
	}
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := base
		for i := 1; i < attempt && d < limit; i++ {
			if d > limit/2 {
				return limit
			}
			d *= 2
		}
		if d > limit {
			return limit
		}
		return d
	}
}

// record is what scanJob reads: a value backed by either *sql.Row or *sql.Rows.
type record interface {
	Scan(dest ...any) error
}

func scanJob(r record) (Job, error) {
	var j Job
	var payload []byte
	var state string
	var runAt, created, updated time.Time
	var lockedUntil sql.NullTime
	var lastError sql.NullString
	if err := r.Scan(&j.ID, &j.Kind, &payload, &state, &j.Attempts, &j.MaxAttempts,
		&runAt, &lockedUntil, &lastError, &created, &updated); err != nil {
		return Job{}, err
	}
	j.Payload = payload
	j.State = State(state)
	j.RunAt = runAt
	j.LockedUntil = lockedUntil.Time
	j.LastError = lastError.String
	j.CreatedAt = created
	j.UpdatedAt = updated
	return j, nil
}

func readAttempts(ctx context.Context, db *sql.DB, id int64) (attempt, maxAttempts int, err error) {
	if db == nil {
		return 0, 0, errors.New("jobs: nil database")
	}
	err = db.QueryRowContext(ctx, `SELECT attempts, max_attempts FROM jobs WHERE id = $1`, id).Scan(&attempt, &maxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, fmt.Errorf("jobs: job %d: %w", id, sql.ErrNoRows)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("jobs: read job %d: %w", id, err)
	}
	return attempt, maxAttempts, nil
}

func execUpdate(ctx context.Context, db *sql.DB, op, q string, args ...any) error {
	if db == nil {
		return errors.New("jobs: nil database")
	}
	if _, err := db.ExecContext(ctx, q, args...); err != nil {
		return fmt.Errorf("jobs: %s: %w", op, err)
	}
	return nil
}
