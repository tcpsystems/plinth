package cron

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// querier is the subset of the service pool the Scheduler's queries use.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

// RawBytes scans any column as its unparsed text without copying, and is the
// recommended destination for json and jsonb (pgx stdlib). SQL NULL leaves it
// nil.
type RawBytes []byte

// Scan implements sql.Scanner.
func (b *RawBytes) Scan(v any) error {
	switch v := v.(type) {
	case nil:
		*b = nil
		return nil
	case []byte:
		*b = v
		return nil
	case string:
		*b = append((*b)[:0], v...)
		return nil
	default:
		return fmt.Errorf("cron: cannot scan %T into RawBytes", v)
	}
}

// Run is a due row claimed by a Scheduler.
type Run struct {
	// ScheduleID and Due identify the row; (ScheduleID, Due) is its key.
	ScheduleID string
	Due        time.Time
	// Attempt counts attempts including this one.
	Attempt    int
	MaxAttempt int
	// Payload is the row's stored payload.
	Payload []byte
}

// dueRow is a row the survey found due.
type dueRow struct {
	ScheduleID string
	Due        time.Time
}

// SurveySQL lists rows that are due. A pending row is due once next_due has
// passed. A failed row is due again once its backoff, counted from when it
// failed (updated_at), has passed. A running row whose lease expired was
// interrupted (its leader died or lost the lock): it is reclaimed by its
// next_due at once, without a backoff, because it never reported a failure.
const SurveySQL = `
SELECT schedule_id, next_due
FROM cron.schedule
WHERE (status = 'pending' AND next_due <= $1)
   OR (status = 'failed' AND updated_at + backoff <= $1)
   OR (status = 'running' AND lease_until <= $1)
ORDER BY next_due, schedule_id
LIMIT $2`

// claimSQL takes a row for this scheduler. The status conditions make it a
// compare-and-set against every other way a row changes: two claimers never
// both match one row; a succeeded or dead row is never claimed; and a
// running row is only taken by the reclaim path below, so a retry never
// races the lease of a live run. A pending or failed row starts a new
// attempt; a running row with an expired lease stays running and carries its
// attempt count over. Every claim stamps the job's MaxAttempts, backoff and
// Every on the row, so a schedule is retried and advanced as the job is
// configured now, not as the row was seeded. backoff and every pass as whole
// milliseconds; a zero every clears the column, which then falls back to the
// job.
const claimSQL = `
UPDATE cron.schedule
SET status = 'running', owner = $3, lease_until = $4,
    attempt = attempt + 1, updated_at = $5,
    max_attempts = $6,
    backoff = make_interval(secs => $7::bigint / 1000.0),
    every = CASE WHEN $8::bigint > 0 THEN make_interval(secs => $8::bigint / 1000.0) ELSE every END
WHERE schedule_id = $1 AND next_due = $2
  AND (status = 'pending' OR status = 'failed' OR (status = 'running' AND lease_until <= $5))
RETURNING attempt, max_attempts, payload`

// renewSQL extends the lease of a run this scheduler still owns.
const renewSQL = `
UPDATE cron.schedule SET lease_until = $3, updated_at = $4
WHERE schedule_id = $1 AND next_due = $2 AND status = 'running' AND owner = $5`

// finishSQL writes a run's outcome. It only matches a run this scheduler
// still owns, so a reclaimed run is never overwritten by its old owner, and
// it leaves a terminal row untouched.
const finishSQL = `
UPDATE cron.schedule SET status = $3, owner = NULL, lease_until = NULL,
    updated_at = $4
WHERE schedule_id = $1 AND next_due = $2 AND status = 'running' AND owner = $5`

// scheduleNextSQL adds the next occurrence of a recurring job unless that
// row already exists or the schedule already has a pending row. A schedule
// with a pending row needs no other: the pending one is its next
// occurrence. Every and backoff pass as whole milliseconds.
const scheduleNextSQL = `
INSERT INTO cron.schedule (schedule_id, next_due, max_attempts, backoff, every)
SELECT $1, $2, $3, make_interval(secs => $4::bigint / 1000.0), make_interval(secs => $5::bigint / 1000.0)
WHERE NOT EXISTS (
    SELECT 1 FROM cron.schedule WHERE schedule_id = $1 AND status = 'pending')
ON CONFLICT (schedule_id, next_due) DO NOTHING`

// decodePayload returns a jsonb column's JSON text, or nil for SQL NULL. The
// pgx database/sql adapter hands jsonb to the scan target as its raw JSON
// bytes (see *RawBytes), so no decoding is needed.
func decodePayload(v *RawBytes) []byte {
	if v == nil {
		return nil
	}
	return []byte(*v)
}
