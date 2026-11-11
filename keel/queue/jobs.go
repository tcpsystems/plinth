package queue

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Job states. Dead is the dead-letter state.
const (
	StatePending = "pending"
	StateRunning = "running"
	StateDone    = "done"
	StateDead    = "dead"
)

//go:embed jobs_schema.sql
var jobsSchemaSQL string

// JobsDDL returns the statements that create the job table and its indexes
// for the named relation, for use in a migration. The table may be
// schema-qualified.
func JobsDDL(table string) (string, error) {
	if err := validateName(table); err != nil {
		return "", err
	}
	name := table[strings.LastIndex(table, ".")+1:]
	return strings.NewReplacer("{{table}}", table, "{{name}}", name).Replace(jobsSchemaSQL), nil
}

// JobsConfig configures Jobs. Zero fields take the documented default.
type JobsConfig struct {
	// Table is the relation name; default "queue_job".
	Table string
	// LeaseTTL is how long a claim or heartbeat keeps a job; default 30s. A
	// running job whose lease has expired is reclaimed by the next Claim.
	LeaseTTL time.Duration
	// DefaultMaxAttempts applies to jobs enqueued without MaxAttempts; default 5.
	DefaultMaxAttempts int
	// BackoffBase is the retry delay after the first failed attempt; each
	// further attempt doubles it. Default 1s.
	BackoffBase time.Duration
	// BackoffMax caps the retry delay; default 1h.
	BackoffMax time.Duration
}

// Job is one row of the job table.
type Job struct {
	ID             int64
	Kind           string
	Payload        json.RawMessage
	IdempotencyKey string
	State          string
	// Attempts counts claims, including the current one.
	Attempts    int
	MaxAttempts int
	// Epoch is the lease epoch; pass it to Heartbeat, Complete and Fail.
	Epoch          int64
	RunAt          time.Time
	LeaseExpiresAt time.Time
	Result         json.RawMessage
	LastError      string
	CreatedAt      time.Time
	FinishedAt     time.Time
}

// NewJob describes a job to enqueue.
type NewJob struct {
	Kind    string
	Payload json.RawMessage // JSON; empty means {}
	// IdempotencyKey, when set, makes Enqueue return the existing job for
	// the same Kind and key instead of adding another.
	IdempotencyKey string
	// MaxAttempts overrides JobsConfig.DefaultMaxAttempts when positive.
	MaxAttempts int
	// RunAt delays the first claim; zero means now.
	RunAt time.Time
}

// Filter narrows what Claim considers.
type Filter struct {
	// Kinds limits the claim to these kinds; empty means every kind.
	Kinds []string
}

// Jobs is a job queue over one table: enqueue with idempotency, batched
// claims with SKIP LOCKED, leases with heartbeats, retry with exponential
// backoff and a dead-letter state. Every transition after a claim is fenced on
// the lease epoch, so a worker that lost its lease gets ErrStale. It is safe
// for concurrent use.
type Jobs struct {
	db         *sql.DB
	cfg        JobsConfig
	insert     string
	byKey      string
	claim      string
	heartbeat  string
	complete   string
	fail       string
	reap       string
	requeue    string
	dead       string
	stuck      string
	depth      string
	throughput string
}

// NewJobs returns Jobs over db. It does not reach the database.
func NewJobs(db *sql.DB, cfg JobsConfig) (*Jobs, error) {
	if db == nil {
		return nil, errors.New("queue: nil database")
	}
	if cfg.Table == "" {
		cfg.Table = "queue_job"
	}
	if cfg.LeaseTTL == 0 {
		cfg.LeaseTTL = 30 * time.Second
	}
	if cfg.DefaultMaxAttempts == 0 {
		cfg.DefaultMaxAttempts = 5
	}
	if cfg.BackoffBase == 0 {
		cfg.BackoffBase = time.Second
	}
	if cfg.BackoffMax == 0 {
		cfg.BackoffMax = time.Hour
	}
	if cfg.LeaseTTL < 0 || cfg.DefaultMaxAttempts < 1 || cfg.BackoffBase < 0 || cfg.BackoffMax < cfg.BackoffBase {
		return nil, errors.New("queue: invalid JobsConfig")
	}
	if err := validateName(cfg.Table); err != nil {
		return nil, err
	}
	r := strings.NewReplacer("{{table}}", cfg.Table, "{{cols}}", columns(""), "{{tcols}}", columns("t."))
	return &Jobs{
		db: db, cfg: cfg,
		insert:     r.Replace(jobInsertSQL),
		byKey:      r.Replace(jobByKeySQL),
		claim:      r.Replace(jobClaimSQL),
		heartbeat:  r.Replace(jobHeartbeatSQL),
		complete:   r.Replace(jobCompleteSQL),
		fail:       r.Replace(jobFailSQL),
		reap:       r.Replace(jobReapSQL),
		requeue:    r.Replace(jobRequeueSQL),
		dead:       r.Replace(jobDeadSQL),
		stuck:      r.Replace(jobStuckSQL),
		depth:      r.Replace(jobDepthSQL),
		throughput: r.Replace(jobThroughputSQL),
	}, nil
}

// jobColumns are the selected columns in scanJob order; %s is a column prefix.
var jobColumns = []string{
	"%sid", "%skind", "%spayload", "coalesce(%sidempotency_key, '')", "%sstate", "%sattempts",
	"%smax_attempts", "%slease_epoch", "%srun_at", "%slease_expires_at", "%sresult",
	"coalesce(%slast_error, '')", "%screated_at", "%sfinished_at",
}

func columns(prefix string) string {
	out := make([]string, len(jobColumns))
	for i, c := range jobColumns {
		out[i] = strings.ReplaceAll(c, "%s", prefix)
	}
	return strings.Join(out, ", ")
}

const jobInsertSQL = `INSERT INTO {{table}} (kind, payload, idempotency_key, max_attempts, run_at)
VALUES ($1, $2::jsonb, $3, $4, coalesce($5::timestamptz, now()))
ON CONFLICT (kind, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
RETURNING {{cols}}`

const jobByKeySQL = `SELECT {{cols}} FROM {{table}} WHERE kind = $1 AND idempotency_key = $2`

// jobClaimSQL claims due pending rows and running rows whose lease expired
// (while attempts remain), in one statement. Rows another claimer holds are
// skipped.
const jobClaimSQL = `UPDATE {{table}} AS t
SET state = 'running', attempts = t.attempts + 1, lease_epoch = t.lease_epoch + 1,
    lease_expires_at = now() + make_interval(secs => $2::float8), updated_at = now()
WHERE t.id IN (
	SELECT d.id FROM {{table}} AS d
	WHERE ((d.state = 'pending' AND d.run_at <= now())
	    OR (d.state = 'running' AND d.lease_expires_at <= now() AND d.attempts < d.max_attempts))
	  AND (cardinality($3::text[]) = 0 OR d.kind = ANY($3::text[]))
	ORDER BY d.run_at, d.id
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
RETURNING {{tcols}}`

const jobHeartbeatSQL = `UPDATE {{table}}
SET lease_expires_at = now() + make_interval(secs => $3::float8), updated_at = now()
WHERE id = $1 AND lease_epoch = $2 AND state = 'running'`

const jobCompleteSQL = `UPDATE {{table}}
SET state = 'done', result = $3::jsonb, last_error = NULL, finished_at = now(),
    lease_expires_at = NULL, lease_epoch = lease_epoch + 1, updated_at = now()
WHERE id = $1 AND lease_epoch = $2 AND state = 'running'`

// jobFailSQL retries with delay base * 2^(attempts-1) capped at max ($4, $5),
// or dead-letters the row once attempts reach max_attempts. The exponent is
// capped so power() cannot overflow.
const jobFailSQL = `UPDATE {{table}}
SET state = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'pending' END,
    run_at = CASE WHEN attempts >= max_attempts THEN run_at
             ELSE now() + make_interval(secs => least($5::float8,
                  $4::float8 * power(2::float8, least(attempts - 1, 30)::float8))) END,
    finished_at = CASE WHEN attempts >= max_attempts THEN now() END,
    last_error = $3, lease_expires_at = NULL, lease_epoch = lease_epoch + 1, updated_at = now()
WHERE id = $1 AND lease_epoch = $2 AND state = 'running'
RETURNING state`

// jobReapSQL dead-letters running rows whose lease expired with no attempts
// left; Claim cannot reclaim those.
const jobReapSQL = `UPDATE {{table}}
SET state = 'dead', last_error = 'lease expired on the final attempt', finished_at = now(),
    lease_expires_at = NULL, lease_epoch = lease_epoch + 1, updated_at = now()
WHERE state = 'running' AND lease_expires_at <= now() AND attempts >= max_attempts`

const jobRequeueSQL = `UPDATE {{table}}
SET state = 'pending', attempts = 0, run_at = now(), finished_at = NULL,
    lease_epoch = lease_epoch + 1, updated_at = now()
WHERE id = $1 AND state = 'dead'`

const jobDeadSQL = `SELECT {{cols}} FROM {{table}} WHERE state = 'dead' ORDER BY finished_at DESC, id DESC LIMIT $1`

const jobStuckSQL = `SELECT {{cols}} FROM {{table}}
WHERE state = 'running' AND lease_expires_at <= now() - make_interval(secs => $1::float8)
ORDER BY lease_expires_at, id LIMIT $2`

const jobDepthSQL = `SELECT
    count(*) FILTER (WHERE state = 'pending' AND run_at <= now()),
    count(*) FILTER (WHERE state = 'pending' AND run_at > now()),
    count(*) FILTER (WHERE state = 'running'),
    count(*) FILTER (WHERE state = 'done'),
    count(*) FILTER (WHERE state = 'dead')
FROM {{table}}`

const jobThroughputSQL = `SELECT
    count(*) FILTER (WHERE state = 'done'),
    count(*) FILTER (WHERE state = 'dead')
FROM {{table}}
WHERE finished_at >= now() - make_interval(secs => $1::float8)`

// Enqueue adds a job. With an IdempotencyKey, a job of the same Kind and key
// that already exists is returned unchanged and created is false; concurrent
// enqueues of one key add exactly one row.
func (j *Jobs) Enqueue(ctx context.Context, n NewJob) (job Job, created bool, err error) {
	if n.Kind == "" {
		return Job{}, false, errors.New("queue: enqueue: empty kind")
	}
	payload := "{}"
	if len(n.Payload) > 0 {
		if !json.Valid(n.Payload) {
			return Job{}, false, errors.New("queue: enqueue: payload is not valid JSON")
		}
		payload = string(n.Payload)
	}
	var key, runAt any
	if n.IdempotencyKey != "" {
		key = n.IdempotencyKey
	}
	if !n.RunAt.IsZero() {
		runAt = n.RunAt
	}
	maxAttempts := n.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = j.cfg.DefaultMaxAttempts
	}
	job, err = scanJob(j.db.QueryRowContext(ctx, j.insert, n.Kind, payload, key, maxAttempts, runAt))
	switch {
	case err == nil:
		return job, true, nil
	case errors.Is(err, sql.ErrNoRows) && n.IdempotencyKey != "":
		// The conflicting row is committed by now: ON CONFLICT waits for it.
		job, err = scanJob(j.db.QueryRowContext(ctx, j.byKey, n.Kind, n.IdempotencyKey))
		if err != nil {
			return Job{}, false, fmt.Errorf("queue: enqueue: load existing: %w", err)
		}
		return job, false, nil
	default:
		return Job{}, false, fmt.Errorf("queue: enqueue: %w", err)
	}
}

// Claim leases up to max due jobs for LeaseTTL: pending jobs whose run_at has
// passed, and running jobs whose lease expired (a crashed or stalled worker),
// provided attempts remain. It first dead-letters expired jobs that have none
// left. Concurrent claimers get disjoint sets. max is clamped to 1..MaxBatch.
func (j *Jobs) Claim(ctx context.Context, limit int, f Filter) ([]Job, error) {
	if _, err := j.Reap(ctx); err != nil {
		return nil, err
	}
	if limit < 1 {
		limit = 1
	}
	if limit > MaxBatch {
		limit = MaxBatch
	}
	kinds := f.Kinds
	if kinds == nil {
		kinds = []string{}
	}
	rows, err := j.db.QueryContext(ctx, j.claim, limit, j.cfg.LeaseTTL.Seconds(), kinds)
	if err != nil {
		return nil, fmt.Errorf("queue: claim: %w", err)
	}
	return collect(rows, "claim")
}

// Heartbeat extends the lease of a claimed job by LeaseTTL. It returns
// ErrStale if the job was reclaimed, completed or failed.
func (j *Jobs) Heartbeat(ctx context.Context, id, epoch int64) error {
	return j.exec(ctx, "heartbeat", j.heartbeat, id, epoch, j.cfg.LeaseTTL.Seconds())
}

// Complete marks the job done and stores result, which must be valid JSON or
// empty. It returns ErrStale if the caller no longer holds the lease; a second
// Complete of the same lease is stale.
func (j *Jobs) Complete(ctx context.Context, id, epoch int64, result json.RawMessage) error {
	var res any
	if len(result) > 0 {
		if !json.Valid(result) {
			return errors.New("queue: complete: result is not valid JSON")
		}
		res = string(result)
	}
	return j.exec(ctx, "complete", j.complete, id, epoch, res)
}

// Fail records cause and releases the job. While attempts remain it is
// retried after BackoffBase * 2^(attempts-1), capped at BackoffMax; once
// Attempts reaches MaxAttempts it becomes dead. It returns the new state,
// StatePending or StateDead, or ErrStale if the caller no longer holds the
// lease.
func (j *Jobs) Fail(ctx context.Context, id, epoch int64, cause error) (string, error) {
	msg := "unspecified failure"
	if cause != nil {
		msg = cause.Error()
	}
	var state string
	err := j.db.QueryRowContext(ctx, j.fail, id, epoch, msg,
		j.cfg.BackoffBase.Seconds(), j.cfg.BackoffMax.Seconds()).Scan(&state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("queue: fail job %d epoch %d: %w", id, epoch, ErrStale)
	}
	if err != nil {
		return "", fmt.Errorf("queue: fail: %w", err)
	}
	return state, nil
}

// Reap moves running jobs whose lease expired on their final attempt to the
// dead-letter state and returns how many it moved. Claim calls it; call it on
// a timer too if nothing claims for long stretches.
func (j *Jobs) Reap(ctx context.Context) (int64, error) {
	res, err := j.db.ExecContext(ctx, j.reap)
	if err != nil {
		return 0, fmt.Errorf("queue: reap: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("queue: reap: rows affected: %w", err)
	}
	return n, nil
}

// Requeue returns a dead job to pending with its attempts reset. It returns
// ErrStale if the job is not dead.
func (j *Jobs) Requeue(ctx context.Context, id int64) error {
	res, err := j.db.ExecContext(ctx, j.requeue, id)
	if err != nil {
		return fmt.Errorf("queue: requeue: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return fmt.Errorf("queue: requeue job %d: %w", id, ErrStale)
	}
	return nil
}

// DeadLetters lists up to limit dead jobs, most recently dead first.
func (j *Jobs) DeadLetters(ctx context.Context, limit int) ([]Job, error) {
	rows, err := j.db.QueryContext(ctx, j.dead, clampLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("queue: dead letters: %w", err)
	}
	return collect(rows, "dead letters")
}

// Stuck lists up to limit running jobs whose lease expired more than grace
// ago and that nothing has reclaimed: the workers are gone and no claimer is
// polling, or the job has no attempts left and Reap has not run.
func (j *Jobs) Stuck(ctx context.Context, grace time.Duration, limit int) ([]Job, error) {
	if grace < 0 {
		grace = 0
	}
	rows, err := j.db.QueryContext(ctx, j.stuck, grace.Seconds(), clampLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("queue: stuck: %w", err)
	}
	return collect(rows, "stuck")
}

// Depth counts jobs by state.
type Depth struct {
	// Ready are pending jobs whose run_at has passed.
	Ready int64
	// Delayed are pending jobs waiting for run_at, including retry backoff.
	Delayed int64
	Running int64
	Done    int64
	Dead    int64
}

// Depth returns the queue depth.
func (j *Jobs) Depth(ctx context.Context) (Depth, error) {
	var d Depth
	err := j.db.QueryRowContext(ctx, j.depth).Scan(&d.Ready, &d.Delayed, &d.Running, &d.Done, &d.Dead)
	if err != nil {
		return Depth{}, fmt.Errorf("queue: depth: %w", err)
	}
	return d, nil
}

// Throughput is the number of jobs that finished within a window.
type Throughput struct {
	Window time.Duration
	Done   int64
	Dead   int64
	// PerSecond is Done divided by the window.
	PerSecond float64
}

// Throughput counts jobs completed or dead-lettered in the last window.
func (j *Jobs) Throughput(ctx context.Context, window time.Duration) (Throughput, error) {
	if window <= 0 {
		return Throughput{}, errors.New("queue: throughput: window must be positive")
	}
	t := Throughput{Window: window}
	if err := j.db.QueryRowContext(ctx, j.throughput, window.Seconds()).Scan(&t.Done, &t.Dead); err != nil {
		return Throughput{}, fmt.Errorf("queue: throughput: %w", err)
	}
	t.PerSecond = float64(t.Done) / window.Seconds()
	return t, nil
}

func clampLimit(n int) int {
	if n < 1 {
		return 1
	}
	if n > MaxBatch {
		return MaxBatch
	}
	return n
}

func (j *Jobs) exec(ctx context.Context, op, stmt string, args ...any) error {
	res, err := j.db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return fmt.Errorf("queue: %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("queue: %s: rows affected: %w", op, err)
	}
	if n == 0 {
		return fmt.Errorf("queue: %s job %d epoch %d: %w", op, args[0], args[1], ErrStale)
	}
	return nil
}

type scanner interface{ Scan(dest ...any) error }

func scanJob(s scanner) (Job, error) {
	var (
		job     Job
		payload []byte
		result  []byte
		lease   sql.NullTime
		done    sql.NullTime
	)
	err := s.Scan(&job.ID, &job.Kind, &payload, &job.IdempotencyKey, &job.State, &job.Attempts,
		&job.MaxAttempts, &job.Epoch, &job.RunAt, &lease, &result, &job.LastError, &job.CreatedAt, &done)
	if err != nil {
		return Job{}, err
	}
	job.Payload = payload
	job.Result = result
	job.LeaseExpiresAt = lease.Time
	job.FinishedAt = done.Time
	return job, nil
}

func collect(rows *sql.Rows, op string) ([]Job, error) {
	defer func() { _ = rows.Close() }()
	var out []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("queue: %s: scan: %w", op, err)
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: %s: %w", op, err)
	}
	return out, nil
}
