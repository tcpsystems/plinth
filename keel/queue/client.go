package queue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverdatabasesql"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

// Default tuning for Client. Every value can be overridden in Config.
const (
	// DefaultLease is how long a job may stay running before the leader
	// treats its worker as dead and reschedules it.
	DefaultLease = 5 * time.Minute
	// DefaultMaxAttempts is how many times a job runs before it is dead.
	DefaultMaxAttempts = 8
	// DefaultBackoffBase is the delay after the first failed attempt.
	DefaultBackoffBase = 5 * time.Second
	// DefaultBackoffMax caps the delay between attempts.
	DefaultBackoffMax = 15 * time.Minute
	// DefaultWorkers is the concurrent jobs per client on the default queue.
	DefaultWorkers = 10
)

// Dead is the state of a job that used all its attempts. Dead jobs are kept
// for inspection and are never run again.
const Dead = rivertype.JobStateDiscarded

// Config configures a Client. Only DB is required.
type Config struct {
	// DB is the pool River works on and migrates. It must be a PostgreSQL
	// database/sql pool; the caller owns it and closes it after Stop.
	DB *sql.DB
	// Workers registers the job kinds this client runs. Leave nil for an
	// insert-only client that is never started.
	Workers *river.Workers
	// Workers per queue. Nil means river.QueueDefault with DefaultWorkers.
	Queues map[string]river.QueueConfig
	// Lease is how long a running job may go without finishing before it is
	// rescued: retried with backoff, or dead when attempts are used up. Set
	// it above the longest legitimate job; a job that outlives its lease can
	// run twice. The per-job timeout is set to nine tenths of the lease, so a
	// job is cancelled before it can be rescued. Zero means DefaultLease.
	Lease time.Duration
	// MaxAttempts per job, including the first run. Zero means
	// DefaultMaxAttempts. A job's own InsertOpts.MaxAttempts wins.
	MaxAttempts int
	// BackoffBase and BackoffMax shape the retry delay, see Backoff. Zero
	// means DefaultBackoffBase and DefaultBackoffMax.
	BackoffBase time.Duration
	BackoffMax  time.Duration
	// Periodic jobs are enqueued by the leader only, so a fleet of clients
	// inserts each occurrence once.
	Periodic []*river.PeriodicJob
	// NoLeader keeps this client out of leader election: it works jobs but
	// never runs maintenance, rescue or periodic enqueues. Use it for clients
	// with a partial worker set. Periodic must be empty when it is set.
	NoLeader bool
	// SkipMigrate stops Start from applying River's schema migrations. Set it
	// when the role keel runs as cannot create tables and Migrate runs
	// elsewhere.
	SkipMigrate bool
	// Schema is the PostgreSQL schema holding River's tables. It must already
	// exist. Empty means the connection's search path, normally public.
	Schema string
	// FetchPollInterval and FetchCooldown tune how often idle workers look
	// for new jobs. Zero keeps River's defaults.
	FetchPollInterval time.Duration
	FetchCooldown     time.Duration
	// Logger receives River's logs. Nil discards them.
	Logger *slog.Logger
}

// Client is a River client over database/sql. Jobs are inserted with the
// caller's transaction, workers claim them with row locks that never block
// other workers, and the elected leader runs maintenance: scheduling,
// rescuing stuck jobs, and cleaning finished ones. It is safe for concurrent
// use.
type Client struct {
	rc          *river.Client[*sql.Tx]
	db          *sql.DB
	schema      string
	skipMigrate bool
}

// NewClient validates cfg and builds a client. It does not reach the database.
func NewClient(cfg Config) (*Client, error) {
	if cfg.DB == nil {
		return nil, errors.New("queue: nil database")
	}
	if cfg.Lease < 0 || cfg.MaxAttempts < 0 || cfg.BackoffBase < 0 || cfg.BackoffMax < 0 {
		return nil, errors.New("queue: lease, attempts and backoff must not be negative")
	}
	lease := orDuration(cfg.Lease, DefaultLease)
	base := orDuration(cfg.BackoffBase, DefaultBackoffBase)
	limit := orDuration(cfg.BackoffMax, DefaultBackoffMax)
	if limit < base {
		return nil, errors.New("queue: BackoffMax is below BackoffBase")
	}
	rcfg := &river.Config{
		Workers:              cfg.Workers,
		MaxAttempts:          orInt(cfg.MaxAttempts, DefaultMaxAttempts),
		RescueStuckJobsAfter: lease,
		// River requires the job timeout to stay below the rescue threshold,
		// otherwise a job could be rescued while still inside its timeout.
		JobTimeout:        lease - lease/10,
		RetryPolicy:       Backoff{Base: base, Max: limit},
		PeriodicJobs:      cfg.Periodic,
		FetchPollInterval: cfg.FetchPollInterval,
		FetchCooldown:     cfg.FetchCooldown,
		Logger:            cfg.Logger,
		Schema:            cfg.Schema,

		LeaderElectionDisabled: cfg.NoLeader,
	}
	if rcfg.Logger == nil {
		rcfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Workers != nil {
		rcfg.Queues = cfg.Queues
		if rcfg.Queues == nil {
			rcfg.Queues = map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: DefaultWorkers}}
		}
	}
	rc, err := river.NewClient(riverdatabasesql.New(cfg.DB), rcfg)
	if err != nil {
		return nil, fmt.Errorf("queue: new client: %w", err)
	}
	return &Client{rc: rc, db: cfg.DB, schema: cfg.Schema, skipMigrate: cfg.SkipMigrate}, nil
}

// Migrate brings River's tables in schema (empty for the search path) up to
// date. It is idempotent and safe to run from several processes at once.
// Client.Start calls it unless SkipMigrate is set.
func Migrate(ctx context.Context, db *sql.DB, schema string) error {
	if db == nil {
		return errors.New("queue: nil database")
	}
	m, err := rivermigrate.New(riverdatabasesql.New(db), &rivermigrate.Config{Schema: schema})
	if err != nil {
		return fmt.Errorf("queue: migrator: %w", err)
	}
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("queue: migrate: %w", err)
	}
	return nil
}

// Start applies River's migrations (unless skipped) and begins working jobs,
// electing a leader for maintenance and periodic jobs. It returns once the
// client is running; call Stop to shut down.
func (c *Client) Start(ctx context.Context) error {
	if !c.skipMigrate {
		if err := Migrate(ctx, c.db, c.schema); err != nil {
			return err
		}
	}
	return c.rc.Start(ctx)
}

// Stop stops fetching new jobs and waits for running ones to finish or ctx to
// end, whichever comes first.
func (c *Client) Stop(ctx context.Context) error { return c.rc.Stop(ctx) }

// InsertTx enqueues a job in the caller's transaction. The job exists only if
// tx commits, and workers cannot see it before then.
func (c *Client) InsertTx(ctx context.Context, tx *sql.Tx, args river.JobArgs, opts *river.InsertOpts) (*rivertype.JobInsertResult, error) {
	if tx == nil {
		return nil, errors.New("queue: nil transaction")
	}
	return c.rc.InsertTx(ctx, tx, args, opts)
}

// InsertManyTx enqueues jobs in the caller's transaction. Jobs whose opts set
// UniqueOpts and match an existing job are skipped; the matching result has
// UniqueSkippedAsDuplicate set. Results are in input order. One batch must not
// contain the same unique key twice: PostgreSQL rejects the statement.
func (c *Client) InsertManyTx(ctx context.Context, tx *sql.Tx, jobs []river.InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	if tx == nil {
		return nil, errors.New("queue: nil transaction")
	}
	return c.rc.InsertManyTx(ctx, tx, jobs)
}

// InsertMany enqueues jobs in one transaction of its own. Use InsertManyTx when
// the jobs must commit with other changes.
func (c *Client) InsertMany(ctx context.Context, jobs []river.InsertManyParams) ([]*rivertype.JobInsertResult, error) {
	return c.rc.InsertMany(ctx, jobs)
}

// Idempotent returns insert options under which a second insert of the same
// job is skipped for as long as the first one is pending, running, retrying,
// scheduled or completed (completed rows live until River's cleaner removes
// them, 24 hours by default).
//
// A job is identified by its args: tag the field that carries the idempotency
// key with `river:"unique"` and only tagged fields are compared; with no tag
// the whole args value is. Dead and cancelled jobs do not block a new insert.
// base may be nil.
func Idempotent(base *river.InsertOpts) *river.InsertOpts {
	var o river.InsertOpts
	if base != nil {
		o = *base
	}
	o.UniqueOpts = river.UniqueOpts{
		ByArgs: true,
		ByState: []rivertype.JobState{
			rivertype.JobStateAvailable,
			rivertype.JobStatePending,
			rivertype.JobStateRunning,
			rivertype.JobStateRetryable,
			rivertype.JobStateScheduled,
			rivertype.JobStateCompleted,
		},
	}
	return &o
}

// Every is a periodic schedule that fires every d after the previous run.
func Every(d time.Duration) river.PeriodicSchedule { return river.PeriodicInterval(d) }

// Periodic describes cron-style work: Make is called each time the schedule
// fires and returns the job to enqueue.
type Periodic struct {
	// ID names the schedule; it must be unique per client.
	ID string
	// Schedule says when to fire. Use Every, or any river.PeriodicSchedule
	// such as one backed by a cron parser.
	Schedule river.PeriodicSchedule
	// Make builds the job for one occurrence.
	Make func() (river.JobArgs, *river.InsertOpts)
	// RunOnStart also enqueues one occurrence when a leader starts, so a long
	// interval does not wait for the first tick after a failover.
	RunOnStart bool
}

// PeriodicJob converts p for Config.Periodic.
func PeriodicJob(p Periodic) *river.PeriodicJob {
	return river.NewPeriodicJob(p.Schedule, p.Make, &river.PeriodicJobOpts{ID: p.ID, RunOnStart: p.RunOnStart})
}

// AddPeriodic registers p on a running or stopped client. Only the leader
// enqueues it.
func (c *Client) AddPeriodic(p Periodic) error {
	if p.ID == "" || p.Schedule == nil || p.Make == nil {
		return errors.New("queue: periodic job needs ID, Schedule and Make")
	}
	_, err := c.rc.PeriodicJobs().AddSafely(PeriodicJob(p))
	return err
}

// Backoff is a bounded exponential retry policy: the delay after attempt n is
// Base * 2^(n-1), never more than Max. It plugs into River as the client's
// retry policy.
type Backoff struct {
	Base time.Duration
	Max  time.Duration
}

// Delay returns the wait before the run that follows the given failed attempt
// (1 for the first failure).
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(b.Base) * math.Pow(2, float64(attempt-1))
	if d >= float64(b.Max) || math.IsInf(d, 0) || math.IsNaN(d) {
		return b.Max
	}
	return time.Duration(d)
}

// NextRetry implements river.ClientRetryPolicy.
func (b Backoff) NextRetry(job *rivertype.JobRow) time.Time {
	return time.Now().UTC().Add(b.Delay(job.Attempt))
}

func orDuration(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

func orInt(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}
