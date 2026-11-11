// Package queue claims due rows from a PostgreSQL job table in batches and
// fences the owner with a lease epoch. Client, in client.go, is a River-backed
// durable queue for work enqueued from application transactions.
//
// A job table claimed by Queue has these columns:
//
//	id          bigint      primary key
//	priority    integer     lower is claimed first
//	next_due    timestamptz the row is due when next_due <= now()
//	state       text        'pending' before a claim, 'claimed' after
//	lease_epoch bigint      bumped on every claim; the owner's fencing token
//
// ClaimN runs one statement, so it is its own transaction: the subquery locks
// the rows it picks with FOR UPDATE SKIP LOCKED and the update claims exactly
// those rows. Settle and Fail run on the caller's *sql.DB, each on a pooled
// connection.
//
// Fencing: ClaimN returns each row's new lease_epoch. Settle and Fail only
// update the row while state = 'claimed' AND lease_epoch = the caller's
// epoch, so an owner whose lease was reclaimed (or who settles twice)
// reports ErrStale and must stop touching the row.
package queue

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// ErrStale reports that Settle or Fail no longer owns the row: it was
// reclaimed and claimed by another owner, settled, or already failed.
var ErrStale = errors.New("queue: lease is stale; the row was reclaimed or settled")

// Claimed is one row handed to a claimer by ClaimN.
type Claimed struct {
	// ID is the row's key.
	ID int64
	// Epoch is the row's lease_epoch after the claim; pass it to Settle and
	// Fail. It is strictly larger than any epoch a previous owner held.
	Epoch int64
}

// Schema names the table, columns and states Queue reads and writes. The zero
// value is the schema in the package documentation, on table "job".
type Schema struct {
	// Table is the relation name, optionally schema-qualified ("schema.job").
	Table string
	// ID is the bigint key column.
	ID string
	// Priority orders claims; lower is claimed first.
	Priority string
	// NextDue is the timestamptz column; a row is due once it has passed.
	NextDue string
	// State is the text column holding Pending or Claimed.
	State string
	// Epoch is the bigint lease column; every claim adds one.
	Epoch string
	// Pending is the state of a row waiting to be claimed. ClaimN only
	// considers rows in this state.
	Pending string
	// Claimed is the state ClaimN sets and Settle and Fail require.
	Claimed string
}

// MaxBatch caps ClaimN's n. Rows claimed together share one owner, so larger
// batches amortize the round trip at the cost of a coarser split between
// claimers.
const MaxBatch = 1000

// Default is the schema documented on the package, on table "job".
var Default = Schema{
	Table:    "job",
	ID:       "id",
	Priority: "priority",
	NextDue:  "next_due",
	State:    "state",
	Epoch:    "lease_epoch",
	Pending:  "pending",
	Claimed:  "claimed",
}

// withDefaults fills zero fields from Default and validates the names.
func (s Schema) withDefaults() (Schema, error) {
	d := Default
	for _, f := range []struct {
		got *string
		def string
	}{
		{&s.Table, d.Table}, {&s.ID, d.ID}, {&s.Priority, d.Priority}, {&s.NextDue, d.NextDue},
		{&s.State, d.State}, {&s.Epoch, d.Epoch}, {&s.Pending, d.Pending}, {&s.Claimed, d.Claimed},
	} {
		if *f.got == "" {
			*f.got = f.def
		}
	}
	if err := validateName(s.Table); err != nil {
		return Schema{}, err
	}
	for _, col := range []string{s.ID, s.Priority, s.NextDue, s.State, s.Epoch} {
		if err := validateName(col); err != nil {
			return Schema{}, err
		}
		if strings.Contains(col, ".") {
			return Schema{}, fmt.Errorf("queue: column name %q must be a plain identifier", col)
		}
	}
	return s, nil
}

// validateName accepts a relation name (parts joined by one dot) or a plain
// identifier: an ASCII letter or underscore, then letters, digits, underscores
// or dollars, at most 63 bytes per part. These names come from the program,
// never from users, so they are interpolated without quoting; anything outside
// this set is rejected before it can reach a statement.
func validateName(name string) error {
	for _, part := range strings.Split(name, ".") {
		if part == "" || len(part) > 63 {
			return fmt.Errorf("queue: invalid name %q", name)
		}
		for i := 0; i < len(part); i++ {
			c := part[i]
			switch {
			case c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z':
			case i > 0 && (c >= '0' && c <= '9' || c == '$'):
			default:
				return fmt.Errorf("queue: invalid name %q", name)
			}
		}
	}
	return nil
}

// Queue claims, settles and fails rows of one job table on one pool. It is
// safe for concurrent use.
type Queue struct {
	db     *sql.DB
	claim  string
	settle string
	fail   string
}

// New returns a Queue over db that uses Default. It does not reach the
// database; the table is checked by the first statement.
func New(db *sql.DB) (*Queue, error) {
	return NewWithSchema(db, Default)
}

// NewWithSchema returns a Queue over db for the named schema. Fields left
// zero take their Default value.
func NewWithSchema(db *sql.DB, schema Schema) (*Queue, error) {
	if db == nil {
		return nil, errors.New("queue: nil database")
	}
	s, err := schema.withDefaults()
	if err != nil {
		return nil, err
	}
	// Single-pass Replacer: substituted names are never rescanned, so a column
	// named like a placeholder cannot change which text is substituted.
	names := strings.NewReplacer(
		"{{table}}", s.Table,
		"{{id}}", s.ID,
		"{{priority}}", s.Priority,
		"{{next_due}}", s.NextDue,
		"{{state}}", s.State,
		"{{epoch}}", s.Epoch,
		"{{pending}}", s.Pending,
		"{{claimed}}", s.Claimed,
	)
	return &Queue{
		db:     db,
		claim:  names.Replace(claimSQL),
		settle: names.Replace(settleSQL),
		fail:   names.Replace(failSQL),
	}, nil
}

// claimSQL claims one batch in one statement. The subquery picks the n best
// due rows in claim order and locks them, skipping rows another claimer holds
// right now. Because the lock is held by this statement's transaction until it
// commits, the outer update cannot race another claimer: the only rows it can
// see under the ids are the ones this statement already locked.
const claimSQL = `UPDATE {{table}} AS t
SET {{state}} = '{{claimed}}', {{epoch}} = t.{{epoch}} + 1
WHERE t.{{id}} IN (
	SELECT d.{{id}}
	FROM {{table}} AS d
	WHERE d.{{state}} = '{{pending}}' AND d.{{next_due}} <= now()
	ORDER BY d.{{priority}}, d.{{next_due}}, d.{{id}}
	LIMIT $1
	FOR UPDATE SKIP LOCKED
)
RETURNING t.{{id}}, t.{{epoch}}`

// settleSQL marks the row done and retires the lease. The epoch bump makes a
// repeated Settle with the same epoch stale rather than a second win.
const settleSQL = `UPDATE {{table}}
SET {{state}} = 'done', {{epoch}} = {{epoch}} + 1
WHERE {{id}} = $1 AND {{epoch}} = $2 AND {{state}} = '{{claimed}}'`

// failSQL returns the row to pending with next_due pushed back by $3 seconds
// and retires the lease, so a repeated Fail with the same epoch is stale.
const failSQL = `UPDATE {{table}}
SET {{state}} = '{{pending}}', {{next_due}} = now() + make_interval(secs => $3), {{epoch}} = {{epoch}} + 1
WHERE {{id}} = $1 AND {{epoch}} = $2 AND {{state}} = '{{claimed}}'`
