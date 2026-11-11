// Package quota implements single-statement atomic PostgreSQL quota admission
// backed by the quota_ledger and quota_cap tables.
//
// Admission executes an atomic upsert:
//
//	INSERT INTO quota_ledger AS l (account_id, concern, window_start, units)
//	VALUES ($1, $2, $3, $4)
//	ON CONFLICT (account_id, concern, window_start) DO UPDATE
//	SET units = l.units + excluded.units
//	WHERE l.units + excluded.units <= $5
//	RETURNING units
//
// Because the check and the spend happen inside one statement, concurrent
// service instances cannot overspend a shared cap. When a cap is exhausted,
// the statement affects zero rows and returns sql.ErrNoRows, which [Ledger]
// translates into Decision{Allowed: false}.
package quota

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// DefaultWindow is the default quota window duration when none is configured.
const DefaultWindow = 24 * time.Hour

// Schema is the idempotent PostgreSQL DDL for quota_cap and quota_ledger.
const Schema = `
CREATE TABLE IF NOT EXISTS quota_cap (
	account_id TEXT NOT NULL,
	concern    TEXT NOT NULL,
	window_s   INTEGER NOT NULL DEFAULT 86400,
	cap        BIGINT NOT NULL,
	reason     TEXT NOT NULL DEFAULT '',
	PRIMARY KEY (account_id, concern)
);
CREATE TABLE IF NOT EXISTS quota_ledger (
	account_id   TEXT NOT NULL,
	concern      TEXT NOT NULL,
	window_start TIMESTAMPTZ NOT NULL,
	units        BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (account_id, concern, window_start)
);
`

// AdmitSQL is the single-statement atomic quota admission query with an explicit cap.
const AdmitSQL = `INSERT INTO quota_ledger AS l (account_id, concern, window_start, units) VALUES ($1, $2, $3, $4) ON CONFLICT (account_id, concern, window_start) DO UPDATE SET units = l.units + excluded.units WHERE l.units + excluded.units <= $5 RETURNING units`

// AdmitCapTableSQL is the single-statement atomic quota admission query backed by quota_cap.
const AdmitCapTableSQL = `INSERT INTO quota_ledger AS l (account_id, concern, window_start, units) VALUES ($1, $2, $3, $4) ON CONFLICT (account_id, concern, window_start) DO UPDATE SET units = l.units + excluded.units WHERE l.units + excluded.units <= (SELECT cap FROM quota_cap WHERE account_id = $1 AND concern = $2) RETURNING units`

// Sentinel errors returned by quota operations.
var (
	// ErrExceeded is returned when an operation requires a hard error on cap exhaustion.
	ErrExceeded = errors.New("quota: cap exceeded")
	// ErrInvalid is returned when admission parameters are missing or negative.
	ErrInvalid = errors.New("quota: invalid admission arguments")
	// ErrCapNotFound is returned when no quota_cap row exists for an account and concern.
	ErrCapNotFound = errors.New("quota: cap not found")
)

// Cap defines the quota limit for an account and concern over a fixed window.
type Cap struct {
	AccountID     string
	Concern       string
	WindowSeconds int
	Limit         int64
	Reason        string
}

// Decision reports the outcome of a quota admission check.
type Decision struct {
	Allowed     bool
	Used        int64
	Cap         int64
	Remaining   int64
	WindowStart time.Time
	Exempt      bool
}

// Ledger enforces shared quotas against a PostgreSQL quota_ledger table.
type Ledger struct {
	// DB is the database connection pool.
	DB *sql.DB
	// Window is the default window duration used by Admit (defaults to DefaultWindow).
	Window time.Duration
	// Exempt reports whether an accountID is exempt from quota caps.
	Exempt func(accountID string) bool
	// Now returns the current time (defaults to time.Now).
	Now func() time.Time
}

// NewLedger creates a Ledger backed by db with DefaultWindow.
func NewLedger(db *sql.DB) *Ledger {
	return &Ledger{
		DB:     db,
		Window: DefaultWindow,
		Now:    time.Now,
	}
}

func (l *Ledger) now() time.Time {
	if l.Now != nil {
		return l.Now()
	}
	return time.Now()
}

func (l *Ledger) window() time.Duration {
	if l.Window > 0 {
		return l.Window
	}
	return DefaultWindow
}

// WindowStart truncates t in UTC to the start of the fixed window of duration w.
// If w is non-positive, DefaultWindow is used.
func WindowStart(t time.Time, w time.Duration) time.Time {
	if w <= 0 {
		w = DefaultWindow
	}
	return t.UTC().Truncate(w)
}

// Migrate applies the quota_cap and quota_ledger schema to the database.
func (l *Ledger) Migrate(ctx context.Context) error {
	if l == nil || l.DB == nil {
		return errors.New("quota: nil database")
	}
	_, err := l.DB.ExecContext(ctx, Schema)
	if err != nil {
		return fmt.Errorf("quota: migrate: %w", err)
	}
	return nil
}

// Admit checks and spends units against cap in the current window using a
// single atomic SQL statement.
func (l *Ledger) Admit(ctx context.Context, accountID, concern string, units, capLimit int64) (Decision, error) {
	ws := WindowStart(l.now(), l.window())
	return l.AdmitWindow(ctx, accountID, concern, ws, units, capLimit)
}

// AdmitWindow checks and spends units against capLimit in the window starting at
// windowStart using a single atomic SQL statement.
func (l *Ledger) AdmitWindow(ctx context.Context, accountID, concern string, windowStart time.Time, units, capLimit int64) (Decision, error) {
	if l == nil || l.DB == nil {
		return Decision{}, errors.New("quota: nil database")
	}
	if accountID == "" || concern == "" || units <= 0 || capLimit < 0 {
		return Decision{}, ErrInvalid
	}
	ws := windowStart.UTC()
	if l.Exempt != nil && l.Exempt(accountID) {
		return Decision{
			Allowed:     true,
			Used:        0,
			Cap:         capLimit,
			Remaining:   capLimit,
			WindowStart: ws,
			Exempt:      true,
		}, nil
	}
	if units > capLimit {
		return Decision{
			Allowed:     false,
			Used:        0,
			Cap:         capLimit,
			Remaining:   0,
			WindowStart: ws,
		}, nil
	}
	var used int64
	err := l.DB.QueryRowContext(ctx, AdmitSQL, accountID, concern, ws, units, capLimit).Scan(&used)
	if errors.Is(err, sql.ErrNoRows) {
		return Decision{
			Allowed:     false,
			Used:        capLimit,
			Cap:         capLimit,
			Remaining:   0,
			WindowStart: ws,
		}, nil
	}
	if err != nil {
		return Decision{}, fmt.Errorf("quota: admit: %w", err)
	}
	rem := capLimit - used
	if rem < 0 {
		rem = 0
	}
	return Decision{
		Allowed:     true,
		Used:        used,
		Cap:         capLimit,
		Remaining:   rem,
		WindowStart: ws,
	}, nil
}

// AdmitFromCap checks and spends units against the cap stored in quota_cap for
// accountID and concern in a single atomic SQL statement.
func (l *Ledger) AdmitFromCap(ctx context.Context, accountID, concern string, windowStart time.Time, units int64) (int64, bool, error) {
	if l == nil || l.DB == nil {
		return 0, false, errors.New("quota: nil database")
	}
	if accountID == "" || concern == "" || units <= 0 {
		return 0, false, ErrInvalid
	}
	ws := windowStart.UTC()
	if l.Exempt != nil && l.Exempt(accountID) {
		return 0, true, nil
	}
	var used int64
	err := l.DB.QueryRowContext(ctx, AdmitCapTableSQL, accountID, concern, ws, units).Scan(&used)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("quota: admit from cap: %w", err)
	}
	return used, true, nil
}

// SetCap upserts a Cap row in quota_cap.
func (l *Ledger) SetCap(ctx context.Context, c Cap) error {
	if l == nil || l.DB == nil {
		return errors.New("quota: nil database")
	}
	if c.AccountID == "" || c.Concern == "" || c.Limit < 0 {
		return ErrInvalid
	}
	winSec := c.WindowSeconds
	if winSec <= 0 {
		winSec = int(l.window() / time.Second)
	}
	_, err := l.DB.ExecContext(ctx,
		`INSERT INTO quota_cap (account_id, concern, window_s, cap, reason) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (account_id, concern) DO UPDATE SET window_s = excluded.window_s, cap = excluded.cap, reason = excluded.reason`,
		c.AccountID, c.Concern, winSec, c.Limit, c.Reason)
	if err != nil {
		return fmt.Errorf("quota: set cap: %w", err)
	}
	return nil
}

// GetCap loads the Cap row for accountID and concern from quota_cap.
func (l *Ledger) GetCap(ctx context.Context, accountID, concern string) (Cap, error) {
	if l == nil || l.DB == nil {
		return Cap{}, errors.New("quota: nil database")
	}
	c := Cap{AccountID: accountID, Concern: concern}
	err := l.DB.QueryRowContext(ctx,
		`SELECT window_s, cap, reason FROM quota_cap WHERE account_id = $1 AND concern = $2`,
		accountID, concern).Scan(&c.WindowSeconds, &c.Limit, &c.Reason)
	if errors.Is(err, sql.ErrNoRows) {
		return Cap{}, ErrCapNotFound
	}
	if err != nil {
		return Cap{}, fmt.Errorf("quota: get cap: %w", err)
	}
	return c, nil
}

// Usage returns the current spent units for accountID and concern in windowStart,
// or 0 if no ledger row exists yet.
func (l *Ledger) Usage(ctx context.Context, accountID, concern string, windowStart time.Time) (int64, error) {
	if l == nil || l.DB == nil {
		return 0, errors.New("quota: nil database")
	}
	var used int64
	err := l.DB.QueryRowContext(ctx,
		`SELECT units FROM quota_ledger WHERE account_id = $1 AND concern = $2 AND window_start = $3`,
		accountID, concern, windowStart.UTC()).Scan(&used)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("quota: usage: %w", err)
	}
	return used, nil
}

// PruneBefore deletes quota_ledger rows older than cutoff and returns the
// number of deleted rows.
func (l *Ledger) PruneBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	if l == nil || l.DB == nil {
		return 0, errors.New("quota: nil database")
	}
	res, err := l.DB.ExecContext(ctx,
		`DELETE FROM quota_ledger WHERE window_start < $1`,
		cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("quota: prune: %w", err)
	}
	return res.RowsAffected()
}
