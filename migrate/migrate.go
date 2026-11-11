// Package migrate applies versioned SQL migrations to PostgreSQL through
// database/sql or a caller-supplied executor.
//
// Files are named NNN_name.sql (the version is the leading integer) and are
// embedded with embed.FS. An optional NNN_name.down.sql pairs with it; Rollback
// applies the down bodies newest first in one transaction. Rollback requires a
// down file for every migration it reverses; it is meant for tested reversals,
// not as a substitute for a database restore.
//
// Applied versions are recorded in a ledger table (schema_migrations in the
// current schema unless Options selects another) with a SHA-256 checksum of the
// up file. Editing an applied file is an error. A session advisory lock
// serialises concurrent starters (several replicas boot at once).
//
// Up, Version and Rollback accept Execer and Queryer values, so a caller can
// pass a database/sql pool or transaction directly, or a small adapter over
// another driver, without this module importing that driver.
package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	defaultLockKey = 727274 // arbitrary, shared by every tcpsystems service
	defaultTable   = "schema_migrations"
)

// identPattern is the PostgreSQL identifier subset this package accepts:
// an ASCII letter or underscore, then letters, digits, underscores and dollar
// signs, at most 63 bytes.
var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_$]{0,62}$`)

// Options selects where the migration ledger lives and which advisory lock
// serialises runners. The zero value keeps the historical behaviour: the
// schema_migrations table in the current schema and lock key 727274.
type Options struct {
	// Schema qualifies the ledger table. Empty means the current schema.
	Schema string
	// Table is the ledger table name. Empty means schema_migrations.
	Table string
	// LockKey is the PostgreSQL advisory lock key. Zero means 727274.
	LockKey int64
}

func resolve(opts []Options) (Options, error) {
	if len(opts) > 1 {
		return Options{}, errors.New("migrate: at most one Options value is accepted")
	}
	var o Options
	if len(opts) == 1 {
		o = opts[0]
	}
	if o.Table == "" {
		o.Table = defaultTable
	}
	if o.LockKey == 0 {
		o.LockKey = defaultLockKey
	}
	if o.Schema != "" && !identPattern.MatchString(o.Schema) {
		return Options{}, fmt.Errorf("migrate: schema %q is not a valid identifier", o.Schema)
	}
	if !identPattern.MatchString(o.Table) {
		return Options{}, fmt.Errorf("migrate: table %q is not a valid identifier", o.Table)
	}
	return o, nil
}

// table returns the quoted, optionally schema-qualified ledger name.
func (o Options) table() string {
	if o.Schema != "" {
		return quoteIdent(o.Schema) + "." + quoteIdent(o.Table)
	}
	return quoteIdent(o.Table)
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

// Execer executes one statement.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Queryer runs one query and returns its rows.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// DB is the executor pair Up and Rollback need. *sql.DB, *sql.Conn and
// *sql.Tx all satisfy it.
type DB interface {
	Execer
	Queryer
}

// Migration is one numbered SQL file plus its optional down file.
type Migration struct {
	Version int
	Name    string
	SQL     string
	// Down is the body of NNN_name.down.sql, or empty when the file is absent.
	Down string
}

// Checksum is the hex SHA-256 of the up SQL text.
func (m Migration) Checksum() string {
	s := sha256.Sum256([]byte(m.SQL))
	return hex.EncodeToString(s[:])
}

// Load reads NNN_name.sql files from dir in fsys, sorted by version, and pairs
// each with NNN_name.down.sql when that file is present. Duplicate versions and
// down files without a matching up file are errors; gaps are allowed.
// Options is validated here so a bad schema or table fails before any database
// connection is opened.
func Load(fsys fs.FS, dir string, opts ...Options) ([]Migration, error) {
	if _, err := resolve(opts); err != nil {
		return nil, err
	}
	ents, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range ents {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".sql") || strings.HasSuffix(n, ".down.sql") {
			continue
		}
		num, name, ok := strings.Cut(strings.TrimSuffix(n, ".sql"), "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil || v <= 0 {
			return nil, fmt.Errorf("migrate: %s: want NNN_name.sql", n)
		}
		b, err := fs.ReadFile(fsys, path.Join(dir, n))
		if err != nil {
			return nil, err
		}
		out = append(out, Migration{Version: v, Name: name, SQL: string(b)})
	}
	slices.SortFunc(out, func(a, b Migration) int { return a.Version - b.Version })
	index := make(map[int]int, len(out))
	for i := range out {
		if _, dup := index[out[i].Version]; dup {
			return nil, fmt.Errorf("migrate: duplicate version %d", out[i].Version)
		}
		index[out[i].Version] = i
	}
	for _, e := range ents {
		n := e.Name()
		if e.IsDir() || !strings.HasSuffix(n, ".down.sql") {
			continue
		}
		num, name, ok := strings.Cut(strings.TrimSuffix(n, ".down.sql"), "_")
		v, err := strconv.Atoi(num)
		if !ok || err != nil || v <= 0 {
			return nil, fmt.Errorf("migrate: %s: want NNN_name.down.sql", n)
		}
		i, ok := index[v]
		if !ok || out[i].Name != name {
			return nil, fmt.Errorf("migrate: %s: no NNN_name.sql matches this down file", n)
		}
		b, err := fs.ReadFile(fsys, path.Join(dir, n))
		if err != nil {
			return nil, err
		}
		out[i].Down = string(b)
	}
	return out, nil
}

// session pins an advisory-lock session. When the caller supplies a pool it
// takes one connection so the lock and every statement share it; a caller that
// supplies a transaction or a single connection is used directly.
type session struct {
	db   DB
	conn *sql.Conn
}

func openSession(ctx context.Context, db DB) (*session, error) {
	if c, ok := db.(interface {
		Conn(context.Context) (*sql.Conn, error)
	}); ok {
		conn, err := c.Conn(ctx)
		if err != nil {
			return nil, err
		}
		return &session{db: conn, conn: conn}, nil
	}
	return &session{db: db}, nil
}

func (s *session) close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *session) lock(ctx context.Context, key int64) error {
	_, err := s.db.ExecContext(ctx, "SELECT pg_advisory_lock($1)", key)
	return err
}

func (s *session) unlock(ctx context.Context, key int64) error {
	_, err := s.db.ExecContext(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", key)
	return err
}

// inTx runs fn in one transaction when the executor can start one. A caller
// that supplies an existing transaction keeps control of commit and rollback.
func (s *session) inTx(ctx context.Context, fn func(Execer) error) error {
	if b, ok := s.db.(interface {
		BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
	}); ok {
		tx, err := b.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			_ = tx.Rollback()
			return err
		}
		return tx.Commit()
	}
	return fn(s.db)
}

// applied reads the ledger into a version-to-checksum map.
func applied(ctx context.Context, q Queryer, table string) (map[int]string, error) {
	rows, err := q.QueryContext(ctx, "SELECT version, checksum FROM "+table)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	done := map[int]string{}
	for rows.Next() {
		var v int
		var sum string
		if err := rows.Scan(&v, &sum); err != nil {
			return nil, err
		}
		done[v] = sum
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return done, nil
}

// Up applies every migration not yet recorded and returns the versions it
// applied. Each migration runs in its own transaction when the executor is a
// pool or connection; an executor that is already a transaction is used as is.
func Up(ctx context.Context, db DB, ms []Migration, opts ...Options) (appliedVersions []int, err error) {
	o, err := resolve(opts)
	if err != nil {
		return nil, err
	}
	s, err := openSession(ctx, db)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := s.close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	if err = s.lock(ctx, o.LockKey); err != nil {
		return nil, err
	}
	defer func() {
		if uerr := s.unlock(ctx, o.LockKey); uerr != nil {
			err = errors.Join(err, uerr)
		}
	}()
	if _, err = s.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+o.table()+` (
		version int PRIMARY KEY, name text NOT NULL, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return nil, err
	}
	done, err := applied(ctx, s.db, o.table())
	if err != nil {
		return nil, err
	}
	for _, m := range ms {
		if sum, ok := done[m.Version]; ok {
			if sum != m.Checksum() {
				return appliedVersions, fmt.Errorf("migrate: version %d (%s) was edited after being applied", m.Version, m.Name)
			}
			continue
		}
		err := s.inTx(ctx, func(ex Execer) error {
			if _, err := ex.ExecContext(ctx, m.SQL); err != nil {
				return err
			}
			_, err := ex.ExecContext(ctx, "INSERT INTO "+o.table()+" (version, name, checksum) VALUES ($1, $2, $3)", m.Version, m.Name, m.Checksum())
			return err
		})
		if err != nil {
			return appliedVersions, fmt.Errorf("migrate: version %d (%s): %w", m.Version, m.Name, err)
		}
		appliedVersions = append(appliedVersions, m.Version)
	}
	return appliedVersions, nil
}

// Version returns the highest applied version, or 0 for a fresh database.
func Version(ctx context.Context, q Queryer, opts ...Options) (int, error) {
	o, err := resolve(opts)
	if err != nil {
		return 0, err
	}
	rows, err := q.QueryContext(ctx, "SELECT max(version) FROM "+o.table())
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	var v sql.NullInt64
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, err
		}
		return 0, nil
	}
	if err := rows.Scan(&v); err != nil {
		return 0, err
	}
	return int(v.Int64), rows.Err()
}

// Rollback applies the down file of every applied migration newer than target,
// newest first, in one transaction, and deletes their ledger rows. It refuses
// to run when an applied up file no longer matches its recorded checksum or
// when a migration that must be reversed has no down file. Target 0 reverts
// everything.
func Rollback(ctx context.Context, db DB, ms []Migration, target int, opts ...Options) (reverted []int, err error) {
	o, err := resolve(opts)
	if err != nil {
		return nil, err
	}
	if target < 0 {
		return nil, fmt.Errorf("migrate: target version %d is negative", target)
	}
	s, err := openSession(ctx, db)
	if err != nil {
		return nil, err
	}
	defer func() {
		if cerr := s.close(); cerr != nil {
			err = errors.Join(err, cerr)
		}
	}()
	if err = s.lock(ctx, o.LockKey); err != nil {
		return nil, err
	}
	defer func() {
		if uerr := s.unlock(ctx, o.LockKey); uerr != nil {
			err = errors.Join(err, uerr)
		}
	}()
	done, err := applied(ctx, s.db, o.table())
	if err != nil {
		return nil, err
	}
	byVersion := make(map[int]Migration, len(ms))
	for _, m := range ms {
		byVersion[m.Version] = m
	}
	plan := make([]Migration, 0, len(done))
	for v, sum := range done {
		if v <= target {
			continue
		}
		m, ok := byVersion[v]
		if !ok {
			return nil, fmt.Errorf("migrate: applied version %d has no loaded migration file", v)
		}
		if sum != m.Checksum() {
			return nil, fmt.Errorf("migrate: version %d (%s) was edited after being applied", m.Version, m.Name)
		}
		if strings.TrimSpace(m.Down) == "" {
			return nil, fmt.Errorf("migrate: version %d (%s) has no down file", m.Version, m.Name)
		}
		plan = append(plan, m)
	}
	slices.SortFunc(plan, func(a, b Migration) int { return b.Version - a.Version })
	if len(plan) == 0 {
		return nil, nil
	}
	err = s.inTx(ctx, func(ex Execer) error {
		for _, m := range plan {
			if _, err := ex.ExecContext(ctx, m.Down); err != nil {
				return fmt.Errorf("migrate: version %d (%s): %w", m.Version, m.Name, err)
			}
			if _, err := ex.ExecContext(ctx, "DELETE FROM "+o.table()+" WHERE version = $1", m.Version); err != nil {
				return fmt.Errorf("migrate: version %d (%s): %w", m.Version, m.Name, err)
			}
			reverted = append(reverted, m.Version)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return reverted, nil
}
