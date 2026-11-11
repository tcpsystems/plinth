// Package leader elects a single holder for a named background job using
// PostgreSQL session advisory locks.
//
// A session lock belongs to the database connection that acquired it, so
// Manager keeps its own connection (connection pinning): TryAcquire holds the
// lock on a dedicated connection leased from a small pool owned by the
// Manager, and Release gives the connection back. Other goroutines using the
// service's main pool are unaffected, and the lock never depends on which
// pooled connection the service happened to be using.
//
// If the process dies without releasing, the server drops the lock when the
// connection closes, so the next starter acquires it immediately; the lease
// is not waiting on the TTL in that case.
//
// The lease is a liveness signal, not a fencing token. A holder that stops
// renewing inside ttl should treat its work as at risk and stop; a job that
// must never overlap should call Renew before each unit of side-effecting
// work and stop when it returns false.
package leader

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

// Config controls lock keys and connection handling.
type Config struct {
	// Namespace separates lock keys from other keys drawn from the same
	// 64-bit space, such as the migration runner's fixed key. Keep it
	// constant for a deployment.
	Namespace int32
	// MaxConns bounds the Manager's own connection pool; each held lock
	// uses one connection. Zero means 4.
	MaxConns int
}

// Manager acquires and releases per-job locks on one database. It is safe
// for concurrent use.
type Manager struct {
	db     *sql.DB
	config Config

	mu     sync.Mutex
	held   map[string]*sql.Conn // connection holding each job's lock
	pinned *sql.DB              // Manager's pool, opened on first acquire
}

// New returns a Manager for the database behind db. Open db with OpenSQL so
// the Manager can open its pinned connections to the same database.
func New(db *sql.DB, config Config) *Manager {
	return &Manager{db: db, config: config, held: make(map[string]*sql.Conn)}
}

// TryAcquire takes the lock for jobName for at most ttl and reports whether
// this caller is now the leader. It does not wait: a lock held elsewhere
// returns false with a nil error, and false is also returned when a goroutine
// in this process holds the job.
//
// ttl must be positive. The lock is held until Release, the connection
// closes, or the session ends; Renew extends it.
func (m *Manager) TryAcquire(ctx context.Context, jobName string, ttl time.Duration) (bool, error) {
	if jobName == "" {
		return false, errors.New("leader: job name is empty")
	}
	if ttl <= 0 {
		return false, errors.New("leader: ttl must be positive")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.held[jobName]; ok {
		return false, nil
	}
	conn, err := m.conn(ctx)
	if err != nil {
		return false, err
	}
	classid, objid := m.config.lockKey(jobName)
	var acquired bool
	err = conn.QueryRowContext(ctx, tryLockSQL, classid, objid).Scan(&acquired)
	if err != nil {
		return false, errors.Join(fmt.Errorf("leader: try acquire %q: %w", jobName, err), conn.Close())
	}
	if !acquired {
		if err := conn.Close(); err != nil {
			return false, fmt.Errorf("leader: release connection: %w", err)
		}
		return false, nil
	}
	m.held[jobName] = conn
	return true, nil
}

// Release gives up the lock for jobName and returns its connection. It
// reports false when this Manager did not hold the lock. A failed unlock
// still returns the connection; closing it also releases the lock.
func (m *Manager) Release(ctx context.Context, jobName string) (bool, error) {
	m.mu.Lock()
	conn, ok := m.held[jobName]
	delete(m.held, jobName)
	m.mu.Unlock()
	if !ok {
		return false, nil
	}
	classid, objid := m.config.lockKey(jobName)
	if _, err := conn.ExecContext(ctx, unlockSQL, classid, objid); err != nil {
		return true, errors.Join(fmt.Errorf("leader: release %q: %w", jobName, err), conn.Close())
	}
	if err := conn.Close(); err != nil {
		return true, fmt.Errorf("leader: release connection: %w", err)
	}
	return true, nil
}

// Renew reports whether this Manager still holds jobName. Run it well
// inside ttl and stop the job when it returns false.
//
// A connection that has died means the session ended and the server has
// dropped the lock, so Renew reports false rather than an error.
func (m *Manager) Renew(ctx context.Context, jobName string) (bool, error) {
	m.mu.Lock()
	conn, ok := m.held[jobName]
	m.mu.Unlock()
	if !ok {
		return false, nil
	}
	classid, objid := m.config.lockKey(jobName)
	var held bool
	err := conn.QueryRowContext(ctx, renewSQL, uint32(classid), uint32(objid)).Scan(&held)
	if err != nil {
		if ctx.Err() != nil {
			return false, fmt.Errorf("leader: renew %q: %w", jobName, err)
		}
		m.forget(jobName, conn)
		return false, nil
	}
	return held, nil
}

// forget drops a dead connection from the held set.
func (m *Manager) forget(jobName string, conn *sql.Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.held[jobName] == conn {
		delete(m.held, jobName)
	}
	_ = conn.Close()
}

// conn returns a connection from the Manager's pool, opening the pool on
// first use. Pooled connections never expire and are never closed for
// idleness, so nothing reaps a connection that still holds a lock.
func (m *Manager) conn(ctx context.Context) (*sql.Conn, error) {
	if m.pinned == nil {
		pool, err := openPinned(m.db, m.config)
		if err != nil {
			return nil, err
		}
		m.pinned = pool
	}
	return m.pinned.Conn(ctx)
}

// Close releases every lock the Manager holds and closes its pool. The next
// TryAcquire opens a new pool.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for job, conn := range m.held {
		errs = append(errs, conn.Close())
		delete(m.held, job)
	}
	if m.pinned != nil {
		errs = append(errs, m.pinned.Close())
		m.pinned = nil
	}
	return errors.Join(errs...)
}

// Locks use the two-int advisory lock form, which keeps the namespace in the
// top bit of the job key: lockKey splits Key into (classid, objid) and puts
// the namespace in classid's high 31 bits and the key's top bit in its sign.
const (
	tryLockSQL = "SELECT pg_try_advisory_lock($1::int4, $2::int4)"
	unlockSQL  = "SELECT pg_advisory_unlock($1::int4, $2::int4)"
	// pg_locks shows both ints as unsigned oid values.
	renewSQL = `SELECT count(*) = 1 FROM pg_locks
		WHERE locktype = 'advisory' AND pid = pg_backend_pid() AND granted
		AND classid::bigint = $1::bigint AND objid::bigint = $2::bigint
		AND objsubid = 2`
)

// lockKey splits the namespace and job key into the two advisory lock ints.
func (c Config) lockKey(jobName string) (classid, objid int32) {
	key := uint64(Key(jobName))
	classid = int32(key>>33&(1<<30-1) | uint64(c.Namespace&0x7fffffff)<<31)
	objid = int32(key>>1 | key<<31)
	return classid, objid
}

// Key hashes a job name to a 64-bit advisory lock key. It is stable across
// versions, so two services agree on a job's lock. Distinct jobs get distinct
// keys in practice, and a collision only makes two jobs exclude each other.
func Key(jobName string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(jobName))
	return int64(h.Sum64())
}
