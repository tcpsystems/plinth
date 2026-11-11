package leader

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"sync"
)

// A session advisory lock lives on one connection, and database/sql gives no
// promise that a *sql.DB hands the same connection to two calls. The Manager
// therefore keeps its own small pool. database/sql has no portable way to
// read a pool's DSN, and drivers such as pgx need it to reach the same
// database, so the Manager asks for the connector its pool's DSN builds:
// sources remembers, for each pool opened through OpenSQL, that connector.
var sources struct {
	sync.Mutex
	byPool map[*sql.DB]driver.Connector
}

func init() { sources.byPool = make(map[*sql.DB]driver.Connector) }

// OpenSQL opens driverName with dsn like sql.Open and remembers the driver's
// connector so New can open pinned connections to the same database. Use it
// for the pool leader locks are taken on; New still accepts any pool, but
// can only pin connections for a pool opened here.
func OpenSQL(driverName, dsn string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}
	dc, ok := db.Driver().(driver.DriverContext)
	if !ok {
		return db, nil
	}
	connector, err := dc.OpenConnector(dsn)
	if err != nil {
		return db, nil
	}
	sources.Lock()
	sources.byPool[db] = connector
	sources.Unlock()
	return db, nil
}

var errNoPinnedSource = errors.New("leader: open the pool with leader.OpenSQL so lock connections reach the same database")

// openPinned opens a pool over the same database as db. Pooled connections
// never expire and are never closed for idleness, so nothing reaps a
// connection that still holds a lock.
func openPinned(db *sql.DB, config Config) (*sql.DB, error) {
	sources.Lock()
	connector, ok := sources.byPool[db]
	sources.Unlock()
	if !ok {
		return nil, errNoPinnedSource
	}
	max := config.MaxConns
	if max <= 0 {
		max = 4
	}
	pool := sql.OpenDB(connector)
	pool.SetMaxOpenConns(max)
	pool.SetMaxIdleConns(max)
	pool.SetConnMaxIdleTime(0)
	pool.SetConnMaxLifetime(0)
	return pool, nil
}
