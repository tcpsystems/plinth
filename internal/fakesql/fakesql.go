// Package fakesql is a scriptable database/sql driver for tests, so SQL-backed
// packages are exercised without a database or network.
package fakesql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
)

// Handler answers one statement. Control statements arrive as "BEGIN",
// "COMMIT" and "ROLLBACK". For queries return cols and rows; for execs return
// the affected row count.
type Handler func(query string, args []driver.Value) (cols []string, rows [][]driver.Value, affected int64, err error)

// Open returns a *sql.DB whose every statement goes through h.
func Open(h Handler) *sql.DB { return sql.OpenDB(connector{h}) }

// Register makes h available as the database/sql driver name, with a
// connector, so code that opens its pool with sql.Open or with a driver that
// opens connectors (leader.OpenSQL does) can be tested against h unmodified.
// The last call for a name wins, and pools opened before it keep the h they
// were registered with.
func Register(name string, h Handler) { sql.Register(name, driverAdaptor{h}) }

type driverAdaptor struct{ h Handler }

func (d driverAdaptor) Open(string) (driver.Conn, error) { return driver.Conn(conn(d)), nil }
func (d driverAdaptor) Connect(context.Context) (driver.Conn, error) {
	return driver.Conn(conn(d)), nil
}
func (d driverAdaptor) Driver() driver.Driver { return d }

func (d driverAdaptor) OpenConnector(string) (driver.Connector, error) { return d, nil }

type connector struct{ h Handler }

func (c connector) Connect(context.Context) (driver.Conn, error) { return conn(c), nil }
func (c connector) Driver() driver.Driver                        { return nil }

type conn struct{ h Handler }

func (c conn) Prepare(q string) (driver.Stmt, error) { return stmt{c.h, q}, nil }
func (c conn) Close() error                          { return nil }
func (c conn) Begin() (driver.Tx, error) {
	if _, _, _, err := c.h("BEGIN", nil); err != nil {
		return nil, err
	}
	return tx(c), nil
}

type tx struct{ h Handler }

func (t tx) Commit() error   { _, _, _, err := t.h("COMMIT", nil); return err }
func (t tx) Rollback() error { _, _, _, err := t.h("ROLLBACK", nil); return err }

type stmt struct {
	h Handler
	q string
}

func (s stmt) Close() error  { return nil }
func (s stmt) NumInput() int { return -1 }
func (s stmt) Exec(a []driver.Value) (driver.Result, error) {
	_, _, n, err := s.h(s.q, a)
	return driver.RowsAffected(n), err
}
func (s stmt) Query(a []driver.Value) (driver.Rows, error) {
	cols, rows, _, err := s.h(s.q, a)
	if err != nil {
		return nil, err
	}
	return &rowset{cols: cols, rows: rows}, nil
}

type rowset struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *rowset) Columns() []string { return r.cols }
func (r *rowset) Close() error      { return nil }
func (r *rowset) Next(dst []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dst, r.rows[r.i])
	r.i++
	return nil
}
