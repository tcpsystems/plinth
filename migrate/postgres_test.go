package migrate_test

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/tcpsystems/plinth/migrate"
)

// The tests in this file run against the disposable PostgreSQL started by
// tools/testdb.sh, which exports KEEL_TEST_DSN. They are skipped when the
// variable is absent so a plain `go test ./...` still works. The root module
// carries no third-party dependencies, so the few wire-protocol pieces these
// tests need (SCRAM-SHA-256 and the query protocol) live here.

func TestPostgresUpAndRollback(t *testing.T) {
	db := testDB(t)
	schema := newSchema(t, db)
	ms := loadPair(t, schema)
	opts := migrate.Options{Schema: schema, Table: "ledger"}

	got, err := migrate.Up(t.Context(), db, ms, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("applied %v", got)
	}
	if v, err := migrate.Version(t.Context(), db, opts); err != nil || v != 2 {
		t.Fatalf("version %d %v", v, err)
	}

	got, err = migrate.Rollback(t.Context(), db, ms, 0, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("reverted %v", got)
	}
	if v, err := migrate.Version(t.Context(), db, opts); err != nil || v != 0 {
		t.Fatalf("version after rollback %d %v", v, err)
	}
	if tableExists(t, db, schema, "a") || tableExists(t, db, schema, "b") {
		t.Fatal("rollback left a migration table behind")
	}
	if tableExists(t, db, "public", "schema_migrations") {
		t.Fatal("ledger ignored Options.Schema")
	}
}

func TestPostgresSchemaQualifiedLedger(t *testing.T) {
	db := testDB(t)
	schema := newSchema(t, db)
	ms := loadPair(t, schema)
	opts := migrate.Options{Schema: schema, Table: "applied"}

	if _, err := migrate.Up(t.Context(), db, ms, opts); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM "`+schema+`"."applied"`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("ledger rows = %d", n)
	}
	if tableExists(t, db, schema, "schema_migrations") {
		t.Fatal("created schema_migrations despite Options.Table")
	}
}

func TestPostgresRollbackRefusesDrift(t *testing.T) {
	db := testDB(t)
	schema := newSchema(t, db)
	ms := loadPair(t, schema)
	opts := migrate.Options{Schema: schema}

	if _, err := migrate.Up(t.Context(), db, ms, opts); err != nil {
		t.Fatal(err)
	}
	edited := append([]migrate.Migration(nil), ms...)
	edited[1].SQL += "\n-- edited after apply"

	if _, err := migrate.Rollback(t.Context(), db, edited, 0, opts); err == nil || !strings.Contains(err.Error(), "edited") {
		t.Fatalf("want drift error, got %v", err)
	}
	if !tableExists(t, db, schema, "b") {
		t.Fatal("rollback ran despite drift")
	}
}

func TestPostgresRollbackIsOneTransaction(t *testing.T) {
	db := testDB(t)
	schema := newSchema(t, db)
	fsys := fstest.MapFS{
		"sql/001_a.sql":      {Data: []byte(`CREATE TABLE "` + schema + `".a (id int);`)},
		"sql/001_a.down.sql": {Data: []byte(`DROP TABLE "` + schema + `".a;`)},
		"sql/002_b.sql":      {Data: []byte(`CREATE TABLE "` + schema + `".b (id int);`)},
		"sql/002_b.down.sql": {Data: []byte(`DROP TABLE "` + schema + `".b; SELECT 1/0;`)},
	}
	ms, err := migrate.Load(fsys, "sql")
	if err != nil {
		t.Fatal(err)
	}
	opts := migrate.Options{Schema: schema}
	if _, err := migrate.Up(t.Context(), db, ms, opts); err != nil {
		t.Fatal(err)
	}

	if _, err := migrate.Rollback(t.Context(), db, ms, 0, opts); err == nil {
		t.Fatal("want the failing down statement to fail the rollback")
	}
	if !tableExists(t, db, schema, "a") || !tableExists(t, db, schema, "b") {
		t.Fatal("rollback was not atomic: a committed down survived")
	}
	if v, err := migrate.Version(t.Context(), db, opts); err != nil || v != 2 {
		t.Fatalf("ledger changed by a failed rollback: version %d %v", v, err)
	}
}

func TestPostgresLockKeyOverride(t *testing.T) {
	db := testDB(t)
	schema := newSchema(t, db)
	ms := loadPair(t, schema)
	const key int64 = 42424242
	opts := migrate.Options{Schema: schema, LockKey: key}

	locker, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Close() }()
	if _, err := locker.ExecContext(t.Context(), "SELECT pg_advisory_lock($1)", key); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := migrate.Up(t.Context(), db, ms, opts)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Up did not wait on the configured lock key: %v", err)
	case <-time.After(300 * time.Millisecond):
	}

	if _, err := locker.ExecContext(t.Context(), "SELECT pg_advisory_unlock($1)", key); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Up stayed blocked after the lock was released")
	}
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("KEEL_TEST_DSN")
	if dsn == "" {
		t.Skip("KEEL_TEST_DSN is not set; run tools/testdb.sh")
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("KEEL_TEST_DSN: %v", err)
	}
	if ssl := u.Query().Get("sslmode"); ssl != "" && ssl != "disable" {
		t.Fatalf("KEEL_TEST_DSN must use sslmode=disable, got %q", ssl)
	}
	port := u.Port()
	if port == "" {
		port = "5432"
	}
	pw, _ := u.User.Password()
	cfg := pgConfig{
		addr: net.JoinHostPort(u.Hostname(), port),
		user: u.User.Username(),
		pass: pw,
		db:   strings.TrimPrefix(u.Path, "/"),
	}
	db := sql.OpenDB(pgConnector{cfg: cfg})
	t.Cleanup(func() { _ = db.Close() })
	if err := db.PingContext(t.Context()); err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	return db
}

func newSchema(t *testing.T, db *sql.DB) string {
	t.Helper()
	name := "mig_" + strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		}
		return '_'
	}, t.Name())
	if len(name) > 60 {
		name = name[:60]
	}
	if _, err := db.ExecContext(t.Context(), `CREATE SCHEMA "`+name+`"`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = db.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+name+`" CASCADE`)
	})
	return name
}

// loadPair returns two migrations whose up and down files act inside schema.
func loadPair(t *testing.T, schema string) []migrate.Migration {
	t.Helper()
	ms, err := migrate.Load(fstest.MapFS{
		"sql/001_a.sql":      {Data: []byte(`CREATE TABLE "` + schema + `".a (id int);`)},
		"sql/001_a.down.sql": {Data: []byte(`DROP TABLE "` + schema + `".a;`)},
		"sql/002_b.sql":      {Data: []byte(`CREATE TABLE "` + schema + `".b (id int);`)},
		"sql/002_b.down.sql": {Data: []byte(`DROP TABLE "` + schema + `".b;`)},
	}, "sql")
	if err != nil {
		t.Fatal(err)
	}
	return ms
}

func tableExists(t *testing.T, db *sql.DB, schema, table string) bool {
	t.Helper()
	var n int
	err := db.QueryRowContext(t.Context(),
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`,
		schema, table).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n > 0
}

// Test-only PostgreSQL wire client ------------------------------------------

type pgConfig struct {
	addr string
	user string
	pass string
	db   string
}

type pgConnector struct{ cfg pgConfig }

func (p pgConnector) Connect(ctx context.Context) (driver.Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", p.cfg.addr)
	if err != nil {
		return nil, err
	}
	c := &pgConn{nc: nc, cfg: p.cfg}
	if err := c.startup(); err != nil {
		_ = nc.Close()
		return nil, err
	}
	return c, nil
}

func (p pgConnector) Driver() driver.Driver { return nil }

type pgConn struct {
	nc     net.Conn
	cfg    pgConfig
	broken bool
}

var (
	_ driver.Conn           = (*pgConn)(nil)
	_ driver.ExecerContext  = (*pgConn)(nil)
	_ driver.QueryerContext = (*pgConn)(nil)
	_ driver.ConnBeginTx    = (*pgConn)(nil)
	_ driver.Pinger         = (*pgConn)(nil)
)

func (c *pgConn) startup() error {
	body := binary.BigEndian.AppendUint32(nil, 196608)
	for _, kv := range [][2]string{
		{"user", c.cfg.user},
		{"database", c.cfg.db},
		{"application_name", "plinth-migrate-test"},
	} {
		if kv[1] == "" {
			continue
		}
		body = append(body, kv[0]...)
		body = append(body, 0)
		body = append(body, kv[1]...)
		body = append(body, 0)
	}
	body = append(body, 0)
	msg := binary.BigEndian.AppendUint32(nil, uint32(4+len(body)))
	msg = append(msg, body...)
	if _, err := c.nc.Write(msg); err != nil {
		return err
	}
	for {
		typ, payload, err := c.readMessage()
		if err != nil {
			return err
		}
		switch typ {
		case 'R':
			if len(payload) < 4 {
				return errors.New("pgtest: short Authentication message")
			}
			switch binary.BigEndian.Uint32(payload[:4]) {
			case 0:
			case 3:
				if _, err := c.nc.Write(append([]byte{'p'}, append([]byte(c.cfg.pass), 0)...)); err != nil {
					return err
				}
			case 10:
				if err := c.scram(); err != nil {
					return err
				}
			default:
				return fmt.Errorf("pgtest: unsupported authentication method %d", binary.BigEndian.Uint32(payload[:4]))
			}
		case 'S', 'K', 'N':
		case 'Z':
			return nil
		case 'E':
			return pgError(payload)
		default:
			return fmt.Errorf("pgtest: unexpected startup message %q", typ)
		}
	}
}

func (c *pgConn) scram() error {
	nonce := make([]byte, 18)
	if _, err := rand.Read(nonce); err != nil {
		return err
	}
	clientNonce := base64.StdEncoding.EncodeToString(nonce)
	firstBare := "n=,r=" + clientNonce
	first := "n,," + firstBare
	body := append([]byte("SCRAM-SHA-256\x00"), binary.BigEndian.AppendUint32(nil, uint32(len(first)))...)
	body = append(body, first...)
	if err := c.send('p', body); err != nil {
		return err
	}
	typ, payload, err := c.readMessage()
	if err != nil {
		return err
	}
	if typ == 'E' {
		return pgError(payload)
	}
	if typ != 'R' || len(payload) < 4 || binary.BigEndian.Uint32(payload[:4]) != 11 {
		return fmt.Errorf("pgtest: want AuthenticationSASLContinue, got %q", typ)
	}
	serverFirst := string(payload[4:])
	attrs := scramAttrs(serverFirst)
	combined := attrs["r"]
	if !strings.HasPrefix(combined, clientNonce) {
		return errors.New("pgtest: bad server nonce")
	}
	iter, err := strconv.Atoi(attrs["i"])
	if err != nil || iter < 1 {
		return fmt.Errorf("pgtest: bad iteration count %q", attrs["i"])
	}
	salt, err := base64.StdEncoding.DecodeString(attrs["s"])
	if err != nil {
		return err
	}
	salted := hi(c.cfg.pass, salt, iter)
	clientKey := hmac256(salted, []byte("Client Key"))
	storedKey := sha256.Sum256(clientKey)
	finalNoProof := "c=biws,r=" + combined
	authMessage := firstBare + "," + serverFirst + "," + finalNoProof
	proof := xorBytes(clientKey, hmac256(storedKey[:], []byte(authMessage)))
	serverSig := hmac256(hmac256(salted, []byte("Server Key")), []byte(authMessage))
	final := finalNoProof + ",p=" + base64.StdEncoding.EncodeToString(proof)
	if err := c.send('p', []byte(final)); err != nil {
		return err
	}
	typ, payload, err = c.readMessage()
	if err != nil {
		return err
	}
	if typ == 'E' {
		return pgError(payload)
	}
	if typ != 'R' || len(payload) < 4 || binary.BigEndian.Uint32(payload[:4]) != 12 {
		return fmt.Errorf("pgtest: want AuthenticationSASLFinal, got %q", typ)
	}
	sig, err := base64.StdEncoding.DecodeString(scramAttrs(string(payload[4:]))["v"])
	if err != nil || !hmac.Equal(sig, serverSig) {
		return errors.New("pgtest: server signature mismatch")
	}
	return nil
}

func (c *pgConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("pgtest: prepared statements are not supported")
}

func (c *pgConn) Close() error {
	if c.nc == nil {
		return nil
	}
	err := c.nc.Close()
	c.nc = nil
	return err
}

func (c *pgConn) Ping(ctx context.Context) error {
	_, _, _, err := c.roundTrip(ctx, "SELECT 1", nil)
	return err
}

func (c *pgConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *pgConn) BeginTx(ctx context.Context, _ driver.TxOptions) (driver.Tx, error) {
	if _, _, _, err := c.roundTrip(ctx, "BEGIN", nil); err != nil {
		return nil, err
	}
	return pgTx{c: c}, nil
}

func (c *pgConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	_, _, tag, err := c.roundTrip(ctx, query, args)
	if err != nil {
		return nil, err
	}
	var n int64
	if f := strings.Fields(tag); len(f) > 0 {
		n, _ = strconv.ParseInt(f[len(f)-1], 10, 64)
	}
	return driver.RowsAffected(n), nil
}

func (c *pgConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	cols, rows, _, err := c.roundTrip(ctx, query, args)
	if err != nil {
		return nil, err
	}
	return &pgRows{cols: cols, rows: rows}, nil
}

func (c *pgConn) roundTrip(ctx context.Context, query string, args []driver.NamedValue) ([]string, [][]driver.Value, string, error) {
	if c.broken || c.nc == nil {
		return nil, nil, "", errors.New("pgtest: connection is closed")
	}
	if d, ok := ctx.Deadline(); ok {
		_ = c.nc.SetDeadline(d)
		defer func() { _ = c.nc.SetDeadline(time.Time{}) }()
	}
	if len(args) > 0 {
		return c.extended(query, args)
	}
	if err := c.send('Q', append([]byte(query), 0)); err != nil {
		return nil, nil, "", err
	}
	var cols []string
	var oids []uint32
	var rows [][]driver.Value
	var tag string
	for {
		typ, payload, err := c.readMessage()
		if err != nil {
			return nil, nil, "", err
		}
		switch typ {
		case 'T':
			cols, oids, err = rowDescriptionOIDs(payload)
			if err != nil {
				return nil, nil, "", err
			}
		case 'D':
			row, err := dataRow(payload, oids)
			if err != nil {
				return nil, nil, "", err
			}
			rows = append(rows, row)
		case 'C':
			tag, _, _ = cstring(payload)
		case 'Z':
			return cols, rows, tag, nil
		case 'E':
			perr := pgError(payload)
			if _, _, derr := c.drain(); derr != nil {
				return nil, nil, "", perr
			}
			return nil, nil, "", perr
		case 'I', 'N', 'S', 'A':
		default:
			return nil, nil, "", fmt.Errorf("pgtest: unexpected message %q", typ)
		}
	}
}

func (c *pgConn) extended(query string, args []driver.NamedValue) ([]string, [][]driver.Value, string, error) {
	parse := []byte{0}
	parse = append(parse, query...)
	parse = append(parse, 0)
	parse = binary.BigEndian.AppendUint16(parse, 0)
	if err := c.send('P', parse); err != nil {
		return nil, nil, "", err
	}
	bind := []byte{0, 0}
	bind = binary.BigEndian.AppendUint16(bind, 0)
	bind = binary.BigEndian.AppendUint16(bind, uint16(len(args)))
	for _, a := range args {
		if a.Value == nil {
			bind = binary.BigEndian.AppendUint32(bind, 0xFFFFFFFF)
			continue
		}
		s, err := encodeArg(a.Value)
		if err != nil {
			return nil, nil, "", err
		}
		bind = binary.BigEndian.AppendUint32(bind, uint32(len(s)))
		bind = append(bind, s...)
	}
	bind = binary.BigEndian.AppendUint16(bind, 0)
	if err := c.send('B', bind); err != nil {
		return nil, nil, "", err
	}
	if err := c.send('D', []byte{'P', 0}); err != nil {
		return nil, nil, "", err
	}
	exec := binary.BigEndian.AppendUint32([]byte{0}, 0)
	if err := c.send('E', exec); err != nil {
		return nil, nil, "", err
	}
	if err := c.send('S', nil); err != nil {
		return nil, nil, "", err
	}
	var cols []string
	var oids []uint32
	var rows [][]driver.Value
	var tag string
	for {
		typ, payload, err := c.readMessage()
		if err != nil {
			return nil, nil, "", err
		}
		switch typ {
		case '1', '2', 'n':
		case 'T':
			cols, oids, err = rowDescriptionOIDs(payload)
			if err != nil {
				return nil, nil, "", err
			}
		case 'D':
			row, err := dataRow(payload, oids)
			if err != nil {
				return nil, nil, "", err
			}
			rows = append(rows, row)
		case 'C':
			tag, _, _ = cstring(payload)
		case 'Z':
			return cols, rows, tag, nil
		case 'E':
			perr := pgError(payload)
			if _, _, derr := c.drain(); derr != nil {
				return nil, nil, "", perr
			}
			return nil, nil, "", perr
		case 'N', 'S':
		default:
			return nil, nil, "", fmt.Errorf("pgtest: unexpected message %q", typ)
		}
	}
}

// drain consumes messages up to ReadyForQuery after an ErrorResponse.
func (c *pgConn) drain() ([]string, [][]driver.Value, error) {
	for {
		typ, _, err := c.readMessage()
		if err != nil {
			return nil, nil, err
		}
		if typ == 'Z' {
			return nil, nil, nil
		}
	}
}

func (c *pgConn) readMessage() (byte, []byte, error) {
	var head [5]byte
	if _, err := io.ReadFull(c.nc, head[:]); err != nil {
		c.broken = true
		return 0, nil, err
	}
	n := binary.BigEndian.Uint32(head[1:])
	if n < 4 || n > 1<<28 {
		c.broken = true
		return 0, nil, fmt.Errorf("pgtest: bad message length %d", n)
	}
	payload := make([]byte, n-4)
	if _, err := io.ReadFull(c.nc, payload); err != nil {
		c.broken = true
		return 0, nil, err
	}
	return head[0], payload, nil
}

func (c *pgConn) send(typ byte, payload []byte) error {
	msg := make([]byte, 5+len(payload))
	msg[0] = typ
	binary.BigEndian.PutUint32(msg[1:], uint32(4+len(payload)))
	copy(msg[5:], payload)
	if _, err := c.nc.Write(msg); err != nil {
		c.broken = true
		return err
	}
	return nil
}

type pgTx struct{ c *pgConn }

func (t pgTx) Commit() error   { return t.end("COMMIT") }
func (t pgTx) Rollback() error { return t.end("ROLLBACK") }

func (t pgTx) end(q string) error {
	_, _, _, err := t.c.roundTrip(context.Background(), q, nil)
	return err
}

type pgRows struct {
	cols []string
	rows [][]driver.Value
	i    int
}

func (r *pgRows) Columns() []string { return r.cols }
func (r *pgRows) Close() error      { return nil }
func (r *pgRows) Next(dst []driver.Value) error {
	if r.i >= len(r.rows) {
		return io.EOF
	}
	copy(dst, r.rows[r.i])
	r.i++
	return nil
}

func rowDescriptionOIDs(payload []byte) ([]string, []uint32, error) {
	if len(payload) < 2 {
		return nil, nil, errors.New("pgtest: short RowDescription")
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	payload = payload[2:]
	cols := make([]string, 0, n)
	oids := make([]uint32, 0, n)
	for i := 0; i < n; i++ {
		name, rest, ok := cstring(payload)
		if !ok || len(rest) < 18 {
			return nil, nil, errors.New("pgtest: truncated RowDescription")
		}
		oids = append(oids, binary.BigEndian.Uint32(rest[6:10]))
		cols = append(cols, name)
		payload = rest[18:]
	}
	return cols, oids, nil
}

func dataRow(payload []byte, oids []uint32) ([]driver.Value, error) {
	if len(payload) < 2 {
		return nil, errors.New("pgtest: short DataRow")
	}
	n := int(binary.BigEndian.Uint16(payload[:2]))
	payload = payload[2:]
	out := make([]driver.Value, n)
	for i := 0; i < n; i++ {
		if len(payload) < 4 {
			return nil, errors.New("pgtest: truncated DataRow")
		}
		l := int32(binary.BigEndian.Uint32(payload[:4]))
		payload = payload[4:]
		if l < 0 {
			out[i] = nil
			continue
		}
		if int(l) > len(payload) {
			return nil, errors.New("pgtest: truncated DataRow value")
		}
		text := string(payload[:l])
		payload = payload[l:]
		out[i] = decodeValue(text, oids, i)
	}
	return out, nil
}

func decodeValue(text string, oids []uint32, i int) driver.Value {
	if i >= len(oids) {
		return text
	}
	switch oids[i] {
	case 16:
		return text == "t"
	case 20, 21, 23, 26:
		n, err := strconv.ParseInt(text, 10, 64)
		if err == nil {
			return n
		}
	case 700, 701, 1700:
		f, err := strconv.ParseFloat(text, 64)
		if err == nil {
			return f
		}
	}
	return text
}

func encodeArg(v driver.Value) ([]byte, error) {
	switch x := v.(type) {
	case int64:
		return []byte(strconv.FormatInt(x, 10)), nil
	case float64:
		return []byte(strconv.FormatFloat(x, 'g', -1, 64)), nil
	case bool:
		if x {
			return []byte("true"), nil
		}
		return []byte("false"), nil
	case string:
		return []byte(x), nil
	case []byte:
		return x, nil
	case time.Time:
		return []byte(x.Format(time.RFC3339Nano)), nil
	default:
		return nil, fmt.Errorf("pgtest: unsupported argument type %T", v)
	}
}

type pgErr struct {
	severity string
	code     string
	message  string
}

func (e *pgErr) Error() string { return "pgtest: " + e.severity + " " + e.code + ": " + e.message }

func pgError(payload []byte) error {
	e := &pgErr{}
	for len(payload) > 0 {
		field := payload[0]
		payload = payload[1:]
		if field == 0 {
			break
		}
		value, rest, _ := cstring(payload)
		payload = rest
		switch field {
		case 'S', 'V':
			if e.severity == "" {
				e.severity = value
			}
		case 'C':
			e.code = value
		case 'M':
			e.message = value
		}
	}
	return e
}

func cstring(b []byte) (string, []byte, bool) {
	for i, c := range b {
		if c == 0 {
			return string(b[:i]), b[i+1:], true
		}
	}
	return "", b, false
}

func scramAttrs(s string) map[string]string {
	out := map[string]string{}
	for _, part := range strings.Split(s, ",") {
		if i := strings.Index(part, "="); i > 0 {
			out[part[:i]] = part[i+1:]
		}
	}
	return out
}

func hi(password string, salt []byte, iterations int) []byte {
	mac := hmac.New(sha256.New, []byte(password))
	mac.Write(salt)
	mac.Write([]byte{0, 0, 0, 1})
	u := mac.Sum(nil)
	out := append([]byte(nil), u...)
	for i := 1; i < iterations; i++ {
		mac = hmac.New(sha256.New, []byte(password))
		mac.Write(u)
		u = mac.Sum(nil)
		for j := range out {
			out[j] ^= u[j]
		}
	}
	return out
}

func hmac256(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}

func xorBytes(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}
