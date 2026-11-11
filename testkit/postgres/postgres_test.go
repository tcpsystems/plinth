package postgres

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
)

// OpenDSN connects the way a driver would: by parsing DB.DSN.
func OpenDSN(t *testing.T, dsn string) *pgConn {
	t.Helper()
	c, err := dialDSN(dsn)
	if err != nil {
		t.Fatalf("connect with DSN: %v", err)
	}
	t.Cleanup(c.close)
	return c
}

func dialDSN(dsn string) (*pgConn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "postgres" || strings.TrimPrefix(u.Path, "/") == "" {
		return nil, fmt.Errorf("DSN %q is not a postgres URL with a database", dsn)
	}
	addr := u.Host
	if u.Port() == "" {
		addr += ":5432"
	}
	pass, _ := u.User.Password()
	return connectSession(context.Background(), addr, u.User.Username(), pass, strings.TrimPrefix(u.Path, "/"))
}

// Query runs one statement through the DSN and returns its rows.
func Query(t *testing.T, dsn, sql string) [][]string {
	t.Helper()
	raw, err := OpenDSN(t, dsn).run(context.Background(), sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := make([][]string, len(raw))
	for i, line := range raw {
		out[i] = parseRow(line)
	}
	return out
}

// One returns the single cell of a single-row, single-column query.
func One(t *testing.T, dsn, sql string) string {
	t.Helper()
	rows := Query(t, dsn, sql)
	if len(rows) != 1 || len(rows[0]) != 1 {
		t.Fatalf("%s: got %v, want one value", sql, rows)
	}
	return rows[0][0]
}

// Count returns how many databases are named datname.
func Count(t *testing.T, dsn, datname string) string {
	return One(t, dsn, "SELECT count(*) FROM pg_database WHERE datname = "+quoteLiteral(datname))
}

func TestStartReturnsWorkingDSN(t *testing.T) {
	db := Start(t)
	u, err := url.Parse(db.DSN)
	if err != nil {
		t.Fatalf("parse %q: %v", db.DSN, err)
	}
	if got := strings.TrimPrefix(u.Path, "/"); got != db.Name {
		t.Errorf("DSN database %q, want %q", got, db.Name)
	}
	if !strings.HasPrefix(db.Name, "test_teststartreturnsworkingdsn") {
		t.Errorf("database name %q does not carry the test name", db.Name)
	}
	if got := One(t, db.DSN, "SELECT current_database()"); got != db.Name {
		t.Errorf("current_database() = %q, want %q", got, db.Name)
	}
	Query(t, db.DSN, `CREATE TABLE items (id int PRIMARY KEY, name text)`)
	Query(t, db.DSN, `INSERT INTO items VALUES (1, 'one'), (2, NULL)`)
	rows := Query(t, db.DSN, `SELECT id, name FROM items ORDER BY id`)
	if len(rows) != 2 || rows[0][0] != "1" || rows[0][1] != "one" || rows[1][1] != `\N` {
		t.Errorf("round trip: %v", rows)
	}
}

func TestStartCreatesANewDatabasePerTest(t *testing.T) {
	for _, sub := range []string{"first", "second"} {
		t.Run(sub, func(t *testing.T) {
			db := Start(t)
			if got := One(t, db.DSN, "SELECT current_database()"); got != db.Name || !strings.Contains(db.Name, sub) {
				t.Errorf("got database %q (name %q), want the %s subtest's own", got, db.Name, sub)
			}
		})
	}
}

func TestConcurrentStartsGetIsolatedDatabases(t *testing.T) {
	const n = 4
	type result struct {
		db  *DB
		err error
	}
	results := make(chan result, n)
	for i := 0; i < n; i++ {
		go func() {
			db, err := start(t, Options{})
			results <- result{db, err}
		}()
	}
	var dbs []*DB
	for range n {
		r := <-results
		if r.err != nil {
			t.Fatalf("concurrent start: %v", r.err)
		}
		dbs = append(dbs, r.db)
	}
	// Every caller must have got its own, differently named database.
	seen := map[string]bool{}
	for _, db := range dbs {
		if seen[db.Name] {
			t.Errorf("database %q handed to two concurrent callers", db.Name)
		}
		seen[db.Name] = true
	}
	// A table in one is invisible in every other.
	for i, db := range dbs {
		Query(t, db.DSN, "CREATE TABLE only_here_"+strconv.Itoa(i)+" (v int)")
	}
	for i, db := range dbs {
		got := One(t, db.DSN, "SELECT string_agg(tablename, ',' ORDER BY tablename) FROM pg_tables WHERE schemaname = 'public'")
		if want := "only_here_" + strconv.Itoa(i); got != want {
			t.Errorf("database %q has tables %q, want only %q", db.Name, got, want)
		}
	}
	// All n databases are still live: cleanup runs when this test ends.
	got := One(t, dbs[0].DSN, "SELECT count(*) FROM pg_database WHERE datname LIKE "+quoteLiteral("test_testconcurrentstartsgetisolateddatabases%"))
	if got != strconv.Itoa(n) {
		t.Errorf("%s live databases, want %d", got, n)
	}
}

func TestBaseDSNReusesServer(t *testing.T) {
	base := startLocalServer(t)
	var dbs []*DB
	run := func(t *testing.T, starts int) {
		var mine []*DB
		for range starts {
			mine = append(mine, Start(t, base))
		}
		Query(t, mine[0].DSN, "CREATE TABLE t (v int)")
		Query(t, mine[0].DSN, "INSERT INTO t VALUES (7)")
		for _, db := range mine[1:] {
			if got := Count(t, db.DSN, db.Name); got != "1" {
				t.Errorf("database %q missing", db.Name)
			}
			if got := One(t, db.DSN, `SELECT to_regclass('t') IS NULL`); got != "t" {
				t.Errorf("another test's table reached database %q", db.Name)
			}
		}
		dbs = append(dbs, mine...)
	}
	t.Run("one", func(t *testing.T) { run(t, 1) })
	t.Run("two", func(t *testing.T) { run(t, 2) })
	// Sub-tests are done: their cleanup dropped their databases.
	seen := map[string]bool{}
	baseURL, _ := url.Parse(base)
	for _, db := range dbs {
		if seen[db.Name] {
			t.Errorf("database %q handed out twice", db.Name)
		}
		seen[db.Name] = true
		u, _ := url.Parse(db.DSN)
		if u.Host != baseURL.Host || u.User.String() != baseURL.User.String() {
			t.Errorf("DSN %q does not use the base server", db.DSN)
		}
		if got := Count(t, base, db.Name); got != "0" {
			t.Errorf("database %q still exists after cleanup", db.Name)
		}
	}
	if len(dbs) != 3 {
		t.Fatalf("collected %d databases, want 3", len(dbs))
	}
}

func TestInheritedBaseDSNIsReused(t *testing.T) {
	base := startLocalServer(t)
	t.Setenv(envTestDSN, base)
	db := Start(t)
	u, err := url.Parse(db.DSN)
	if err != nil {
		t.Fatalf("parse %q: %v", db.DSN, err)
	}
	if baseURL, _ := url.Parse(base); u.Host != baseURL.Host {
		t.Errorf("Start used %q, want the %s server", u.Host, envTestDSN)
	}
	Query(t, db.DSN, `CREATE TABLE from_env (v int)`)
	if got := One(t, db.DSN, `SELECT current_database()`); got != db.Name {
		t.Errorf("current_database() = %q, want %q", got, db.Name)
	}
}

func TestCleanupDropsOnlyOwnDatabases(t *testing.T) {
	base := startLocalServer(t)
	stray := "test_anuntaggeddatabase"
	own, err := baseServer(context.Background(), base)
	if err != nil {
		t.Fatalf("base server: %v", err)
	}
	if _, err := own.run(context.Background(), "postgres", `CREATE DATABASE `+quoteIdent(stray)); err != nil {
		t.Fatalf("create stray: %v", err)
	}
	t.Cleanup(func() {
		_, _ = own.run(context.Background(), "postgres", `DROP DATABASE IF EXISTS `+quoteIdent(stray))
	})
	own.dropDatabase(context.Background(), stray)
	if got := Count(t, base, stray); got != "1" {
		t.Errorf("an untagged database was dropped")
	}
	mine, err := own.createDatabase(context.Background(), "TestCleanupDropsOnlyOwnDatabases")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !own.tagged(context.Background(), mine) {
		t.Errorf("own database %q is not tagged", mine)
	}
	own.dropDatabase(context.Background(), mine)
	if got := Count(t, base, mine); got != "0" {
		t.Errorf("own database %q survived its cleanup", mine)
	}
}

// Another process's tag is not this process's permission: a test database
// left behind by a different Go process survives cleanup here.
func TestCleanupLeavesOtherProcessesDatabases(t *testing.T) {
	base := startLocalServer(t)
	other, err := baseServer(context.Background(), base)
	if err != nil {
		t.Fatalf("base server: %v", err)
	}
	name := "test_someotherprocess"
	ctx := context.Background()
	if _, err := other.run(ctx, "postgres", `CREATE DATABASE `+quoteIdent(name)); err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Cleanup(func() {
		_, _ = other.run(context.Background(), "postgres", `DROP DATABASE IF EXISTS `+quoteIdent(name))
	})
	if err := other.tag(ctx, name); err != nil {
		t.Fatalf("tag: %v", err)
	}
	mine, err := baseServer(context.Background(), base)
	if err != nil {
		t.Fatalf("base server: %v", err)
	}
	if mine.token == other.token {
		t.Fatalf("two servers share the permission token %q", mine.token)
	}
	mine.dropDatabase(context.Background(), name)
	if got := Count(t, base, name); got != "1" {
		t.Errorf("another process's database was dropped")
	}
}

func TestBaseDSNHostMustBeLocal(t *testing.T) {
	for host, ok := range map[string]bool{
		"localhost": true, "127.0.0.1": true, "::1": true, "": true,
		"db.example.com": false, "10.0.0.5": false,
	} {
		if err := localHostOnly(host); (err == nil) != ok {
			t.Errorf("localHostOnly(%q) = %v", host, err)
		}
	}
	if _, err := baseServer(context.Background(), "postgres://u@db.example.com:5432/app"); err == nil || !strings.Contains(err.Error(), "not a local address") {
		t.Errorf("baseServer accepted a remote host: %v", err)
	}
}

func TestSanitize(t *testing.T) {
	for in, want := range map[string]string{
		"TestFoo":                "testfoo",
		"TestFoo/bar_1":          "testfoo_bar_1",
		"Test Ångström (x)":      "test_ngstr_m_x",
		"__Test__":               "test",
		"":                       "",
		strings.Repeat("a", 100): strings.Repeat("a", 58),
	} {
		if got := sanitize(in); got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestQuote(t *testing.T) {
	if got := quoteIdent(`we"ird`); got != `"we""ird"` {
		t.Errorf("quoteIdent: %s", got)
	}
	if got := quoteLiteral(`it's`); got != `'it''s'` {
		t.Errorf("quoteLiteral: %s", got)
	}
}

func TestDSN(t *testing.T) {
	s := &server{tcp: tcpServer{address: "localhost:6000", user: "postgres", pass: "p@ss:word", maint: "postgres"}}
	u, err := url.Parse(s.dsn("my db"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if u.Host != "localhost:6000" || u.Path != "/my db" || u.Query().Get("sslmode") != "prefer" {
		t.Errorf("dsn: %s", s.dsn("my db"))
	}
	if pass, _ := u.User.Password(); pass != "p@ss:word" {
		t.Errorf("password not preserved")
	}
}

func TestParseRowRoundTrip(t *testing.T) {
	cells := []string{"plain", `with\ttab`, `with\nnewline`, `with\rcr`, `back\\slash`, `\N`}
	got := parseRow(strings.Join(cells, "\t"))
	want := []string{"plain", "with\ttab", "with\nnewline", "with\rcr", `back\slash`, `\N`}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("parseRow: %q, want %q", got, want)
	}
}
