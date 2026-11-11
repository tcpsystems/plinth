// Package postgres gives a test a real PostgreSQL database and the DSN that
// reaches it, then removes the database when the test ends.
//
// By default Start launches a single-use container (podman, image
// postgres:18-alpine) and removes it when the test ends. When a base DSN is
// supplied, as an argument, through Options.BaseDSN, or through
// TEST_POSTGRES_DSN, no container starts: the server named by the base DSN is
// reused. Either way every Start call creates
// its own database with a name unique to the test (the sanitized test name,
// with a numeric suffix when two tests share one) and drops it at cleanup.
//
// The returned DSN is a URL that database/sql drivers accept (the
// github.com/jackc/pgx stdlib driver, for one). It reaches the database from
// the test process: a reused server keeps its address, and a container's
// server listens on a port picked from the loopback interfaces, the way
// `postgres -p PORT` would. The helper itself needs no driver.
//
// Requirements. A container start needs podman and the image present
// locally; Start never pulls. The role in a base DSN must be allowed to
// CREATE DATABASE.
//
// Only databases the helper created are removed: it tags each one with a
// per-process token and drops a name only if the tag matches, so a stray
// name in a reused server is never touched. Base DSNs must name a loopback
// TCP host (localhost, 127.0.0.1 or ::1).
package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// Options tune Start. The zero value starts a throwaway container.
type Options struct {
	// BaseDSN reuses a running server instead of starting a container.
	BaseDSN string
	// Image is the container image. Default "docker.io/library/postgres:18-alpine".
	Image string
	// Podman is the podman binary. Default "podman".
	Podman string
	// StartupTimeout bounds server start and database creation.
	// Default 2 minutes.
	StartupTimeout time.Duration
}

// DB is one private database for one test.
type DB struct {
	// DSN is the connection URL for the test database.
	DSN string
	// Name is the database name.
	Name string
}

const (
	defaultImage = "docker.io/library/postgres:18-alpine"
	envTestDSN   = "TEST_POSTGRES_DSN"
)

// Start creates a private database for t and registers cleanup that drops it
// and, when no base DSN was used, stops the container. When setup fails it
// removes everything it created and fails the test with t.Fatalf.
//
// base is optional; the first non-empty of base, Options.BaseDSN and
// TEST_POSTGRES_DSN is used. An application's DATABASE_URL is never used:
// tests must not reach a database they were not pointed at.
func Start(t testing.TB, base ...string) *DB {
	t.Helper()
	return StartWithOptions(t, Options{BaseDSN: firstNonEmpty(base...)})
}

// StartWithOptions is Start with explicit options.
func StartWithOptions(t testing.TB, opts Options) *DB {
	t.Helper()
	db, err := start(t, opts)
	if err != nil {
		t.Fatalf("postgres: %v", err)
	}
	return db
}

// start is StartWithOptions without the test failure. It returns the error so
// a caller running in a goroutine can report it on that goroutine's terms;
// StartWithOptions is the usual entry point. Cleanup is registered on t and
// testing.TB methods are safe to call from any goroutine.
func start(t testing.TB, opts Options) (*DB, error) {
	t.Helper()
	timeout := opts.StartupTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	srv, err := newServer(ctx, opts)
	if err != nil {
		return nil, err
	}
	name, err := srv.createDatabase(ctx, t.Name())
	if err != nil {
		srv.stop(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("create test database: %w", err)
	}
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), timeout)
		defer ccancel()
		// A created but half-made database (say, tagging failed) is still
		// ours once we know its name.
		srv.own(name)
		srv.dropDatabase(cctx, name)
		srv.stop(cctx)
	})
	return &DB{DSN: srv.dsn(name), Name: name}, nil
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

// server is a running PostgreSQL the helper creates databases on.
type server struct {
	tcp   tcpServer
	token string // permission tag recorded on databases this helper made

	mu      sync.Mutex
	created map[string]bool // databases this server created, by name
}

func newServer(ctx context.Context, opts Options) (*server, error) {
	base := firstNonEmpty(opts.BaseDSN, os.Getenv(envTestDSN))
	if base != "" {
		return baseServer(ctx, base)
	}
	return containerServer(ctx, opts)
}

// baseServer reuses the server named by a base DSN.
func baseServer(ctx context.Context, dsn string) (*server, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse base DSN: %w", err)
	}
	if u.Scheme != "postgres" && u.Scheme != "postgresql" {
		return nil, fmt.Errorf("base DSN scheme %q is not postgres or postgresql", u.Scheme)
	}
	addr := u.Host
	if u.Port() == "" {
		addr = u.Hostname() + ":5432"
	}
	user := "postgres"
	var pass string
	if u.User != nil {
		if name := u.User.Username(); name != "" {
			user = name
		}
		pass, _ = u.User.Password()
	}
	maint := strings.TrimPrefix(u.Path, "/")
	if maint == "" {
		maint = "postgres"
	}
	if err := localHostOnly(u.Hostname()); err != nil {
		return nil, err
	}
	// A server that has just started accepts connections before it accepts
	// sessions, so retry while it reports "starting up".
	return adoptReady(ctx, tcpServer{address: addr, user: user, pass: pass, maint: maint})
}

func (s *server) connect(ctx context.Context) error {
	c, err := connectSession(ctx, s.tcp.address, s.tcp.user, s.tcp.pass, s.tcp.maint)
	if err != nil {
		return err
	}
	c.close()
	return nil
}

func (s *server) run(ctx context.Context, database, sql string) ([]string, error) {
	c, err := connectSession(ctx, s.tcp.address, s.tcp.user, s.tcp.pass, database)
	if err != nil {
		return nil, err
	}
	defer c.close()
	return c.run(ctx, sql)
}

// adopt opens a server whose session parameters are known and gives it a
// permission token.
func adopt(ctx context.Context, tcp tcpServer) (*server, error) {
	srv := &server{tcp: tcp, created: map[string]bool{}}
	if err := srv.connect(ctx); err != nil {
		return nil, fmt.Errorf("connect to %s: %w", tcp.address, err)
	}
	token, err := randomToken()
	if err != nil {
		return nil, err
	}
	srv.token = token
	return srv, nil
}

// localHostOnly keeps the helper from touching anything but a loopback
// server: it is for tests, not for managed servers.
func localHostOnly(host string) error {
	if host == "" {
		return nil // no host: the server is local by default
	}
	switch strings.Trim(host, "[]") {
	case "localhost", "127.0.0.1", "::1":
		return nil
	}
	return fmt.Errorf("base DSN host %q is not a local address", host)
}

// createDatabase creates a database named after test (plus a counter when
// that name is taken) and tags it so cleanup only removes testkit databases.
func (s *server) createDatabase(ctx context.Context, test string) (string, error) {
	base := "test_" + sanitize(test)
	if base == "test_" {
		base = "test_db"
	}
	for n := 0; n < 100; n++ {
		name := base
		if n > 0 {
			name = base + "_" + strconv.Itoa(n)
		}
		_, err := s.run(ctx, s.tcp.maint, `CREATE DATABASE `+quoteIdent(name)+` TEMPLATE template0`)
		if err == nil {
			if err := s.tag(ctx, name); err != nil {
				return "", err
			}
			return name, nil
		}
		if nameTaken(err) {
			continue
		}
		return "", err
	}
	return "", fmt.Errorf("no free database name for %q", test)
}

// nameTaken reports whether CREATE DATABASE failed because another session
// created the same name first. Servers report that as a unique violation
// (SQLSTATE 23505) or as "already exists" (42P04).
func nameTaken(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "23505") || strings.Contains(msg, "42P04") || strings.Contains(msg, "already exists")
}

// own records name in the created set so cleanup drops it even if tagging
// it did not finish.
func (s *server) own(name string) {
	s.mu.Lock()
	s.created[name] = true
	s.mu.Unlock()
}

// tag records this server's token on the new database.
func (s *server) tag(ctx context.Context, name string) error {
	_, err := s.run(ctx, s.tcp.maint, `COMMENT ON DATABASE `+quoteIdent(name)+` IS `+quoteLiteral("plinth testkit "+s.token))
	return err
}

func (s *server) tagged(ctx context.Context, name string) bool {
	rows, err := s.run(ctx, s.tcp.maint, `SELECT shobj_description(oid, 'pg_database')`+
		` FROM pg_database WHERE datname = `+quoteLiteral(name))
	if err != nil || len(rows) != 1 {
		return false
	}
	cells := parseRow(rows[0])
	return len(cells) == 1 && cells[0] == "plinth testkit "+s.token
}

// dropDatabase removes a test database this helper created. Untagged names
// are left alone.
func (s *server) dropDatabase(ctx context.Context, name string) {
	s.mu.Lock()
	mine := s.created[name]
	s.mu.Unlock()
	if !mine && !s.tagged(ctx, name) {
		return
	}
	_, _ = s.run(ctx, s.tcp.maint, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = `+quoteLiteral(name)+` AND pid <> pg_backend_pid()`)
	_, _ = s.run(ctx, s.tcp.maint, `DROP DATABASE IF EXISTS `+quoteIdent(name))
}

func sanitize(name string) string {
	var b strings.Builder
	underscore := true // leading separators are trimmed later
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			b.WriteRune(r)
			underscore = false
		} else if !underscore {
			b.WriteByte('_')
			underscore = true
		}
	}
	s := strings.Trim(b.String(), "_")
	if len(s) > 58 {
		s = s[:58]
	}
	return s
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func quoteLiteral(s string) string { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }

func randomToken() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
