package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/riverqueue/river"

	"github.com/tcpsystems/plinth/keel/outbox"
	"github.com/tcpsystems/plinth/keel/queue"
)

var schemas atomic.Int32

// env opens the disposable database from KEEL_TEST_DSN (tools/testdb.sh go
// ...), gives the test a private schema holding River's tables and the outbox
// table, and returns the pool with that schema as its search path.
func env(t *testing.T) (*sql.DB, string) {
	t.Helper()
	dsn := os.Getenv("KEEL_TEST_DSN")
	if dsn == "" {
		t.Skip("KEEL_TEST_DSN is not set; run through tools/testdb.sh go")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("outbox_%d_%d", os.Getpid(), schemas.Add(1))
	if _, err := admin.Exec(`CREATE SCHEMA ` + name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(`DROP SCHEMA IF EXISTS ` + name + ` CASCADE`)
		_ = admin.Close()
	})
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	db, err := sql.Open("pgx", dsn+sep+"search_path="+name)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(32)
	t.Cleanup(func() { _ = db.Close() })
	ddl, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(ddl)); err != nil {
		t.Fatalf("outbox schema: %v", err)
	}
	return db, name
}

type recorder struct {
	mu   sync.Mutex
	got  map[string]int
	fail func(outbox.Event) error
}

func (r *recorder) Publish(_ context.Context, e outbox.Event) error {
	if r.fail != nil {
		if err := r.fail(e); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.got == nil {
		r.got = map[string]int{}
	}
	r.got[e.ID]++
	return nil
}

func (r *recorder) count(id string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.got[id]
}

func start(t *testing.T, db *sql.DB, schema string, pub outbox.Publisher, cfg queue.Config) *queue.Client {
	t.Helper()
	w := river.NewWorkers()
	if err := outbox.Register(w, db, pub); err != nil {
		t.Fatal(err)
	}
	cfg.DB, cfg.Schema, cfg.Workers = db, schema, w
	cfg.FetchPollInterval, cfg.FetchCooldown = 50*time.Millisecond, 20*time.Millisecond
	c, err := queue.NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = c.Stop(ctx)
	})
	return c
}

func eventually(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func published(t *testing.T, db *sql.DB, id string) bool {
	t.Helper()
	var at sql.NullTime
	if err := db.QueryRow(`SELECT published_at FROM outbox WHERE id = $1`, id).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return at.Valid
}

func TestWriteRollsBackRowAndJobTogether(t *testing.T) {
	db, schema := env(t)
	if err := queue.Migrate(t.Context(), db, schema); err != nil {
		t.Fatal(err)
	}
	c, err := queue.NewClient(queue.Config{DB: db, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	w := outbox.Writer{Queue: c}
	ctx := t.Context()

	tx, _ := db.BeginTx(ctx, nil)
	id, err := w.Write(ctx, tx, outbox.Event{Topic: "card.created", Key: "c1", Payload: []byte(`{"a":1}`), Headers: map[string]string{"k": "v"}})
	if err != nil || len(id) != 32 {
		t.Fatal(id, err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var rows, jobs int
	_ = db.QueryRow(`SELECT count(*) FROM outbox`).Scan(&rows)
	_ = db.QueryRow(`SELECT count(*) FROM river_job`).Scan(&jobs)
	if rows != 0 || jobs != 0 {
		t.Fatalf("rollback left %d outbox rows and %d jobs", rows, jobs)
	}

	tx, _ = db.BeginTx(ctx, nil)
	if _, err := w.Write(ctx, tx, outbox.Event{ID: id, Topic: "card.created", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	_ = db.QueryRow(`SELECT count(*) FROM outbox`).Scan(&rows)
	_ = db.QueryRow(`SELECT count(*) FROM river_job WHERE kind = 'keel.outbox.relay'`).Scan(&jobs)
	if rows != 1 || jobs != 1 {
		t.Fatalf("commit left %d outbox rows and %d jobs, want 1 and 1", rows, jobs)
	}
}

func TestWriteValidation(t *testing.T) {
	w := outbox.Writer{}
	for name, call := range map[string]func() error{
		"nil tx": func() error {
			_, err := w.Write(t.Context(), nil, outbox.Event{Topic: "t", Payload: []byte(`1`)})
			return err
		},
		"nil queue": func() error {
			_, err := w.Write(t.Context(), &sql.Tx{}, outbox.Event{Topic: "t", Payload: []byte(`1`)})
			return err
		},
	} {
		if call() == nil {
			t.Errorf("%s accepted", name)
		}
	}
	w.Queue = &queue.Client{}
	for _, e := range []outbox.Event{{Payload: []byte(`{}`)}, {Topic: "t", Payload: []byte(`{bad`)}, {Topic: "t"}} {
		if _, err := w.Write(t.Context(), &sql.Tx{}, e); err == nil {
			t.Errorf("accepted %+v", e)
		}
	}
	if err := outbox.Register(nil, nil, nil); err == nil {
		t.Error("Register accepted nil arguments")
	}
}

func TestRelayPublishesAfterCommit(t *testing.T) {
	db, schema := env(t)
	rec := &recorder{}
	c := start(t, db, schema, rec, queue.Config{})
	w := outbox.Writer{Queue: c}
	ctx := t.Context()

	tx, _ := db.BeginTx(ctx, nil)
	id, err := w.Write(ctx, tx, outbox.Event{Topic: "t", Key: "k", Payload: []byte(`{"n":1}`), Headers: map[string]string{"h": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond) // many fetch cycles
	if rec.count(id) != 0 {
		t.Fatal("event published before its transaction committed")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 15*time.Second, "publish", func() bool { return rec.count(id) == 1 && published(t, db, id) })
}

func TestRelayDoesNotHoldRowLocksWhilePublishing(t *testing.T) {
	db, schema := env(t)
	inPublish, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	pub := &recorder{fail: func(outbox.Event) error {
		once.Do(func() { close(inPublish) })
		<-release
		return nil
	}}
	c := start(t, db, schema, pub, queue.Config{})
	w := outbox.Writer{Queue: c}
	ctx := t.Context()

	tx, _ := db.BeginTx(ctx, nil)
	id, _ := w.Write(ctx, tx, outbox.Event{Topic: "t", Payload: []byte(`1`)})
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-inPublish:
	case <-time.After(15 * time.Second):
		t.Fatal("publisher never called")
	}
	// While the broker call is in flight the row must be writable at once:
	// no relay transaction holds its lock.
	lockCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ltx, err := db.BeginTx(lockCtx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ltx.ExecContext(lockCtx, `SET LOCAL lock_timeout = '500ms'`); err != nil {
		t.Fatal(err)
	}
	if _, err := ltx.ExecContext(lockCtx, `UPDATE outbox SET headers = '{"x":"y"}' WHERE id = $1`, id); err != nil {
		t.Fatalf("row locked during publish: %v", err)
	}
	_ = ltx.Rollback()
	close(release)
	eventually(t, 15*time.Second, "mark published", func() bool { return published(t, db, id) })
}

func TestFailedPublishRetriesThenDies(t *testing.T) {
	db, schema := env(t)
	var calls atomic.Int32
	pub := &recorder{fail: func(outbox.Event) error {
		calls.Add(1)
		return errors.New("broker down")
	}}
	c := start(t, db, schema, pub, queue.Config{MaxAttempts: 2, BackoffBase: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond})
	w := outbox.Writer{Queue: c}
	ctx := t.Context()

	tx, _ := db.BeginTx(ctx, nil)
	id, _ := w.Write(ctx, tx, outbox.Event{Topic: "t", Payload: []byte(`1`)})
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	eventually(t, 45*time.Second, "dead relay job", func() bool {
		var n int
		_ = db.QueryRow(`SELECT count(*) FROM river_job WHERE state = $1`, string(queue.Dead)).Scan(&n)
		return n == 1
	})
	var attempts int
	_ = db.QueryRow(`SELECT attempts FROM outbox WHERE id = $1`, id).Scan(&attempts)
	if calls.Load() != 2 || attempts != 2 || published(t, db, id) {
		t.Fatalf("calls=%d attempts=%d published=%v; want 2, 2, false", calls.Load(), attempts, published(t, db, id))
	}
}

func TestDuplicateEventIDIsRejected(t *testing.T) {
	db, schema := env(t)
	if err := queue.Migrate(t.Context(), db, schema); err != nil {
		t.Fatal(err)
	}
	c, _ := queue.NewClient(queue.Config{DB: db, Schema: schema})
	ctx := t.Context()
	w := outbox.Writer{Queue: c}
	tx, _ := db.BeginTx(ctx, nil)
	if _, err := w.Write(ctx, tx, outbox.Event{ID: "e1", Topic: "t", Payload: []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	// Same event id again: the primary key rejects the row before a second
	// job is enqueued.
	if _, err := w.Write(ctx, tx, outbox.Event{ID: "e1", Topic: "t", Payload: []byte(`1`)}); err == nil {
		t.Fatal("duplicate event id accepted")
	}
	_ = tx.Rollback()
}
