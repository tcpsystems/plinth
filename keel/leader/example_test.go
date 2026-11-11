package leader_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/tcpsystems/plinth/keel/internal/fakesql"
	"github.com/tcpsystems/plinth/keel/leader"
)

// Example_worker runs a background job in exactly one replica. In
// production, open the pool with leader.OpenSQL(dsn); here an in-memory
// database answers as an empty Postgres would. Start it in several replicas;
// in each round only one of them is the leader and does the work.
func Example_worker() {
	h := func(query string, _ []driver.Value) ([]string, [][]driver.Value, int64, error) {
		switch query {
		case "SELECT pg_try_advisory_lock($1::int4, $2::int4)":
			// An empty database: every caller gets the lock.
			return []string{"pg_try_advisory_lock"}, [][]driver.Value{{true}}, 0, nil
		case "SELECT pg_advisory_unlock($1::int4, $2::int4)":
			return []string{"pg_advisory_unlock"}, [][]driver.Value{{true}}, 0, nil
		}
		if strings.Contains(query, "FROM pg_locks") {
			return []string{"?column?"}, [][]driver.Value{{true}}, 0, nil
		}
		return nil, nil, 0, fmt.Errorf("unexpected statement %q", query)
	}
	fakesql.Register("example-leader", h)
	db, err := leader.OpenSQL("example-leader", "")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	m := leader.New(db, leader.Config{Namespace: 42})
	defer func() { _ = m.Close() }()

	ctx := context.Background()

	const (
		job = "hourly-report"
		ttl = 90 * time.Second
	)
	ok, err := m.TryAcquire(ctx, job, ttl)
	if err != nil {
		log.Fatal(fmt.Errorf("try acquire: %w", err))
	}
	if ok {
		defer func() {
			if _, err := m.Release(ctx, job); err != nil {
				log.Printf("release %s: %v", job, err)
			}
		}()
		fmt.Println("leader: doing the work")
		// Before each side-effecting step, check that the lock is still held,
		// so a lost lock stops the job instead of overlapping the new leader.
		held, err := m.Renew(ctx, job)
		if err != nil || !held {
			log.Fatal(fmt.Errorf("renew: %w", err))
		}
	} else {
		fmt.Println("not the leader; skipping")
	}

	// Output: leader: doing the work
}
