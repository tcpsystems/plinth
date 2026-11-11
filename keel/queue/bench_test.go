package queue_test

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/keel/queue"
)

// dueRows is the seeded backlog: every row is due when the benchmark starts.
const dueRows = 1_000_000

// benchTable loads a job table with dueRows due rows and returns a Queue over
// it. The caller drops the table through cleanup.
func benchTable(b *testing.B) (*queue.Queue, *sql.DB, string, func()) {
	b.Helper()
	dsn := os.Getenv("KEEL_TEST_DSN")
	if dsn == "" {
		b.Skip("KEEL_TEST_DSN is not set; run through tools/testdb.sh go test -bench ...")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		b.Fatalf("open database: %v", err)
	}
	// One claimer: the benchmark measures the claim statement, not pool
	// contention between claimers.
	db.SetMaxOpenConns(1)
	name := nextTableName()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `CREATE TABLE `+name+` (
		id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		priority    integer     NOT NULL DEFAULT 100,
		next_due    timestamptz NOT NULL DEFAULT now(),
		state       text        NOT NULL DEFAULT 'pending',
		lease_epoch bigint      NOT NULL DEFAULT 0
	)`); err != nil {
		b.Fatalf("create table: %v", err)
	}
	// Spread priority and next_due so the claim index has real ordering work.
	if _, err := db.ExecContext(ctx, `INSERT INTO `+name+` (priority, next_due)
		SELECT (g % 10), now() - make_interval(secs => g % 3600)
		FROM generate_series(1, $1) AS g`, dueRows); err != nil {
		b.Fatalf("seed rows: %v", err)
	}
	// Build the index after the load; it is the index ClaimN needs.
	if _, err := db.ExecContext(ctx, `CREATE INDEX ON `+name+` (state, priority, next_due, id)`); err != nil {
		b.Fatalf("create index: %v", err)
	}
	s := queue.Default
	s.Table = name
	q, err := queue.NewWithSchema(db, s)
	if err != nil {
		b.Fatalf("NewWithSchema: %v", err)
	}
	cleanup := func() {
		_, _ = db.ExecContext(context.Background(), `DROP TABLE IF EXISTS `+name)
		_ = db.Close()
	}
	return q, db, name, cleanup
}

// p99 returns the 99th percentile of d.
func p99(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), d...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	i := int(math.Ceil(0.99*float64(len(sorted)))) - 1
	if i < 0 {
		i = 0
	}
	return sorted[i]
}

// BenchmarkClaimN compares one-row claims with 32-row claims against a
// million-row due backlog, reporting claims/s, rows/s and per-claim p99.
func BenchmarkClaimN(b *testing.B) {
	for _, batch := range []int{1, 32} {
		b.Run(fmt.Sprintf("rows=%d", batch), func(b *testing.B) {
			q, db, table, cleanup := benchTable(b)
			defer cleanup()
			ctx := context.Background()

			durations := make([]time.Duration, 0, b.N)
			claimed := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				start := time.Now()
				got, err := q.ClaimN(ctx, batch)
				durations = append(durations, time.Since(start))
				if err != nil {
					b.Fatalf("ClaimN(%d): %v", batch, err)
				}
				if len(got) == 0 {
					// The backlog drained (only reachable on a very fast host
					// with a long benchtime); refill outside the measurement.
					b.StopTimer()
					if _, err := db.ExecContext(ctx, `UPDATE `+table+` SET state = 'pending'`); err != nil {
						b.Fatalf("refill: %v", err)
					}
					b.StartTimer()
					continue
				}
				claimed += len(got)
			}
			b.StopTimer()

			var total time.Duration
			for _, d := range durations {
				total += d
			}
			if total > 0 {
				b.ReportMetric(float64(b.N)/total.Seconds(), "claims/s")
				b.ReportMetric(float64(claimed)/total.Seconds(), "rows/s")
			}
			b.ReportMetric(float64(p99(durations).Microseconds()), "p99-µs")
		})
	}
}
