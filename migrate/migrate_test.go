package migrate

import (
	"database/sql/driver"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/tcpsystems/plinth/internal/fakesql"
)

var fsys = fstest.MapFS{
	"sql/002_b.sql":  {Data: []byte("CREATE TABLE b();")},
	"sql/001_a.sql":  {Data: []byte("CREATE TABLE a();")},
	"sql/README.txt": {Data: []byte("ignored")},
}

var downFS = fstest.MapFS{
	"sql/001_a.sql":      {Data: []byte("CREATE TABLE a();")},
	"sql/001_a.down.sql": {Data: []byte("DROP TABLE a;")},
	"sql/002_b.sql":      {Data: []byte("CREATE TABLE b();")},
	"sql/002_b.down.sql": {Data: []byte("DROP TABLE b;")},
	"sql/003_c.sql":      {Data: []byte("CREATE TABLE c();")},
}

func TestLoadSorted(t *testing.T) {
	ms, err := Load(fsys, "sql")
	if err != nil || len(ms) != 2 || ms[0].Version != 1 || ms[1].Name != "b" {
		t.Fatalf("%+v %v", ms, err)
	}
}

func TestLoadPairsDownFiles(t *testing.T) {
	ms, err := Load(downFS, "sql")
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 3 {
		t.Fatalf("got %d migrations", len(ms))
	}
	if ms[0].Down != "DROP TABLE a;" || ms[1].Down != "DROP TABLE b;" || ms[2].Down != "" {
		t.Fatalf("down bodies: %q %q %q", ms[0].Down, ms[1].Down, ms[2].Down)
	}
	if ms[0].SQL != "CREATE TABLE a();" {
		t.Fatalf("up body: %q", ms[0].SQL)
	}
}

func TestLoadRejectsBadNames(t *testing.T) {
	if _, err := Load(fstest.MapFS{"s/x.sql": {}}, "s"); err == nil {
		t.Fatal("want error for x.sql")
	}
	if _, err := Load(fstest.MapFS{"s/001_a.sql": {}, "s/001_b.sql": {}}, "s"); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatal(err)
	}
}

func TestLoadRejectsBadDownFiles(t *testing.T) {
	cases := map[string]fstest.MapFS{
		"down without up": {
			"s/001_a.down.sql": {Data: []byte("DROP TABLE a;")},
		},
		"down name mismatch": {
			"s/001_a.sql":      {Data: []byte("CREATE TABLE a();")},
			"s/001_b.down.sql": {Data: []byte("DROP TABLE b;")},
		},
		"malformed down": {
			"s/001_a.sql":  {Data: []byte("CREATE TABLE a();")},
			"s/x.down.sql": {Data: []byte("DROP TABLE x;")},
		},
	}
	for name, fsys := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(fsys, "s"); err == nil {
				t.Fatal("want error")
			}
		})
	}
}

func TestLoadValidatesOptions(t *testing.T) {
	if _, err := Load(downFS, "sql", Options{Schema: "bad-name"}); err == nil {
		t.Fatal("want schema error")
	}
	if _, err := Load(downFS, "sql", Options{Table: "1bad"}); err == nil {
		t.Fatal("want table error")
	}
	if _, err := Load(downFS, "sql", Options{Table: "a b"}); err == nil {
		t.Fatal("want table error")
	}
	if _, err := Load(downFS, "sql", Options{}, Options{}); err == nil {
		t.Fatal("want error for two Options values")
	}
	if _, err := Load(downFS, "sql", Options{Schema: "app", Table: "ledger_1"}); err != nil {
		t.Fatal(err)
	}
}

// recorder answers statements from a fixed ledger and records the SQL and the
// numeric arguments Up and Rollback pass.
type recorder struct {
	queries  []string
	lockKeys []driver.Value
	ledger   map[int]string
	max      int
}

func (r *recorder) handler() fakesql.Handler {
	return func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		r.queries = append(r.queries, q)
		switch {
		case strings.HasPrefix(q, "SELECT pg_advisory_lock"):
			r.lockKeys = append(r.lockKeys, a...)
			return nil, nil, 1, nil
		case strings.HasPrefix(q, "SELECT version, checksum"):
			var rows [][]driver.Value
			for v, c := range r.ledger {
				rows = append(rows, []driver.Value{int64(v), c})
			}
			return []string{"version", "checksum"}, rows, 0, nil
		case strings.HasPrefix(q, "SELECT max(version)"):
			return []string{"max"}, [][]driver.Value{{int64(r.max)}}, 0, nil
		}
		return nil, nil, 1, nil
	}
}

func (r *recorder) index(q string) int {
	for i, got := range r.queries {
		if got == q {
			return i
		}
	}
	return -1
}

func TestUpAppliesPending(t *testing.T) {
	ms, _ := Load(fsys, "sql")
	s := &recorder{ledger: map[int]string{1: ms[0].Checksum()}}
	got, err := Up(t.Context(), fakesql.Open(s.handler()), ms)
	if err != nil || len(got) != 1 || got[0] != 2 {
		t.Fatalf("%v %v", got, err)
	}
	log := strings.Join(s.queries, "|")
	if !strings.Contains(log, "SELECT pg_advisory_lock") || !strings.Contains(log, "COMMIT") || !strings.Contains(log, "pg_advisory_unlock") {
		t.Fatal(log)
	}
}

func TestUpDetectsEditedMigration(t *testing.T) {
	ms, _ := Load(fsys, "sql")
	s := &recorder{ledger: map[int]string{1: "deadbeef"}}
	if _, err := Up(t.Context(), fakesql.Open(s.handler()), ms); err == nil || !strings.Contains(err.Error(), "edited") {
		t.Fatal(err)
	}
}

func TestUpUsesOptions(t *testing.T) {
	ms, _ := Load(fsys, "sql")
	s := &recorder{ledger: map[int]string{}}
	opts := Options{Schema: "app", Table: "ledger", LockKey: 918273646}
	if _, err := Up(t.Context(), fakesql.Open(s.handler()), ms, opts); err != nil {
		t.Fatal(err)
	}
	if len(s.lockKeys) != 1 || s.lockKeys[0] != int64(918273646) {
		t.Fatalf("lock keys: %v", s.lockKeys)
	}
	log := strings.Join(s.queries, "|")
	if !strings.Contains(log, `"app"."ledger"`) || strings.Contains(log, "schema_migrations") {
		t.Fatal(log)
	}
}

func TestVersionUsesOptions(t *testing.T) {
	s := &recorder{max: 7}
	got, err := Version(t.Context(), fakesql.Open(s.handler()), Options{Schema: "app", Table: "ledger"})
	if err != nil || got != 7 {
		t.Fatalf("%d %v", got, err)
	}
	if !strings.Contains(s.queries[0], `"app"."ledger"`) {
		t.Fatal(s.queries[0])
	}
}

func TestRollbackNewestFirstInOneTransaction(t *testing.T) {
	ms, _ := Load(downFS, "sql")
	s := &recorder{ledger: map[int]string{
		1: ms[0].Checksum(), 2: ms[1].Checksum(),
	}}
	got, err := Rollback(t.Context(), fakesql.Open(s.handler()), ms, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Fatalf("reverted %v", got)
	}
	i2, i1 := s.index("DROP TABLE b;"), s.index("DROP TABLE a;")
	if i2 < 0 || i1 < 0 || i2 > i1 {
		t.Fatalf("down order: %q", s.queries)
	}
	if s.index("BEGIN") < 0 || s.index("COMMIT") < 0 || s.index("ROLLBACK") >= 0 {
		t.Fatalf("transactions: %q", s.queries)
	}
	if n := strings.Count(strings.Join(s.queries, "|"), "BEGIN"); n != 1 {
		t.Fatalf("want one transaction, got %d: %q", n, s.queries)
	}
	if len(s.lockKeys) != 1 || s.lockKeys[0] != int64(727274) {
		t.Fatalf("lock keys: %v", s.lockKeys)
	}
}

func TestRollbackRefusesDrift(t *testing.T) {
	ms, _ := Load(downFS, "sql")
	s := &recorder{ledger: map[int]string{1: "deadbeef"}}
	if _, err := Rollback(t.Context(), fakesql.Open(s.handler()), ms, 0); err == nil || !strings.Contains(err.Error(), "edited") {
		t.Fatal(err)
	}
	if s.index("BEGIN") >= 0 {
		t.Fatalf("ran a transaction after refusing: %q", s.queries)
	}
}

func TestRollbackRequiresDown(t *testing.T) {
	ms, _ := Load(downFS, "sql")
	s := &recorder{ledger: map[int]string{3: ms[2].Checksum()}}
	if _, err := Rollback(t.Context(), fakesql.Open(s.handler()), ms, 0); err == nil || !strings.Contains(err.Error(), "no down file") {
		t.Fatal(err)
	}
}

func TestRollbackRejectsUnknownAppliedVersion(t *testing.T) {
	ms, _ := Load(downFS, "sql")
	s := &recorder{ledger: map[int]string{9: "unknown"}}
	if _, err := Rollback(t.Context(), fakesql.Open(s.handler()), ms, 0); err == nil || !strings.Contains(err.Error(), "no loaded migration") {
		t.Fatal(err)
	}
}
