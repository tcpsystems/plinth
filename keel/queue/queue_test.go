package queue

import (
	"database/sql"
	"strings"
	"testing"
)

func TestNewWithSchemaRejectsInjectedNames(t *testing.T) {
	for _, s := range []Schema{
		{Table: "job; DROP TABLE job"},
		{Table: "job WHERE true --"},
		{ID: "id = id OR 1=1"},
		{Priority: `"id"`},
		{Table: ".job"},
		{Table: "job."},
		{Epoch: "1epoch"},
		{Epoch: strings.Repeat("a", 64)},
	} {
		if _, err := NewWithSchema(nil, s); err == nil {
			t.Errorf("NewWithSchema(%+v) accepted a name that must be rejected", s)
		}
	}
}

func TestNewRejectsNilDatabase(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) = nil error")
	}
}

func TestDefaultSchemaStatements(t *testing.T) {
	q, err := NewWithSchema(&sql.DB{}, Schema{})
	if err != nil {
		t.Fatalf("NewWithSchema: %v", err)
	}
	flat := strings.Join(strings.Fields(q.claim), " ")
	want := []string{"WHERE d.state = 'pending'", "d.next_due <= now()",
		"ORDER BY d.priority, d.next_due, d.id", "LIMIT $1", "FOR UPDATE SKIP LOCKED",
		"SET state = 'claimed', lease_epoch = t.lease_epoch + 1", "RETURNING t.id, t.lease_epoch"}
	for _, w := range want {
		if !strings.Contains(flat, w) {
			t.Errorf("claim statement misses %q:\n%s", w, q.claim)
		}
	}
	// The locking clause must be inside the limited subquery, so it locks at
	// most n rows, and the update must be the outer statement: one atomic
	// claim, not a select followed by an update.
	if !strings.Contains(flat, "LIMIT $1 FOR UPDATE SKIP LOCKED") {
		t.Errorf("LIMIT must bind before SKIP LOCKED:\n%s", q.claim)
	}
	if !strings.HasPrefix(flat, "UPDATE ") {
		t.Errorf("claim must be one update statement:\n%s", q.claim)
	}
	for name, stmt := range map[string]string{"settle": q.settle, "fail": q.fail} {
		if !strings.Contains(stmt, "id = $1") || !strings.Contains(stmt, "lease_epoch = $2") ||
			!strings.Contains(stmt, "state = 'claimed'") {
			t.Errorf("%s statement is not fenced on id, epoch and state:\n%s", name, stmt)
		}
	}
}
