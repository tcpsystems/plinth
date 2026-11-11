package events

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/keel/internal/fakesql"
)

// fakePG is an in-memory stand-in for the events table, answered with the
// same statements the bus sends to Postgres. BEGIN/COMMIT/ROLLBACK are emulated with an
// undo log, so the bus's transaction boundaries are observable.
type fakePG struct {
	mu      sync.Mutex
	next    int64           // last value handed out by the "sequence"
	rows    map[int64]dbRow // committed rows
	un      map[int64]dbRow // rows written by the open transaction
	inTx    bool
	notices []uint64         // seqs passed to pg_notify
	fail    map[string]error // statement prefix -> error

	// seqReuse counts reserved seqs no committed row uses, as a Postgres
	// sequence does: nextval is never undone.
	seqReuse atomic.Int64

	reads atomic.Int64 // SELECTs of stored events, for notification-free proofs
}

type dbRow struct {
	seq       int64
	topic     string
	kind      string
	ref       string
	payload   []byte
	emittedAt time.Time
}

func newFakePG(t *testing.T) *fakePG {
	t.Helper()
	return &fakePG{rows: map[int64]dbRow{}, un: map[int64]dbRow{}, fail: map[string]error{}}
}

func (f *fakePG) open() *sql.DB {
	db := fakesql.Open(f.handle)
	return db
}

func (f *fakePG) setFail(prefix string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err == nil {
		delete(f.fail, prefix)
		return
	}
	f.fail[prefix] = err
}

func (f *fakePG) handle(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for prefix, err := range f.fail {
		if strings.HasPrefix(q, prefix) {
			return nil, nil, 0, err
		}
	}
	q = strings.TrimSpace(q)
	switch {
	case q == "BEGIN":
		if f.inTx {
			return nil, nil, 0, errors.New("nested transaction")
		}
		f.inTx, f.un = true, map[int64]dbRow{}
		return nil, nil, 0, nil
	case q == "COMMIT":
		if !f.inTx {
			return nil, nil, 0, errors.New("commit outside transaction")
		}
		for k, v := range f.un {
			f.rows[k] = v
		}
		f.inTx, f.un = false, map[int64]dbRow{}
		return nil, nil, 1, nil
	case q == "ROLLBACK":
		if f.inTx {
			f.inTx, f.un = false, map[int64]dbRow{}
		}
		return nil, nil, 0, nil
	case strings.HasPrefix(q, "SELECT nextval"):
		n, _ := a[0].(int64)
		first := f.next + 1
		f.next += n
		f.seqReuse.Add(n)
		return []string{"nextval"}, [][]driver.Value{{first}}, 0, nil
	case strings.HasPrefix(q, "INSERT INTO events.event"):
		return nil, nil, int64(len(a) / 6), f.insert(q, a)
	case strings.HasPrefix(q, "SELECT seq"):
		return f.selectRows(a)
	case strings.HasPrefix(q, "SELECT pg_notify"):
		s, err := strconv.ParseUint(a[1].(string), 10, 64)
		if err != nil {
			return nil, nil, 0, err
		}
		f.notices = append(f.notices, s)
		return []string{"pg_notify"}, [][]driver.Value{{nil}}, 0, nil
	case strings.HasPrefix(q, "LISTEN "):
		return nil, nil, 0, nil
	}
	return nil, nil, 0, fmt.Errorf("fakePG: unexpected statement %q", q)
}

func (f *fakePG) insert(q string, a []driver.Value) error {
	if !f.inTx {
		return errors.New("insert outside transaction")
	}
	for i := 0; i+5 < len(a); i += 6 {
		seq, ok := a[i].(int64)
		if !ok {
			return fmt.Errorf("fakePG: seq %v is not int64", a[i])
		}
		r := dbRow{seq: seq, topic: a[i+1].(string), kind: a[i+2].(string), ref: a[i+3].(string), emittedAt: a[i+5].(time.Time)}
		if p, ok := a[i+4].([]byte); ok {
			r.payload = p
		}
		if _, dup := f.rows[seq]; dup {
			return fmt.Errorf("duplicate key: seq %d", seq)
		}
		if _, dup := f.un[seq]; dup {
			return fmt.Errorf("duplicate key: seq %d", seq)
		}
		f.un[seq] = r
		f.seqReuse.Add(-1)
	}
	return nil
}

func (f *fakePG) selectRows(a []driver.Value) ([]string, [][]driver.Value, int64, error) {
	if !f.inTx {
		f.reads.Add(1)
	}
	cursor := a[0].(int64)
	topic := a[1].(string)
	limit := a[2].(int64)
	var seqs []int64
	for s := range f.rows {
		seqs = append(seqs, s)
	}
	// sort ascending
	for i := 1; i < len(seqs); i++ {
		for j := i; j > 0 && seqs[j] < seqs[j-1]; j-- {
			seqs[j], seqs[j-1] = seqs[j-1], seqs[j]
		}
	}
	cols := []string{"seq", "topic", "kind", "ref", "payload", "emitted_at"}
	var out [][]driver.Value
	for _, s := range seqs {
		r := f.rows[s]
		if s <= cursor || (r.topic != topic && !strings.HasPrefix(r.topic, topic+"/")) {
			continue
		}
		var p driver.Value
		if r.payload != nil {
			p = r.payload
		}
		out = append(out, []driver.Value{r.seq, r.topic, r.kind, r.ref, p, r.emittedAt})
		if int64(len(out)) == limit {
			break
		}
	}
	return cols, out, 0, nil
}

func (f *fakePG) committed() []dbRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	var seqs []int64
	for s := range f.rows {
		seqs = append(seqs, s)
	}
	for i := 1; i < len(seqs); i++ {
		for j := i; j > 0 && seqs[j] < seqs[j-1]; j-- {
			seqs[j], seqs[j-1] = seqs[j-1], seqs[j]
		}
	}
	out := make([]dbRow, len(seqs))
	for i, s := range seqs {
		out[i] = f.rows[s]
	}
	return out
}

func (f *fakePG) noticeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.notices)
}

// collect receives n events or fails after d.
func collect(t *testing.T, sub *Subscription, n int, d time.Duration) []Event {
	t.Helper()
	var got []Event
	deadline := time.After(d)
	for len(got) < n {
		select {
		case e, ok := <-sub.Events:
			if !ok {
				select {
				case err := <-sub.Errors:
					t.Fatalf("subscription ended after %d events: %v", len(got), err)
				default:
					t.Fatalf("subscription closed after %d events", len(got))
				}
			}
			got = append(got, e)
		case err := <-sub.Errors:
			t.Fatalf("subscription error after %d events: %v", len(got), err)
		case <-deadline:
			t.Fatalf("timed out after %d of %d events: %+v", len(got), n, got)
		}
	}
	return got
}

// quiet returns once sub has delivered everything stored up to now: it
// waits until a read finds nothing and the producer goes idle.
func quiet(t *testing.T, pg *fakePG, sub *Subscription) {
	t.Helper()
	for {
		n := pg.reads.Load()
		select {
		case e, ok := <-sub.Events:
			if !ok {
				t.Fatalf("subscription ended")
			}
			t.Fatalf("unexpected event %+v", e)
		case <-time.After(wait + 20*time.Millisecond):
			if pg.reads.Load() != n {
				continue // still reading, or read before the wait began
			}
			return
		}
	}
}
