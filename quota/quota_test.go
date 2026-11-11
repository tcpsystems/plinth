package quota

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/internal/fakesql"
)

type memLedgerDB struct {
	mu      sync.Mutex
	caps    map[string]Cap
	entries map[string]int64
}

func newMemLedgerDB() *memLedgerDB {
	return &memLedgerDB{
		caps:    make(map[string]Cap),
		entries: make(map[string]int64),
	}
}

func (m *memLedgerDB) handler(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch {
	case strings.Contains(q, "CREATE TABLE IF NOT EXISTS quota_cap"):
		return nil, nil, 0, nil

	case strings.HasPrefix(q, "INSERT INTO quota_cap"):
		acc := fmt.Sprint(a[0])
		con := fmt.Sprint(a[1])
		winS, _ := a[2].(int64)
		limit, _ := a[3].(int64)
		reason := fmt.Sprint(a[4])
		m.caps[acc+"|"+con] = Cap{
			AccountID:     acc,
			Concern:       con,
			WindowSeconds: int(winS),
			Limit:         limit,
			Reason:        reason,
		}
		return nil, nil, 1, nil

	case strings.HasPrefix(q, "SELECT window_s, cap, reason FROM quota_cap"):
		acc := fmt.Sprint(a[0])
		con := fmt.Sprint(a[1])
		c, ok := m.caps[acc+"|"+con]
		if !ok {
			return []string{"window_s", "cap", "reason"}, nil, 0, nil
		}
		return []string{"window_s", "cap", "reason"}, [][]driver.Value{
			{int64(c.WindowSeconds), c.Limit, c.Reason},
		}, 0, nil

	case strings.HasPrefix(q, "INSERT INTO quota_ledger AS l") && strings.Contains(q, "SELECT cap FROM quota_cap"):
		acc := fmt.Sprint(a[0])
		con := fmt.Sprint(a[1])
		ws, _ := a[2].(time.Time)
		units, _ := a[3].(int64)
		c, ok := m.caps[acc+"|"+con]
		if !ok {
			return []string{"units"}, nil, 0, nil
		}
		k := fmt.Sprintf("%s|%s|%d", acc, con, ws.UTC().UnixNano())
		cur := m.entries[k]
		if cur+units > c.Limit {
			return []string{"units"}, nil, 0, nil
		}
		m.entries[k] = cur + units
		return []string{"units"}, [][]driver.Value{{m.entries[k]}}, 1, nil

	case strings.HasPrefix(q, "INSERT INTO quota_ledger AS l"):
		acc := fmt.Sprint(a[0])
		con := fmt.Sprint(a[1])
		ws, _ := a[2].(time.Time)
		units, _ := a[3].(int64)
		capLimit, _ := a[4].(int64)
		k := fmt.Sprintf("%s|%s|%d", acc, con, ws.UTC().UnixNano())
		cur := m.entries[k]
		if cur+units > capLimit {
			return []string{"units"}, nil, 0, nil
		}
		m.entries[k] = cur + units
		return []string{"units"}, [][]driver.Value{{m.entries[k]}}, 1, nil

	case strings.HasPrefix(q, "SELECT units FROM quota_ledger"):
		acc := fmt.Sprint(a[0])
		con := fmt.Sprint(a[1])
		ws, _ := a[2].(time.Time)
		k := fmt.Sprintf("%s|%s|%d", acc, con, ws.UTC().UnixNano())
		u, ok := m.entries[k]
		if !ok {
			return []string{"units"}, nil, 0, nil
		}
		return []string{"units"}, [][]driver.Value{{u}}, 0, nil

	case strings.HasPrefix(q, "DELETE FROM quota_ledger"):
		cutoff, _ := a[0].(time.Time)
		var deleted int64
		for k := range m.entries {
			parts := strings.Split(k, "|")
			if len(parts) == 3 {
				var ns int64
				_, _ = fmt.Sscanf(parts[2], "%d", &ns)
				if time.Unix(0, ns).UTC().Before(cutoff.UTC()) {
					delete(m.entries, k)
					deleted++
				}
			}
		}
		return nil, nil, deleted, nil

	default:
		return nil, nil, 0, fmt.Errorf("unexpected query: %s", q)
	}
}

func TestAdmitAtomicAndRefusal(t *testing.T) {
	mem := newMemLedgerDB()
	db := fakesql.Open(mem.handler)
	fixedNow := time.Date(2026, 11, 11, 12, 30, 0, 0, time.UTC)

	ledger := NewLedger(db)
	ledger.Window = time.Hour
	ledger.Now = func() time.Time { return fixedNow }

	ctx := context.Background()
	if err := ledger.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	// 1. First spend of 40 against cap 100 succeeds.
	d1, err := ledger.Admit(ctx, "acct-1", "search.queries", 40, 100)
	if err != nil {
		t.Fatalf("Admit 1: %v", err)
	}
	if !d1.Allowed || d1.Used != 40 || d1.Remaining != 60 {
		t.Fatalf("unexpected decision 1: %+v", d1)
	}

	// 2. Second spend of 60 reaches exact cap 100 and succeeds.
	d2, err := ledger.Admit(ctx, "acct-1", "search.queries", 60, 100)
	if err != nil {
		t.Fatalf("Admit 2: %v", err)
	}
	if !d2.Allowed || d2.Used != 100 || d2.Remaining != 0 {
		t.Fatalf("unexpected decision 2: %+v", d2)
	}

	// 3. Third spend of 1 exceeds cap 100 and is refused cleanly.
	d3, err := ledger.Admit(ctx, "acct-1", "search.queries", 1, 100)
	if err != nil {
		t.Fatalf("Admit 3: %v", err)
	}
	if d3.Allowed || d3.Remaining != 0 {
		t.Fatalf("expected refusal at cap, got %+v", d3)
	}

	// 4. Usage remains 100 (no partial overspend).
	u, err := ledger.Usage(ctx, "acct-1", "search.queries", WindowStart(fixedNow, time.Hour))
	if err != nil {
		t.Fatalf("Usage: %v", err)
	}
	if u != 100 {
		t.Fatalf("expected usage 100, got %d", u)
	}
}

func TestAdmitExemptAndValidation(t *testing.T) {
	mem := newMemLedgerDB()
	db := fakesql.Open(mem.handler)
	ledger := NewLedger(db)
	ledger.Exempt = func(id string) bool {
		return id == "batch-importer" || id == "internal-sync"
	}

	ctx := context.Background()
	d, err := ledger.Admit(ctx, "batch-importer", "search.queries", 500, 10)
	if err != nil {
		t.Fatalf("exempt Admit: %v", err)
	}
	if !d.Allowed || !d.Exempt {
		t.Fatalf("expected exempt allowed decision, got %+v", d)
	}

	// Single request exceeding cap for non-exempt account is refused.
	dOver, err := ledger.Admit(ctx, "user-1", "search.queries", 50, 10)
	if err != nil {
		t.Fatalf("over-cap Admit: %v", err)
	}
	if dOver.Allowed {
		t.Fatalf("expected over-cap refusal, got %+v", dOver)
	}

	// Invalid arguments return ErrInvalid.
	if _, err := ledger.Admit(ctx, "", "search.queries", 1, 10); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid for empty account, got %v", err)
	}
	if _, err := ledger.Admit(ctx, "user-1", "search.queries", 0, 10); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected ErrInvalid for zero units, got %v", err)
	}
}

func TestCapTableAndPrune(t *testing.T) {
	mem := newMemLedgerDB()
	db := fakesql.Open(mem.handler)
	ledger := NewLedger(db)
	ctx := context.Background()

	if err := ledger.SetCap(ctx, Cap{
		AccountID:     "acct-cap",
		Concern:       "tokens",
		WindowSeconds: 3600,
		Limit:         50,
		Reason:        "tier-standard",
	}); err != nil {
		t.Fatalf("SetCap: %v", err)
	}

	gotCap, err := ledger.GetCap(ctx, "acct-cap", "tokens")
	if err != nil {
		t.Fatalf("GetCap: %v", err)
	}
	if gotCap.Limit != 50 || gotCap.Reason != "tier-standard" {
		t.Fatalf("unexpected cap: %+v", gotCap)
	}

	_, err = ledger.GetCap(ctx, "missing", "tokens")
	if !errors.Is(err, ErrCapNotFound) {
		t.Fatalf("expected ErrCapNotFound, got %v", err)
	}

	ws := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	used, ok, err := ledger.AdmitFromCap(ctx, "acct-cap", "tokens", ws, 30)
	if err != nil || !ok || used != 30 {
		t.Fatalf("AdmitFromCap 1: used=%d ok=%v err=%v", used, ok, err)
	}
	_, ok, err = ledger.AdmitFromCap(ctx, "acct-cap", "tokens", ws, 25)
	if err != nil || ok {
		t.Fatalf("AdmitFromCap 2 expected refusal, got ok=%v err=%v", ok, err)
	}

	pruned, err := ledger.PruneBefore(ctx, ws.Add(time.Hour))
	if err != nil || pruned != 1 {
		t.Fatalf("PruneBefore: pruned=%d err=%v", pruned, err)
	}
}

func TestConcurrentRaceSafety(t *testing.T) {
	mem := newMemLedgerDB()
	db := fakesql.Open(mem.handler)
	ledger := NewLedger(db)
	ctx := context.Background()

	const workers = 20
	const capLimit = 10
	var wg sync.WaitGroup
	allowedCount := make(chan int, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := ledger.Admit(ctx, "race-acct", "concurrent", 1, capLimit)
			if err != nil {
				t.Errorf("Admit error: %v", err)
				return
			}
			if d.Allowed {
				allowedCount <- 1
			}
		}()
	}
	wg.Wait()
	close(allowedCount)

	totalAllowed := 0
	for n := range allowedCount {
		totalAllowed += n
	}
	if totalAllowed != capLimit {
		t.Fatalf("expected exactly %d allowed admissions under race, got %d", capLimit, totalAllowed)
	}
}

func TestNilDatabaseErrors(t *testing.T) {
	var l *Ledger
	ctx := context.Background()
	if err := l.Migrate(ctx); err == nil {
		t.Error("expected error on nil ledger Migrate")
	}
	if _, err := (&Ledger{}).Admit(ctx, "a", "c", 1, 1); err == nil {
		t.Error("expected error on nil DB Admit")
	}
	if _, _, err := (&Ledger{}).AdmitFromCap(ctx, "a", "c", time.Now(), 1); err == nil {
		t.Error("expected error on nil DB AdmitFromCap")
	}
	if err := (&Ledger{}).SetCap(ctx, Cap{AccountID: "a", Concern: "c", Limit: 1}); err == nil {
		t.Error("expected error on nil DB SetCap")
	}
	if _, err := (&Ledger{}).GetCap(ctx, "a", "c"); err == nil {
		t.Error("expected error on nil DB GetCap")
	}
	if _, err := (&Ledger{}).Usage(ctx, "a", "c", time.Now()); err == nil {
		t.Error("expected error on nil DB Usage")
	}
	if _, err := (&Ledger{}).PruneBefore(ctx, time.Now()); err == nil {
		t.Error("expected error on nil DB PruneBefore")
	}
	// Ensure sql package reference is exercised.
	_ = sql.ErrNoRows
}
