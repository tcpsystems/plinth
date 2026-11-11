package outbox

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/tcpsystems/plinth/internal/fakesql"
)

type call struct {
	q string
	a []driver.Value
}

func TestWriteUsesCallerTx(t *testing.T) {
	var log []call
	db := fakesql.Open(func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		log = append(log, call{q, a})
		return nil, nil, 1, nil
	})
	tx, err := db.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := Write(t.Context(), tx, Event{Topic: "card.created", Key: "c1", Payload: []byte(`{"a":1}`), Headers: map[string]string{"k": "v"}})
	if err != nil || len(id) != 32 {
		t.Fatal(id, err)
	}
	for _, l := range log {
		if l.q == "COMMIT" {
			t.Fatal("Write must not commit")
		}
	}
	ins := log[len(log)-1]
	if !strings.HasPrefix(ins.q, "INSERT INTO outbox") || ins.a[0] != id || ins.a[1] != "card.created" || string(ins.a[3].([]byte)) != `{"a":1}` || string(ins.a[4].([]byte)) != `{"k":"v"}` {
		t.Fatalf("%+v", ins)
	}
	_ = tx.Rollback()
	if log[len(log)-1].q != "ROLLBACK" {
		t.Fatal("caller owns rollback")
	}
}

func TestWriteValidation(t *testing.T) {
	db := fakesql.Open(func(string, []driver.Value) ([]string, [][]driver.Value, int64, error) { return nil, nil, 1, nil })
	tx, _ := db.BeginTx(t.Context(), nil)
	for _, e := range []Event{{Payload: []byte(`{}`)}, {Topic: "t", Payload: []byte(`{bad`)}, {Topic: "t"}} {
		if _, err := Write(t.Context(), tx, e); err == nil {
			t.Errorf("accepted %+v", e)
		}
	}
	if _, err := Write(t.Context(), nil, Event{Topic: "t", Payload: []byte(`1`)}); err == nil {
		t.Error("nil tx accepted")
	}
	failing := fakesql.Open(func(string, []driver.Value) ([]string, [][]driver.Value, int64, error) {
		return nil, nil, 0, errors.New("duplicate key value violates unique constraint")
	})
	tx2, _ := failing.BeginTx(t.Context(), nil)
	if _, err := Write(t.Context(), tx2, Event{Topic: "t", Payload: []byte(`1`)}); err == nil {
		t.Error("exec error swallowed")
	}
}

type pub struct {
	got  []string
	fail string
}

func (p *pub) Publish(_ context.Context, e Event) error {
	if e.ID == p.fail {
		return errors.New("broker down")
	}
	p.got = append(p.got, e.ID)
	return nil
}

func relayDB(log *[]call) (h func(string, []driver.Value) ([]string, [][]driver.Value, int64, error)) {
	return func(q string, a []driver.Value) ([]string, [][]driver.Value, int64, error) {
		*log = append(*log, call{q, a})
		if strings.HasPrefix(q, "SELECT") {
			return []string{"id", "topic", "key", "payload", "headers"}, [][]driver.Value{
				{"e1", "t", "k", []byte(`1`), []byte(`{}`)}, {"e2", "t", "k", []byte(`2`), []byte(`{"h":"v"}`)}, {"e3", "t", "", []byte(`3`), []byte(`{}`)},
			}, 0, nil
		}
		return nil, nil, 1, nil
	}
}

func TestRelayPublishesAndMarks(t *testing.T) {
	var log []call
	p := &pub{}
	n, err := SQLRelay{DB: fakesql.Open(relayDB(&log)), Pub: p}.Poll(t.Context(), 10)
	if err != nil || n != 3 || len(p.got) != 3 {
		t.Fatal(n, err, p.got)
	}
	marks := 0
	for _, l := range log {
		if strings.Contains(l.q, "published_at = now()") {
			marks++
		}
	}
	if marks != 3 || log[len(log)-1].q != "COMMIT" || !strings.Contains(log[1].q, "SKIP LOCKED") {
		t.Fatal(marks, log)
	}
}

func TestRelayStopsAtFailureAndKeepsEarlier(t *testing.T) {
	var log []call
	p := &pub{fail: "e2"}
	var r Relay = SQLRelay{DB: fakesql.Open(relayDB(&log)), Pub: p}
	n, err := r.Poll(t.Context(), 10)
	if n != 1 || err == nil || len(p.got) != 1 {
		t.Fatal(n, err)
	}
	if log[len(log)-1].q != "COMMIT" {
		t.Fatal("progress before the failure must commit")
	}
	attempts := 0
	for _, l := range log {
		if strings.Contains(l.q, "attempts = attempts + 1") && l.a[0] == "e2" {
			attempts++
		}
	}
	if attempts != 1 {
		t.Fatal("attempt not counted")
	}
}
