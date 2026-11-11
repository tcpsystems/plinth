package postgres

// sessionDialer is the wire-level *pgConn kept for the direct TCP backend
// (see wire.go). Container sessions use backend.run instead.

import (
	"bytes"
	"context"
	"net"
	"strings"
)

// connectSession opens a wire session over TCP and completes the startup
// handshake.
func connectSession(ctx context.Context, addr, user, password, database string) (*pgConn, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c, err := handshake(conn, user, password, database)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return c, nil
}

// run executes one statement and returns its rows in text format.
func (c *pgConn) run(ctx context.Context, sql string) ([]string, error) {
	rows, err := c.exec(sql)
	var lines []string
	for _, r := range rows {
		cells := make([]string, len(r))
		for i, v := range r {
			if v == nil {
				cells[i] = `\N`
			} else {
				cells[i] = escapeCell(*v)
			}
		}
		lines = append(lines, strings.Join(cells, "\t"))
	}
	return lines, err
}

func escapeCell(s string) string {
	if !strings.ContainsAny(s, "\\\t\n\r") {
		return s
	}
	var b bytes.Buffer
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '\t':
			b.WriteString(`\t`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// parseRow splits a client row (psql unaligned conventions) into cells.
func parseRow(line string) []string {
	cells := strings.Split(line, "\t")
	for i, c := range cells {
		if c != `\N` {
			cells[i] = unescape(c)
		}
	}
	return cells
}

func unescape(s string) string {
	if !strings.Contains(s, `\`) {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) {
			i++
			switch s[i] {
			case 't':
				b.WriteByte('\t')
			case 'n':
				b.WriteByte('\n')
			case 'r':
				b.WriteByte('\r')
			default:
				b.WriteByte(s[i])
			}
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
}

// QueryError is a statement the server rejected. Detail is the server's
// message, for test logs and helper errors only.
type QueryError struct {
	sql    string
	detail string
}

func (e *QueryError) Error() string { return "postgres: " + e.detail }

// SQL exposes the statement that failed.
func (e *QueryError) SQL() string { return e.sql }
