package queue

import (
	"context"
	"fmt"
)

// ClaimN claims up to n due rows, in one statement and therefore one
// transaction: highest priority first (lowest priority value, then earliest
// next_due, then lowest id). Each returned row has state = Claimed and an
// epoch one larger than before.
//
// Rows another claimer holds at the same moment are skipped, so concurrent
// claimers on one table get disjoint batches without waiting on each other.
// ClaimN returns fewer than n rows, possibly zero, when fewer than n rows are
// due. n is clamped to 1..MaxBatch; the returned slice is not ordered.
func (q *Queue) ClaimN(ctx context.Context, n int) ([]Claimed, error) {
	if n < 1 {
		n = 1
	}
	if n > MaxBatch {
		n = MaxBatch
	}
	rows, err := q.db.QueryContext(ctx, q.claim, n)
	if err != nil {
		return nil, fmt.Errorf("queue: claim: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Claimed
	for rows.Next() {
		var c Claimed
		if err := rows.Scan(&c.ID, &c.Epoch); err != nil {
			return nil, fmt.Errorf("queue: claim: scan: %w", err)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue: claim: %w", err)
	}
	return out, nil
}
