package queue

import (
	"context"
	"fmt"
	"time"
)

// Settle marks the row done. It succeeds only while the row is still claimed
// at epoch; otherwise it returns ErrStale and changes nothing. A successful
// Settle retires the lease, so a second Settle of the same id and epoch is
// ErrStale.
func (q *Queue) Settle(ctx context.Context, id, epoch int64) error {
	return q.update(ctx, "settle", q.settle, id, epoch)
}

// Fail returns the row to pending so it is claimed again after backoff, and
// retires the lease. It succeeds only while the row is still claimed at epoch;
// otherwise it returns ErrStale and changes nothing. A negative backoff is
// treated as zero, which makes the row due immediately.
func (q *Queue) Fail(ctx context.Context, id, epoch int64, backoff time.Duration) error {
	secs := backoff.Seconds()
	if secs < 0 {
		secs = 0
	}
	return q.update(ctx, "fail", q.fail, id, epoch, secs)
}

func (q *Queue) update(ctx context.Context, op, stmt string, args ...any) error {
	res, err := q.db.ExecContext(ctx, stmt, args...)
	if err != nil {
		return fmt.Errorf("queue: %s: %w", op, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("queue: %s: rows affected: %w", op, err)
	}
	if n == 0 {
		return fmt.Errorf("queue: %s row %d epoch %d: %w", op, args[0], args[1], ErrStale)
	}
	return nil
}
