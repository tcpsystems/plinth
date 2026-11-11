package idem

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"time"
)

// MemoryStore is an in-process Store for tests and single-instance services.
type MemoryStore struct {
	mu sync.Mutex
	m  map[string]Record
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore { return &MemoryStore{m: map[string]Record{}} }

// Get implements Store.
func (s *MemoryStore) Get(_ context.Context, key string, now time.Time) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.m[key]
	if !ok || !r.ExpiresAt.After(now) {
		delete(s.m, key)
		return nil, nil
	}
	return &r, nil
}

// Put implements Store.
func (s *MemoryStore) Put(_ context.Context, rec Record, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.m[rec.Key]; ok && r.ExpiresAt.After(now) {
		return false, nil
	}
	s.m[rec.Key] = rec
	return true, nil
}

// SQLStore keeps records in the idempotency_key table (see schema.sql) using
// PostgreSQL placeholders. Any database/sql driver can be injected.
type SQLStore struct{ DB *sql.DB }

// Get implements Store.
func (s SQLStore) Get(ctx context.Context, key string, now time.Time) (*Record, error) {
	r := Record{Key: key}
	err := s.DB.QueryRowContext(ctx,
		`SELECT request_hash, status, content_type, body, expires_at FROM idempotency_key WHERE key = $1 AND expires_at > $2`,
		key, now).Scan(&r.RequestHash, &r.Status, &r.ContentType, &r.Body, &r.ExpiresAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// Put implements Store. An expired row under the same key is replaced.
func (s SQLStore) Put(ctx context.Context, rec Record, now time.Time) (bool, error) {
	res, err := s.DB.ExecContext(ctx,
		`INSERT INTO idempotency_key (key, request_hash, status, content_type, body, created_at, expires_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (key) DO UPDATE SET request_hash = EXCLUDED.request_hash, status = EXCLUDED.status,
  content_type = EXCLUDED.content_type, body = EXCLUDED.body, created_at = EXCLUDED.created_at, expires_at = EXCLUDED.expires_at
WHERE idempotency_key.expires_at <= $6`,
		rec.Key, rec.RequestHash, rec.Status, rec.ContentType, rec.Body, now, rec.ExpiresAt)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// Purge deletes expired rows; run it periodically.
func (s SQLStore) Purge(ctx context.Context, now time.Time) (int64, error) {
	res, err := s.DB.ExecContext(ctx, `DELETE FROM idempotency_key WHERE expires_at <= $1`, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
