// Package settings loads runtime settings from the setting table through an
// interface, with an in-process cache and a hook for LISTEN/NOTIFY delivery.
// Environment variables are for bootstrap only, see [Bootstrap].
package settings

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/tcpsystems/plinth/config"
)

// ErrNotFound is returned for a key with no row.
var ErrNotFound = errors.New("settings: not found")

// Setting is one row of the setting table.
type Setting struct {
	Key     string
	Value   json.RawMessage
	Version int64
}

// Source reads settings; SQLSource is the database implementation.
type Source interface {
	Load(ctx context.Context, key string) (Setting, error)
}

// Listener delivers LISTEN notifications. The caller owns the dedicated
// connection (this module has no driver); Listen must return a channel of
// NOTIFY payloads that closes when ctx ends or the connection drops.
type Listener interface {
	Listen(ctx context.Context, channel string) (<-chan string, error)
}

// Loader caches settings for TTL (default 30s) and is safe for concurrent use.
type Loader struct {
	src      Source
	ttl      time.Duration
	now      func() time.Time
	listener Listener
	mu       sync.Mutex
	cache    map[string]entry
}

type entry struct {
	s       Setting
	expires time.Time
}

// New returns a Loader. listener may be nil when Notify is not used.
func New(src Source, ttl time.Duration, listener Listener) *Loader {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &Loader{src: src, ttl: ttl, now: time.Now, listener: listener, cache: map[string]entry{}}
}

// Get returns the cached setting or loads it.
func (l *Loader) Get(ctx context.Context, key string) (Setting, error) {
	l.mu.Lock()
	e, ok := l.cache[key]
	l.mu.Unlock()
	if ok && l.now().Before(e.expires) {
		return e.s, nil
	}
	s, err := l.src.Load(ctx, key)
	if err != nil {
		return Setting{}, err
	}
	l.mu.Lock()
	l.cache[key] = entry{s, l.now().Add(l.ttl)}
	l.mu.Unlock()
	return s, nil
}

// GetInto loads key and unmarshals its JSON value into v.
func (l *Loader) GetInto(ctx context.Context, key string, v any) error {
	s, err := l.Get(ctx, key)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(s.Value, v); err != nil {
		return fmt.Errorf("settings: key %q: %w", key, err)
	}
	return nil
}

// Invalidate drops key, or the whole cache when key is empty.
func (l *Loader) Invalidate(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if key == "" {
		clear(l.cache)
		return
	}
	delete(l.cache, key)
}

// Notify consumes notifications from the caller-supplied Listener on channel
// and invalidates the key named by each payload (an empty payload flushes the
// cache). It blocks until ctx ends or the notification stream closes, and
// returns ctx's error in the first case. After the stream closes the whole cache
// is flushed, since notifications may have been missed; callers reconnect by
// calling Notify again.
func (l *Loader) Notify(ctx context.Context, channel string) error {
	if l.listener == nil {
		return errors.New("settings: no Listener configured")
	}
	ch, err := l.listener.Listen(ctx, channel)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			l.Invalidate("")
			return ctx.Err()
		case key, ok := <-ch:
			if !ok {
				l.Invalidate("")
				return errors.New("settings: notification stream closed")
			}
			l.Invalidate(key)
		}
	}
}

// SQLSource reads the setting table through database/sql (PostgreSQL syntax).
type SQLSource struct{ DB *sql.DB }

// Load implements Source.
func (s SQLSource) Load(ctx context.Context, key string) (Setting, error) {
	out := Setting{Key: key}
	var raw []byte
	err := s.DB.QueryRowContext(ctx, `SELECT value, version FROM setting WHERE key = $1`, key).Scan(&raw, &out.Version)
	if errors.Is(err, sql.ErrNoRows) {
		return Setting{}, ErrNotFound
	}
	if err != nil {
		return Setting{}, err
	}
	out.Value = json.RawMessage(raw)
	return out, nil
}

// Bootstrap reads only the named environment variables (through the injectable
// lookup, normally os.Getenv). Everything else belongs in the setting table.
func Bootstrap(get config.Lookup, names ...string) map[string]string {
	out := make(map[string]string, len(names))
	for _, n := range names {
		out[n] = get(n)
	}
	return out
}
