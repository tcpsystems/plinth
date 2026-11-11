// Package page implements cursor pagination: a bounded page_size and an opaque,
// HMAC-signed page_token carrying a small JSON cursor.
//
// Response envelope shape: {"items": [...], "next_page_token": "..."}.
// next_page_token is omitted when there is no next page.
package page

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/tcpsystems/plinth/problem"
)

// Size limits for the page_size query parameter.
const (
	DefaultSize = 50
	MaxSize     = 200
	maxToken    = 1024
)

// Signer encodes and verifies page tokens.
type Signer struct{ key []byte }

// NewSigner requires a key of at least 16 bytes.
func NewSigner(key []byte) (*Signer, error) {
	if len(key) < 16 {
		return nil, errors.New("page: signing key must be at least 16 bytes")
	}
	return &Signer{key: append([]byte(nil), key...)}, nil
}

func (s *Signer) mac(b []byte) []byte {
	m := hmac.New(sha256.New, s.key)
	m.Write(b)
	return m.Sum(nil)
}

// Encode returns base64url(json || hmac-sha256(json)).
func (s *Signer) Encode(cursor any) (string, error) {
	b, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(append(b, s.mac(b)...)), nil
}

// Decode verifies token and unmarshals its cursor into v. Any failure is an
// ErrInvalid problem error with code invalid_page_token.
func (s *Signer) Decode(token string, v any) error {
	bad := problem.New(problem.ErrInvalid, "invalid_page_token", "page_token is not valid")
	if len(token) > maxToken {
		return bad
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) <= sha256.Size {
		return bad
	}
	body, sig := raw[:len(raw)-sha256.Size], raw[len(raw)-sha256.Size:]
	if !hmac.Equal(sig, s.mac(body)) || json.Unmarshal(body, v) != nil {
		return bad
	}
	return nil
}

// Size reads page_size from the request: DefaultSize when absent, otherwise
// 1..MaxSize or an ErrInvalid problem error with code invalid_page_size.
func Size(r *http.Request) (int, error) {
	v := r.URL.Query().Get("page_size")
	if v == "" {
		return DefaultSize, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > MaxSize {
		return 0, problem.New(problem.ErrInvalid, "invalid_page_size", "page_size must be between 1 and "+strconv.Itoa(MaxSize))
	}
	return n, nil
}

// Parse reads page_size and, when page_token is present, decodes it into
// cursor. first reports that no token was supplied (cursor untouched).
func Parse(r *http.Request, s *Signer, cursor any) (size int, first bool, err error) {
	if size, err = Size(r); err != nil {
		return 0, false, err
	}
	tok := r.URL.Query().Get("page_token")
	if tok == "" {
		return size, true, nil
	}
	return size, false, s.Decode(tok, cursor)
}

// Trim implements the fetch-size+1 pattern: query with Limit(size), pass the
// rows here, and more reports that a next page exists.
func Trim[T any](rows []T, size int) (items []T, more bool) {
	if len(rows) > size {
		return rows[:size], true
	}
	return rows, false
}

// Limit is the LIMIT value to use in a keyset query (size+1).
func Limit(size int) int { return size + 1 }

var ident = regexp.MustCompile(`^[a-z_][a-z0-9_.]*$`)

// Keyset returns a row-value comparison for ascending keyset pagination, for
// example Keyset(1, "created_at", "id") = "(created_at, id) > ($1, $2)". Pair
// it with ORDER BY created_at, id. Column names must be plain identifiers; it
// panics otherwise, since they come from code, never from requests.
func Keyset(firstArg int, cols ...string) string {
	if len(cols) == 0 {
		panic("page: Keyset needs columns")
	}
	args := make([]string, len(cols))
	for i, c := range cols {
		if !ident.MatchString(c) {
			panic("page: bad column name " + c)
		}
		args[i] = "$" + strconv.Itoa(firstArg+i)
	}
	return "(" + strings.Join(cols, ", ") + ") > (" + strings.Join(args, ", ") + ")"
}

// Response is the standard pagination envelope. Items holds the page contents.
// NextPageToken is the opaque token for the next page; it is omitted from JSON
// when empty (no next page).
type Response[T any] struct {
	Items         []T    `json:"items"`
	NextPageToken string `json:"next_page_token,omitempty"`
}

// Write writes the pagination response as JSON with the correct content type.
// It sets Cache-Control: no-store so paginated lists are not cached by
// intermediaries.
func Write[T any](w http.ResponseWriter, r *http.Request, resp Response[T]) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(resp)
}
