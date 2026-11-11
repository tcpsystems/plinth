// Package problem writes RFC 9457 problem+json responses with stable codes.
//
// Responses never contain SQL, stack traces or wrapped error text: the detail
// shown to clients is either a generic per-kind message or the explicit text
// the handler supplied through [New].
package problem

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/tcpsystems/plinth/reqid"
)

// TypeBase is the prefix of every problem type URI.
const TypeBase = "https://tcp.systems/problems/"

// ContentType is the RFC 9457 media type.
const ContentType = "application/problem+json"

// Sentinel kinds. Wrap them with [New] or fmt.Errorf("%w") so [Wrap] can map
// them to a status. A resource owned by another tenant must be reported with
// ErrNotFound, never ErrForbidden, so ids cannot be probed.
var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("conflict")
	ErrInvalid      = errors.New("invalid")
	ErrUnauthorized = errors.New("unauthorized")
	ErrForbidden    = errors.New("forbidden")
	ErrRateLimited  = errors.New("rate limited")
)

// Problem is the wire form. Extension members are code and trace_id.
type Problem struct {
	Type     string `json:"type"`
	Title    string `json:"title"`
	Status   int    `json:"status"`
	Detail   string `json:"detail,omitempty"`
	Instance string `json:"instance,omitempty"`
	Code     string `json:"code"`
	TraceID  string `json:"trace_id,omitempty"`
}

// Error is a client-safe error: Kind selects the status, Code overrides the
// default stable code and Detail is shown to the client verbatim.
type Error struct {
	Kind   error
	Code   string
	Detail string
	Status int // optional override of the kind's status, for example 422
}

// Error returns the client-safe problem detail.
func (e *Error) Error() string { return e.Detail }

// Unwrap returns the underlying sentinel error kind.
func (e *Error) Unwrap() error { return e.Kind }

// New builds a client-safe error of the given sentinel kind. code may be empty
// to use the kind's default. Detail must not contain internal text.
func New(kind error, code, detail string) error {
	return &Error{Kind: kind, Code: code, Detail: detail}
}

type meta struct {
	status int
	code   string
	title  string
}

func lookup(err error) meta {
	switch {
	case errors.Is(err, ErrNotFound):
		return meta{http.StatusNotFound, "not_found", "Not Found"}
	case errors.Is(err, ErrConflict):
		return meta{http.StatusConflict, "conflict", "Conflict"}
	case errors.Is(err, ErrInvalid):
		return meta{http.StatusBadRequest, "invalid", "Invalid Request"}
	case errors.Is(err, ErrUnauthorized):
		return meta{http.StatusUnauthorized, "unauthorized", "Unauthorized"}
	case errors.Is(err, ErrForbidden):
		return meta{http.StatusForbidden, "forbidden", "Forbidden"}
	case errors.Is(err, ErrRateLimited):
		return meta{http.StatusTooManyRequests, "rate_limited", "Too Many Requests"}
	}
	return meta{http.StatusInternalServerError, "internal", "Internal Server Error"}
}

// NewStatus is [New] with an explicit HTTP status overriding the kind's.
func NewStatus(status int, kind error, code, detail string) error {
	return &Error{Kind: kind, Code: code, Detail: detail, Status: status}
}

// Wrap maps err to a Problem. Unknown errors become a generic 500 with no
// detail, so database and library error text never reaches clients.
func Wrap(err error) Problem {
	m := lookup(err)
	p := Problem{Type: TypeBase + m.code, Title: m.title, Status: m.status, Code: m.code}
	var e *Error
	if errors.As(err, &e) {
		if e.Code != "" {
			p.Code, p.Type = e.Code, TypeBase+e.Code
		}
		p.Detail = e.Detail
		if e.Status != 0 {
			p.Status = e.Status
		}
	}
	return p
}

// TraceID extracts the trace id from a W3C traceparent header, or "".
func TraceID(r *http.Request) string {
	parts := strings.Split(r.Header.Get("Traceparent"), "-")
	if len(parts) != 4 || len(parts[0]) != 2 || len(parts[1]) != 32 || len(parts[2]) != 16 || len(parts[3]) != 2 {
		return ""
	}
	id := parts[1]
	zero := true
	for _, c := range id {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return ""
		}
		if c != '0' {
			zero = false
		}
	}
	if zero {
		return ""
	}
	return id
}

// Write sends p with Instance and TraceID filled from r.
func Write(w http.ResponseWriter, r *http.Request, p Problem) {
	if p.Instance == "" {
		p.Instance = reqid.FromContext(r.Context())
	}
	if p.TraceID == "" {
		p.TraceID = TraceID(r)
	}
	w.Header().Set("Content-Type", ContentType)
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

// WriteError wraps err with [Wrap] and writes it.
func WriteError(w http.ResponseWriter, r *http.Request, err error) { Write(w, r, Wrap(err)) }

// NotFoundHandler answers every request with a not_found problem; mount it on
// "/" so unmatched routes use the contract instead of plain text.
func NotFoundHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WriteError(w, r, New(ErrNotFound, "", "no such route"))
	})
}
