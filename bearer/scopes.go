package bearer

import (
	"net/http"
	"strings"

	"github.com/tcpsystems/plinth/problem"
)

// ParseScopes splits a space-delimited scope claim (OAuth `scope`).
func ParseScopes(s string) []string { return strings.Fields(s) }

// HasScope reports whether granted covers need. Scopes are
// <product>:<resource>:<action>; a granted "*" in the action or resource
// position ("search:*:*", "search:queries:*") covers everything below it.
func HasScope(granted []string, need string) bool {
	n := strings.Split(need, ":")
	for _, g := range granted {
		if g == need || g == "*" {
			return true
		}
		p := strings.Split(g, ":")
		if len(p) != len(n) || len(n) != 3 {
			continue
		}
		ok := true
		for i := range p {
			if p[i] != n[i] && p[i] != "*" {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// Require answers 403 forbidden problem+json when granted lacks need and
// returns false; it returns true when the caller may proceed.
func Require(w http.ResponseWriter, r *http.Request, granted []string, need string) bool {
	if HasScope(granted, need) {
		return true
	}
	problem.Write(w, r, problem.Problem{
		Type: problem.Type(problem.CodeForbidden), Title: "Forbidden", Status: http.StatusForbidden,
		Code: problem.CodeForbidden, Detail: "missing scope " + need,
	})
	return false
}
