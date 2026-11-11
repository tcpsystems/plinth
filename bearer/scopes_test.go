package bearer

import (
	"net/http/httptest"
	"testing"
)

func TestHasScope(t *testing.T) {
	cases := []struct {
		g    []string
		need string
		want bool
	}{
		{[]string{"search:queries:create"}, "search:queries:create", true},
		{[]string{"search:queries:read"}, "search:queries:create", false},
		{[]string{"search:queries:*"}, "search:queries:create", true},
		{[]string{"search:*:*"}, "search:keys:delete", true},
		{[]string{"dns:*:*"}, "search:keys:delete", false},
		{nil, "a:b:c", false},
	}
	for _, c := range cases {
		if HasScope(c.g, c.need) != c.want {
			t.Errorf("%v %s", c.g, c.need)
		}
	}
}

func TestRequire(t *testing.T) {
	w := httptest.NewRecorder()
	if Require(w, httptest.NewRequest("GET", "/", nil), ParseScopes("a:b:read"), "a:b:write") {
		t.Fatal("allowed")
	}
	if w.Code != 403 || w.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatal(w.Code)
	}
}
