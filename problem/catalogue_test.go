package problem

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
)

// The Go catalogue must equal errors.yaml.
func TestCatalogueMatchesYAML(t *testing.T) {
	b, err := os.ReadFile("errors.yaml")
	if err != nil {
		t.Fatal(err)
	}
	re := regexp.MustCompile(`slug: (\w+), status: (\d+), title: ([^,]+),`)
	m := re.FindAllStringSubmatch(string(b), -1)
	if len(m) != len(Catalogue) {
		t.Fatalf("yaml has %d entries, Go has %d", len(m), len(Catalogue))
	}
	for i, e := range Catalogue {
		st, _ := strconv.Atoi(m[i][2])
		if m[i][1] != e.Slug || st != e.Status || m[i][3] != e.Title {
			t.Errorf("entry %d: yaml %v != go %v", i, m[i][1:], e)
		}
	}
	if Type("x") != fmt.Sprint(TypeBase, "x") {
		t.Fatal("Type")
	}
}

func TestDefaultsAreCatalogued(t *testing.T) {
	for _, err := range []error{ErrNotFound, ErrConflict, ErrInvalid, ErrUnauthorized, ErrForbidden, ErrRateLimited, nil} {
		p := Wrap(err)
		ok := false
		for _, e := range Catalogue {
			if e.Slug == p.Code && e.Status == p.Status {
				ok = true
			}
		}
		if !ok {
			t.Errorf("%v -> %s/%d not in catalogue", err, p.Code, p.Status)
		}
	}
}
