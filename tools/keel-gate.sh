#!/usr/bin/env bash
# Gate for the keel module: formatting, vet, race tests, lint, dependency
# budget. The runnable leader example is the documented Example in
# keel/leader/example_test.go, so `go test` covers it.
#
# The keel service may depend on PostgreSQL drivers, River for its job queue and
# their small support modules; everything else in this repo stays standard library only, which the
# root tools/gate.sh enforces. Optional host tools: golangci-lint (v2) with jq.
set -euo pipefail
cd "$(dirname "$0")/.."

bad=$(gofmt -l .)
if [ -n "$bad" ]; then echo "gofmt needed:"; echo "$bad"; exit 1; fi
go vet ./...
# Tests against a real PostgreSQL run when KEEL_TEST_DSN is set; without it
# they skip. Use tools/testdb.sh go to provide one.
go test -race -count=1 ./...

# >>> dependency budget: pgx and what it pulls in, nothing else.
allowed=$(
    cat <<'EOF'
github.com/jackc/pgpassfile
github.com/jackc/pgservicefile
github.com/jackc/pgx/v5
github.com/jackc/puddle/v2
github.com/lib/pq
github.com/riverqueue/river
github.com/riverqueue/river/riverdriver
github.com/riverqueue/river/riverdriver/riverdatabasesql
github.com/riverqueue/river/riverdriver/riverpgxv5
github.com/riverqueue/river/rivershared
github.com/riverqueue/river/rivertype
github.com/tidwall/gjson
github.com/tidwall/match
github.com/tidwall/pretty
github.com/tidwall/sjson
golang.org/x/sync
golang.org/x/text
EOF
)
used=$(go list -deps -test -f '{{with .Module}}{{.Path}}{{end}}' ./... |
    sort -u | grep -vx 'github.com/tcpsystems/plinth/keel')
extra=$(while read -r mod; do
    grep -qxF -- "$mod" <<<"$allowed" || echo "$mod"
done <<<"$used")
if [ -n "$extra" ]; then
    echo "keel packages or tests use modules outside the keel dependency budget:"
    echo "$extra"
    exit 1
fi
echo "dependencies: $(tr '\n' ' ' <<<"$used")"
# <<< dependency budget

if command -v golangci-lint >/dev/null; then
    if ! command -v jq >/dev/null; then
        echo "ERROR: golangci-lint is installed but jq is required to parse its JSON report" >&2
        exit 1
    fi
    version=$(golangci-lint version --short 2>/dev/null || true)
    case "$version" in
        v2.* | 2.*) ;;
        *) echo "ERROR: golangci-lint v2 is required (found '${version:-unknown}')" >&2; exit 1 ;;
    esac
    dir=$(mktemp -d)
    trap 'rm -rf "$dir"' EXIT
    args=(run --config ../lint/golangci.base.yml --output.json.path="$dir/report.json" ./...)
    # Scope to new issues when a base revision exists, as the root gate does.
    for base in origin/main origin/HEAD origin/develop; do
        if git rev-parse --verify --quiet "${base}^{commit}" >/dev/null; then
            args=(run --config ../lint/golangci.base.yml --new-from-rev="$base" \
                --output.json.path="$dir/report.json" ./...)
            break
        fi
    done
    set +e
    golangci-lint "${args[@]}" >"$dir/stdout.log" 2>"$dir/stderr.log"
    rc=$?
    set -e
    if ! { [ -s "$dir/report.json" ] && jq -e '.Issues | type == "array"' "$dir/report.json" >/dev/null 2>&1; }; then
        echo "ERROR: golangci-lint exited $rc without a parseable JSON report" >&2
        cat "$dir/stderr.log" >&2 || true
        exit 1
    fi
    blocking=$(jq '[.Issues[] | select(.FromLinter != "typecheck")] | length' "$dir/report.json")
    if [ "$blocking" -gt 0 ]; then
        jq -r '.Issues[] | select(.FromLinter != "typecheck")
            | "\(.Pos.Filename):\(.Pos.Line):\(.Pos.Column): \(.FromLinter): \(.Text)"' "$dir/report.json" >&2
        exit 1
    fi
    if [ "$rc" -ne 0 ] && [ "$(jq '.Issues | length' "$dir/report.json")" -eq 0 ]; then
        echo "ERROR: golangci-lint exited $rc but reported no issues (tool/analysis failure)" >&2
        cat "$dir/stderr.log" >&2 || true
        exit 1
    fi
    echo "lint passed"
fi
echo "gate ok"
