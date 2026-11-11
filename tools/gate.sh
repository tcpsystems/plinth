#!/usr/bin/env bash
# Gate for the root module: formatting, vet, race tests, no module
# requirements, example size.
# Optional host tools: golangci-lint (v2 required) and jq (required whenever
# golangci-lint is present, to parse its JSON report), staticcheck, govulncheck.
set -euo pipefail
cd "$(dirname "$0")/.."

bad=$(gofmt -l .)
if [ -n "$bad" ]; then echo "gofmt needed:"; echo "$bad"; exit 1; fi
go vet ./...
go test -race -count=1 ./...
# jsonx must also work on the v1 fallback engine (single switch point).
GOEXPERIMENT=nojsonv2 go test -count=1 ./jsonx ./httpx
# optional extras when installed: staticcheck, govulncheck
if command -v govulncheck >/dev/null; then govulncheck ./... || echo "WARN: govulncheck findings (toolchain-level ones clear by upgrading Go)"; fi
# golangci-lint is scoped to new issues (relative to a real base revision) so
# legacy findings do not block. This step is fail-closed: stderr and the exit
# status are captured, the JSON report is validated, and a linter that could not
# run makes the gate fail. typecheck issues are reported but do not block: they
# duplicate the mandatory `go vet`/`go test` above, and golangci-lint cannot
# always read the export data of a newer Go toolchain.
# >>> lint gate
if command -v golangci-lint >/dev/null; then
  # Declared dependency: parsing the JSON report below requires jq. If lint is
  # installed but jq is not, fail closed instead of silently passing.
  if ! command -v jq >/dev/null; then
    echo "ERROR: golangci-lint is installed but jq is required to parse its JSON report" >&2
    exit 1
  fi

  # v1 cannot read Go 1.27 export data and has no --output.json.path.
  lint_version=$(golangci-lint version --short 2>/dev/null || true)
  case "$lint_version" in
    v2.*|2.*) ;;
    *)
      echo "ERROR: golangci-lint v2 is required (found '${lint_version:-unknown}')" >&2
      exit 1
      ;;
  esac

  # origin/main may not exist in every clone/worktree. Fall back to origin/HEAD
  # (develop), then origin/develop; if no base is a valid commit, fail closed
  # rather than letting a bad revision look like a clean run.
  lint_base=origin/main
  if ! git rev-parse --verify --quiet "${lint_base}^{commit}" >/dev/null; then
    lint_base=origin/HEAD
  fi
  if ! git rev-parse --verify --quiet "${lint_base}^{commit}" >/dev/null; then
    lint_base=origin/develop
  fi
  if ! git rev-parse --verify --quiet "${lint_base}^{commit}" >/dev/null; then
    echo "ERROR: no usable base revision for golangci-lint --new-from-rev (tried origin/main, origin/HEAD, origin/develop)" >&2
    exit 1
  fi
  echo "lint: scoped to new issues since $lint_base"

  lint_dir=$(mktemp -d) || { echo "ERROR: mktemp failed" >&2; exit 1; }
  lint_json="$lint_dir/report.json"
  lint_stderr="$lint_dir/stderr.log"
  set +e
  golangci-lint run --new-from-rev="$lint_base" --output.json.path="$lint_json" ./... \
    >"$lint_dir/stdout.log" 2>"$lint_stderr"
  lint_rc=$?
  set -e

  # An absent, empty, or unparseable report is a tool failure, never "no issues".
  if ! { [ -s "$lint_json" ] && jq -e '.Issues | type == "array"' "$lint_json" >/dev/null 2>&1; }; then
    echo "ERROR: golangci-lint exited $lint_rc without a parseable JSON report" >&2
    echo "----- golangci-lint stderr -----" >&2
    cat "$lint_stderr" >&2 || true
    echo "----- golangci-lint stdout -----" >&2
    cat "$lint_dir/stdout.log" >&2 || true
    rm -rf "$lint_dir"
    exit 1
  fi

  lint_total=$(jq '.Issues | length' "$lint_json")
  lint_typecheck=$(jq '[.Issues[] | select(.FromLinter == "typecheck")] | length' "$lint_json")
  lint_blocking=$(jq '[.Issues[] | select(.FromLinter != "typecheck")] | length' "$lint_json")

  if [ "$lint_blocking" -gt 0 ]; then
    echo "Lint found issues:" >&2
    jq -r '.Issues[] | select(.FromLinter != "typecheck") | "\(.Pos.Filename):\(.Pos.Line):\(.Pos.Column): \(.FromLinter): \(.Text)"' "$lint_json" >&2
    rm -rf "$lint_dir"
    exit 1
  fi

  # Non-zero exit with an empty issue list means the linter crashed or could
  # not analyse the packages; surface its stderr and fail.
  if [ "$lint_rc" -ne 0 ] && [ "$lint_total" -eq 0 ]; then
    echo "ERROR: golangci-lint exited $lint_rc but reported no issues (tool/analysis failure)" >&2
    echo "----- golangci-lint stderr -----" >&2
    cat "$lint_stderr" >&2 || true
    echo "----- golangci-lint stdout -----" >&2
    cat "$lint_dir/stdout.log" >&2 || true
    rm -rf "$lint_dir"
    exit 1
  fi

  # Even with a zero exit, a fatal/analysis error on stderr is a gate failure.
  if grep -qiE 'level=error|can.t load|failed to load|panic:' "$lint_stderr"; then
    echo "ERROR: golangci-lint reported a fatal/analysis error:" >&2
    cat "$lint_stderr" >&2
    rm -rf "$lint_dir"
    exit 1
  fi

  if [ "$lint_typecheck" -gt 0 ]; then
    echo "lint passed: $lint_typecheck typecheck issue(s) ignored (duplicate of go vet/test; known toolchain mismatch)"
    jq -r '.Issues[] | select(.FromLinter == "typecheck") | "  \(.Pos.Filename):\(.Pos.Line):\(.Pos.Column): \(.Text)"' "$lint_json"
  else
    echo "lint passed: no issues"
  fi
  rm -rf "$lint_dir"
fi
# <<< lint gate

if grep -q '^require' go.mod || grep -q '^[[:space:]]*require' go.mod; then
  echo "go.mod must have no require block (standard library only)"; exit 1
fi

lines=$(find examples/hello -name '*.go' ! -name '*_test.go' -exec cat {} + | wc -l)
if [ "$lines" -ge 200 ]; then echo "examples/hello is $lines lines, must be under 200"; exit 1; fi

# jobs package PostgreSQL tests (nested module).
bash tools/jobs-gate.sh

echo "gate ok (example: $lines lines)"
