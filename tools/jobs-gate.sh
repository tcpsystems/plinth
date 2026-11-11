#!/usr/bin/env bash
# Gate for the jobs package PostgreSQL tests. The behavioural tests live in a
# nested module (jobs/pgtest) so the root module stays standard-library-only.
# This script runs them against a real PostgreSQL instance.
#
# Usage:
#   bash tools/jobs-gate.sh              # uses tools/testdb.sh
#   KEEL_TEST_DSN="postgres://..." bash tools/jobs-gate.sh
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -z "${KEEL_TEST_DSN:-}" ]; then
  # No DSN provided; use testdb.sh to start a throwaway database.
  KEEL_TEST_DSN="$(bash tools/testdb.sh --check)"
  export KEEL_TEST_DSN
  trap 'bash tools/testdb.sh --stop' EXIT
fi

(cd jobs/pgtest && go test -race -count=1 ./...)
