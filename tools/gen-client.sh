#!/usr/bin/env bash
# Generate a typed TypeScript client for the ui monorepo from a service's
# served schema index ({prefix}/schema JSON file or URL).
#
# Usage:
#   tools/gen-client.sh <schema.json | http(s)://host/api/rt/schema> <output.ts>
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "usage: $0 <schema-file-or-url> <output.ts>" >&2
  exit 2
fi

SRC="$1"
OUT="$(realpath -m "$2")"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

TMP=""
cleanup() {
  if [ -n "$TMP" ] && [ -f "$TMP" ]; then
    rm -f "$TMP"
  fi
}
trap cleanup EXIT

case "$SRC" in
  http://*|https://*)
    TMP="$(mktemp)"
    curl -fsSL -H "Accept: application/json" "$SRC" -o "$TMP"
    SCHEMA_FILE="$TMP"
    ;;
  *)
    SCHEMA_FILE="$(realpath "$SRC")"
    ;;
esac

mkdir -p "$(dirname "$OUT")"
cd "$ROOT"
PLINTH_SCHEMA_IN="$SCHEMA_FILE" PLINTH_TS_OUT="$OUT" go test -count=1 -run '^TestGenerateTypeScriptCLI$' ./realtime >/dev/null
echo "generated $OUT from $SRC"
