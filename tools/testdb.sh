#!/usr/bin/env bash
# Disposable PostgreSQL 18 for tests: a throwaway podman container on a
# runtime-allocated loopback port. The fixed password x is a disposable test
# value, never a secret.
#
#   tools/testdb.sh --check            start if needed, print the DSN, keep it
#   tools/testdb.sh --stop             remove this worktree's container
#   tools/testdb.sh go <go args>       run go with KEEL_TEST_DSN exported,
#                                      removing the container afterwards
#   tools/testdb.sh exec "<sql>"       run psql -c against the database
set -euo pipefail

IMAGE="docker.io/library/postgres:18-alpine"
HOST="127.0.0.1"
PORT=""
DSN=""
OWNED=""
RUN=()
here="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTAINER="keel-pg-test-$(basename "$here" | tr -c 'A-Za-z0-9_.\n-' '-')"

usage() {
    sed -n '2,11p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
}

set_dsn() {
    DSN="postgres://keel:x@$HOST:$PORT/keel_test?sslmode=disable"
}

diagnose() {
    echo "testdb: container failed: $CONTAINER" >&2
    podman logs --tail 30 "${OWNED:-$CONTAINER}" >&2 || true
    printf 'testdb: command: ' >&2
    if ((${#RUN[@]})); then
        printf '%q ' "${RUN[@]}" >&2
    else
        printf 'podman container inspect %q' "$CONTAINER" >&2
    fi
    printf '\n' >&2
}

reap_stale() {
    local id name running
    while read -r id name; do
        [[ "$name" == "$CONTAINER" ]] || continue
        running="$(podman container inspect --format '{{.State.Running}}' "$id" 2>/dev/null || true)"
        if [[ "$running" == false ]]; then
            podman rm -f "$id" >/dev/null
        fi
    done < <(podman ps -a --format '{{.ID}} {{.Names}}')
}

cleanup() {
    if [[ -n "$OWNED" ]]; then
        podman rm -f "$OWNED" >/dev/null
    fi
}

wait_ready() {
    local tries running
    for ((tries = 0; tries < 60; tries++)); do
        running="$(podman container inspect --format '{{.State.Running}}' "$OWNED" 2>/dev/null || true)"
        if [[ "$running" != true ]]; then
            diagnose
            return 1
        fi
        if podman exec "$OWNED" pg_isready -h 127.0.0.1 -p 5432 -U keel -d keel_test >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "testdb: PostgreSQL did not become ready within 60s; port=$PORT" >&2
    diagnose
    return 1
}

start_db() {
    local attempt running output
    reap_stale
    if podman container exists "$CONTAINER"; then
        OWNED="$(podman container inspect --format '{{.Id}}' "$CONTAINER")"
        running="$(podman container inspect --format '{{.State.Running}}' "$OWNED" 2>/dev/null || true)"
        if [[ "$running" != true ]]; then
            diagnose
            return 1
        fi
        PORT="$(podman container inspect --format '{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}' "$OWNED")"
        set_dsn
        wait_ready
        return
    fi
    podman image exists "$IMAGE" || podman pull "$IMAGE" >&2
    for ((attempt = 1; attempt <= 5; attempt++)); do
        PORT="$(python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()')"
        set_dsn
        RUN=(podman run -d --name "$CONTAINER"
            -e POSTGRES_USER=keel -e POSTGRES_PASSWORD=x -e POSTGRES_DB=keel_test
            -p "$HOST:$PORT:5432" "$IMAGE")
        if output="$("${RUN[@]}" 2>&1)"; then
            OWNED="$output"
            wait_ready
            return
        fi
        printf '%s\n' "$output" >&2
        diagnose
        # A bind failure may leave a stopped container holding the name.
        running="$(podman container inspect --format '{{.State.Running}}' "$CONTAINER" 2>/dev/null || true)"
        if [[ "$running" == false ]]; then
            OWNED="$(podman container inspect --format '{{.Id}}' "$CONTAINER")"
            cleanup
            OWNED=""
        fi
        case "$output" in
            *"address already in use"* | *"port is already allocated"* | *"cannot bind"*) ;;
            *) return 1 ;;
        esac
    done
    echo "testdb: could not bind a loopback port after 5 attempts" >&2
    return 1
}

main() {
    case "${1:-}" in
        --check)
            start_db
            printf '%s\n' "$DSN"
            ;;
        --stop)
            if podman container exists "$CONTAINER"; then
                podman rm -f "$CONTAINER"
            fi
            ;;
        go)
            shift
            trap cleanup EXIT
            start_db
            export KEEL_TEST_DSN="$DSN"
            go "$@"
            ;;
        exec)
            shift
            if (($# < 1)); then
                echo "testdb: exec needs a SQL argument" >&2
                usage
                exit 2
            fi
            trap cleanup EXIT
            start_db
            podman exec -e PGPASSWORD=x "$OWNED" \
                psql -h 127.0.0.1 -U keel -d keel_test -v ON_ERROR_STOP=1 -c "$1"
            ;;
        -h | --help | "") usage ;;
        *)
            echo "testdb: unknown subcommand: $1" >&2
            usage
            exit 2
            ;;
    esac
}

main "$@"
