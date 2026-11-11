# plinth — repository contract for automated contributors

This repository follows the open, harness-agnostic AGENTS.md standard. Any AI
coding assistant, automated review tool or pair-programming harness must follow
this file. It adds no harness-specific configuration and requires none.

## Purpose

plinth is a small Go toolkit for HTTP services: a library, not a service. It
keeps no state of its own and starts no listener unless the caller does. The
root module is standard-library only; third-party code lives in the nested
modules `keel` and `telemetry`, and database tests live in `jobs/pgtest`.

## Read first

1. `README.md` — what the library provides and the definition of 1.0.
2. `CONTRIBUTING.md` — the development and review rules.
3. `ROADMAP.md` — direction; not a promise.
4. `SECURITY.md` — how to report a vulnerability.
5. `docs/RELEASING.md` — release procedure.
6. `CHANGELOG.md` — every exported change is listed under Unreleased.

## Non-negotiable rules

- The root module keeps no requirements. New third-party code goes in `keel/`
  or `telemetry/`; the keel dependency budget is checked by
  `tools/keel-gate.sh`.
- Never weaken, skip, delete or hide a failing test to make a change pass.
  Never edit a gate or CI definition to bypass a failure.
- Do not add a file, directory or placeholder only to satisfy a checker.
- Do not commit secrets, credentials, customer identifiers or private keys.
- Do not deploy, publish, tag a release or change repository settings.
- Do not force-push, rewrite history or commit directly to `develop`.
- Do not change the module path or exported API without the versioning rule in
  `CONTRIBUTING.md` and an `Unreleased` changelog entry.

## Layout

| Path | Contents |
|---|---|
| root packages | one package per directory; package name describes the capability |
| `keel/` | nested module: PostgreSQL, pgx, queue, cron, leader election |
| `telemetry/` | nested module: OpenTelemetry integration |
| `jobs/pgtest/` | nested module: PostgreSQL behaviour tests for jobs |
| `examples/` | runnable examples; `examples/hello` must pass `conformance.Run` |
| `tools/` | gate and database helpers; development machines and CI only |
| `docs/` | reference, benchmarks, release notes and change notes |

## Commands

    tools/gate.sh                 # root module gate
    bash tools/keel-gate.sh       # keel module gate, from the repository root
    cd telemetry && go vet ./... && go test -race -count=1 ./...
    tools/testdb.sh <command>     # throwaway PostgreSQL for database tests
    go test -race -count=1 ./...  # what the gate runs for the root module

`tools/testdb.sh` starts a disposable PostgreSQL container; never point tests
at a live database. The gate is the verdict: report its exit code, not an
impression.

## Working rules

- Branch from `develop` with a scoped prefix: `feat/`, `fix/`, `perf/`,
  `refactor/`, `test/`, `docs/`, `build/` or `chore/`.
- Commit in Conventional Commits: `type(scope): imperative summary`, lower
  case, at most 72 characters, no trailing period. One logical change per
  commit.
- The commit author is the project owner identity configured in the
  environment. Do not change it and do not add Co-Authored-By or generated-with
  trailers.
- Public text — commit subjects and bodies, README, package docs, comments —
  describes the change only. No internal tool names, no process transcript, no
  emoji, no attribution lines.
- Add or update tests with behaviour changes; add benchmarks for hot paths.
- Add doc comments for public API changes and a package comment on the
  package's main file. Mark deprecated identifiers with a `Deprecated:`
  paragraph.
- Problem responses must not include SQL, stack traces or wrapped internal
  error text.
- If a convention checked by `conformance` changes, update the checks in the
  same change and keep `examples/hello` passing.

## Reporting a change

Run the narrow test first, then the relevant gate. The final report states:
files changed, commands run with exit codes, gate result, and what is not
done. A report without command output is not evidence. Process notes belong
outside the repository; never commit them.
