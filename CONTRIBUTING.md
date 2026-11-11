# Contributing to plinth

Thanks for helping with plinth. This guide covers how to propose a change,
work on it, and get it merged. By contributing you agree that your work is
licensed under the [MIT license](LICENSE).

## Before you change code

- Read the package you will change and the code that calls it.
- Keep a change focused on one problem. Unrelated cleanups belong in their own
  pull request.
- Search the repository for references before moving, renaming, or deleting a
  file. If a build, test, gate, or example reads it, keep it working or update
  it in the same change.
- For anything larger than a bug fix, open an issue describing the problem
  first, so the design can be discussed before you invest in it.

## Development rules

- The root module keeps no requirements: standard library only, and
  `database/sql` with a driver injected by the caller. New third-party code
  goes in a separate module: `keel/` for database integrations and `telemetry/`
  for OpenTelemetry. The keel dependency budget is enforced by
  `tools/keel-gate.sh`. Check telemetry with `cd telemetry && go vet ./... &&
  go test -race -count=1 ./...`. The PostgreSQL behavioural tests of `jobs`
  live in the nested module `jobs/pgtest` for the same reason.
- Name a package after what it provides; never `util`, `common` or `helpers`.
- Add or update doc comments for public API changes, and a package comment on
  the package's main file so pkg.go.dev shows an overview. Mark deprecated
  identifiers with a `Deprecated:` paragraph.
- Add or update tests with behavior changes. Do not weaken or remove a test to
  make a change pass. Hot paths need a benchmark.
- Error and problem responses must not include SQL, stack traces, or wrapped
  internal error text.
- If you change a convention that `conformance` checks, update the checks in
  the same change.
- `examples/hello` must keep passing `conformance.Run`.

## Versioning

plinth uses pre-1.0 semantic versioning: versions are `0.MINOR.PATCH`. A
breaking change (including renaming or removing an exported identifier or a
package, or changing the module path) bumps MINOR; a compatible feature or fix
bumps PATCH, and a notable feature may bump MINOR. There is no `v1` path and
no package claims to be stable in the 1.0 sense: APIs may change between minor
versions. Every exported change goes into `CHANGELOG.md` under `Unreleased`.
A rename keeps a forwarding package or type alias for one release.

What will define 1.0 is written in the README.

Create topic branches from `develop` using a scoped prefix:

```sh
git switch -c feat/page-size-alias develop    # a feature
git switch -c fix/problem-status develop      # a fix
```

`feat/`, `fix/`, `perf/`, `refactor/`, `test/`, `docs/`, `build/`, and `chore/`
are the usual types. Branches are deleted after the pull request merges; if you
pick up old work, start a new branch from `develop`. Do not keep long-lived
project branches (`project/…`, `epic/…`): a change that runs long is a series
of topic branches, each merged on its own.

## Check your change

Run the gate before opening the pull request:

```bash
tools/gate.sh            # root module
bash tools/keel-gate.sh  # keel module, from the repository root
```

The root gate checks formatting, `go vet`, race-enabled tests, JSON engine
coverage, the standard-library-only constraint, and the `examples/hello` size
limit. The keel gate checks formatting, `go vet`, race-enabled tests, the keel
dependency budget, and lint. Tests that need PostgreSQL skip unless
`KEEL_TEST_DSN` is set; `tools/testdb.sh` starts a throwaway database for
them. If the gate fails, fix the cause and run it again.

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/): a type, an
optional scope, a colon, then a short imperative subject ("add", not "added")
of about 72 characters.

```
feat(realtime): add Last-Event-ID resume to the SSE handler
fix(problem): keep the error code when the status is 409
docs: describe the release process
```

Write them the way a colleague would describe the change: what it does and,
when it is not obvious, why. Commit in small steps that each compile.

## Pull requests

Open pull requests against `develop` from your topic branch. Name the pull
request with a Conventional Commit subject; it becomes the squash commit when
it merges, so keep it up to date if the change grows.

In the description, say in a few plain sentences:

- what the change does and why it is needed;
- how you checked it, including any manual steps or databases used;
- anything intentionally left out.

Keep review discussion on the pull request. Do not add issue or tracker
references to commit messages, branch names, or code comments; link them from
the pull request description instead.

## Release notes

User-visible changes need a change note in `docs/changes/`. Add a file named
after your branch or change, written as a changelog fragment; see
[docs/changes/README.md](docs/changes/README.md). The release process in
[docs/RELEASING.md](docs/RELEASING.md) folds them into `CHANGELOG.md`.
