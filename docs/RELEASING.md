# Releasing

This page describes how plinth is versioned and released.

## Versioning

The project is pre-1.0 and uses semantic versioning with the pre-1.0 rule:
minor versions (`v0.x.0`) may change APIs, and a breaking change is called out
with **Breaking:** in the changelog. Patch releases (`v0.x.y`) fix bugs and do
not change APIs.

The repository holds several Go modules, and each is tagged on its own with
its directory as the tag prefix:

| Module | Tag |
| --- | --- |
| `github.com/tcpsystems/plinth` | `v0.x.y` |
| `github.com/tcpsystems/plinth/keel` | `keel/v0.x.y` |
| `github.com/tcpsystems/plinth/telemetry` | `telemetry/v0.x.y` |
| `github.com/tcpsystems/plinth/jobs/pgtest` | `jobs/pgtest/v0.x.y` |
| `github.com/tcpsystems/plinth/examples/telemetry` | `examples/telemetry/v0.x.y` |

All tags of a release point at the same commit on `develop`. A nested module
that requires another module of this repository (`telemetry`, `jobs/pgtest` and
`examples/telemetry` require the root module; `examples/telemetry` also requires
`telemetry`) pins it at the release version in its `go.mod`, with no `replace`
directive, so the tags must be pushed in dependency order: the root tag first,
then `telemetry`, then the remaining modules. A published tag is never moved
or deleted: the Go module proxy keeps the first content it saw for a version,
so a mistake is fixed with a new patch release, and a version that must not be
used is marked with a `retract` directive in `go.mod`.

## Branches

- `develop` is the default branch. Pull requests merge into it.
- `production` tracks the most recent release. It is fast-forwarded to the
  release commit, never merged into.
- Topic branches are `type/short-topic` and are deleted after merge; see
  [CONTRIBUTING.md](../CONTRIBUTING.md).

## Making a release

1. Check that every user-visible change has a note in `docs/changes/`.
2. Fold the notes into `CHANGELOG.md` under a new version heading, with the
   release date, and delete the fragments. Commit this as
   `docs: release v0.x.y`.
3. Check the tree:
   ```sh
   tools/gate.sh
   bash tools/keel-gate.sh
   bash tools/jobs-gate.sh
   (cd telemetry && go vet ./... && go test -race -count=1 ./...)
   ```
4. Tag and push, root module first:
   ```sh
   git tag -a v0.x.y -m "v0.x.y"
   git push origin develop v0.x.y
   for m in telemetry keel jobs/pgtest examples/telemetry; do
     git tag -a "$m/v0.x.y" -m "$m v0.x.y"
   done
   git push origin telemetry/v0.x.y keel/v0.x.y jobs/pgtest/v0.x.y examples/telemetry/v0.x.y
   git push origin v0.x.y^{commit}:refs/heads/production
   ```
5. Ask the module proxy for the new versions so that pkg.go.dev lists them:
   ```sh
   for m in "" /keel /telemetry /jobs/pgtest /examples/telemetry; do
     GOPROXY=https://proxy.golang.org go list -m "github.com/tcpsystems/plinth$m@v0.x.y"
   done
   ```
