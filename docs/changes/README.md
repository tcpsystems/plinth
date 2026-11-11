# Change notes

Every pull request that changes what users of the library see adds one file
here, written as a changelog fragment. The release process folds these into
[CHANGELOG.md](../../CHANGELOG.md) and removes them, so a note describes a
change in the words a release reader wants, not the steps that made it.

## Naming

Name the file after your branch, with `.md` for the extension:
`feat-page-size-alias.md`, `fix-problem-status.md`. One file per pull request is
enough; if a branch changes several things, list them under several headings
in that file.

## Format

```markdown
### Added

- `page`: `page.Query` reads `page[size]` as an alias for `page_size`.
```

The heading is a Keep a Changelog section: Added, Changed, Deprecated,
Removed, Fixed, or Security. Each entry starts with the package in backticks
(or `keel/leader`, `conformance`) and says what changed for someone calling
it. Say what a user would notice, not which files moved.

Rules for a note:

- Start it with the package, then the change, in the present tense
  imperative as in a commit subject ("add", not "added").
- Mention a breaking change explicitly with **Breaking:** at the start of the
  entry, and say what to do instead.
- Mention a security fix under Security, without reproducing the exploit.
- No issue numbers or review notes; link the pull request from the
  description instead.
