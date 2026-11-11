# Third-party notices

The root module has no third-party requirements. The `keel` module depends on
these modules at the versions pinned in [keel/go.mod](../../../keel/go.mod).
Each is distributed under its own license, reproduced next to this file; the
text of a license is not changed.

| Module | License | Text |
| --- | --- | --- |
| `github.com/jackc/pgx/v5` | MIT (Copyright (c) 2013-2021 Jack Christensen) | [pgx/LICENSE](pgx/LICENSE) |
| `github.com/jackc/pgpassfile` | MIT (Copyright (c) 2019 Jack Christensen) | [pgx/LICENSE.pgpassfile](pgx/LICENSE.pgpassfile) |
| `github.com/jackc/pgservicefile` | MIT (Copyright (c) 2020 Jack Christensen) | [pgx/LICENSE.pgservicefile](pgx/LICENSE.pgservicefile) |
| `github.com/jackc/puddle/v2` | MIT (Copyright (c) 2018 Jack Christensen) | [puddle/LICENSE](puddle/LICENSE) |
| `golang.org/x/sync` | BSD 3-Clause (Copyright 2009 The Go Authors) | [x/LICENSE.sync](x/LICENSE.sync) |
| `golang.org/x/text` | BSD 3-Clause (Copyright 2009 The Go Authors) | [x/LICENSE.text](x/LICENSE.text) |

The module versions in use are pinned by `keel/go.sum`. When a dependency is
upgraded, replace the license text here in the same change when the upstream
license has changed.
