# Security policy

## Reporting a vulnerability

Please report security issues privately. Do not open a public issue, and do
not include exploit details in a pull request.

Use GitHub's private vulnerability reporting on this repository
("Security" tab, "Report a vulnerability"). Include:

- the package and version affected;
- what an attacker can do, and what they need to start from;
- the smallest reproduction you can give;
- any known mitigations.

You will get an acknowledgement, and a further reply once the report is
assessed. Once a fix is ready it is released together with an advisory entry
in [CHANGELOG.md](CHANGELOG.md); reporters are credited unless they prefer
otherwise.

## Supported versions

The project is pre-1.0. Fixes go to the latest `v0.x` release line and to
`develop`; older minors are not maintained.

## Scope

plinth is a library, so most issues are in the code you call: request
parsing, authentication helpers, idempotency, signing, the realtime
transports, SQL built by `migrate` and `outbox`, and the PostgreSQL leader and
event code in `keel/`. Misconfiguration of a service that uses the library,
and issues in your own database or reverse proxy, are usually out of scope,
but tell us if the library's defaults made the misconfiguration likely.

`tools/` scripts are meant for development machines and CI, not for exposing
on a network.
