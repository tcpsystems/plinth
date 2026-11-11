# Roadmap

plinth is pre-1.0. This page describes direction, not promises: priorities
change as users report what they need. Open issues and pull requests are the
real snapshot of what is in progress.

Short and honest. Items move out of here when they land in a 0.x release.

## Next (0.3)

- Move `realtime`, `events`, `settings`, `migrate` and `conformance` out of
  experimental as each gains a second real consumer.
- Split the loose helpers into named packages once the proposals
  are settled: `httpjson` (from `httpx`), `envconfig` (from `envx`), and
  `accesslog` (from `logx`).
- Real-Postgres tests for the SQL stores (`idem.SQLStore`, `settings.SQLSource`,
  `outbox.SQLRelay`) behind an opt-in build tag.

## Later

- pkg.go.dev rendering of runnable examples (`Example` functions) for the
  five-minute start.
- An opt-in `http.Handler` adapter that composes the middleware stack the
  examples wire by hand.

## Not planned

- A `v1` import path. Breaking changes bump the minor version until 1.0 is
  defined (see the README).
- Third-party dependencies.
