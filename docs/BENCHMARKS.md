# Benchmarks

Numbers for the paths every request takes through plinth: JSON encode and
decode, writing and reading a response, signed page tokens, idempotency
middleware, and publishing a realtime event. Each row is owned by a
`Benchmark*` function in the package's `bench_test.go`, so anyone can rerun it.

These are single-goroutine microbenchmarks on one machine. Read them as a
budget ("a JSON response costs about two microseconds here"), not as a
promise; compare changes against the same machine.

## How they were measured

- Go 1.27.3 linux/amd64, `GOEXPERIMENT` default (`encoding/json/v2` on),
  AMD EPYC 9575F, `GOMAXPROCS` 5 for the runs below.
- Command:
  `go test -run '^$' -bench . -benchmem -count=5 ./jsonx ./httpx ./page ./idem ./realtime`
- The table reports the median of five runs of 2,000,000 iterations each.
  Allocations are from the same runs (identical across the five).
- The engine-off rows come from `GOEXPERIMENT=nojsonv2` with
  `-count=1`, so treat them as indicative.

The same relative ordering held with a single default run
(`-count=1`); absolute times vary by up to about 40% between runs on this
host, which is why only medians are listed.

## Results

| package | benchmark | what it does | median ns/op | B/op | allocs/op |
|---|---|---|---|---|---|
| `jsonx` | `BenchmarkMarshal` | encode a two-field struct | 204 | 40 | 2 |
| `jsonx` | `BenchmarkUnmarshal` | decode a 19-byte object into a struct | 225 | 0 | 0 |
| `httpx` | `BenchmarkWriteJSON` | encode a page envelope and write it | 2066 | 1528 | 18 |
| `httpx` | `BenchmarkDecodeJSON` | size-capped decode of a 77-byte request body | 3014 | 5889 | 16 |
| `page` | `BenchmarkEncodeDecode` | sign and verify a keyset page token | 1267 | 736 | 9 |
| `page` | `BenchmarkParse` | read `page_size` and a token from a request | 2585 | 1570 | 17 |
| `idem` | `BenchmarkMiddlewareMiss` | keyed request, nothing stored: hash, run, store | 6193 | 8292 | 41 |
| `idem` | `BenchmarkMiddlewareReplay` | duplicate request replayed from the store | 6421 | 7649 | 35 |
| `realtime` | `BenchmarkPublish` | append an event to the hub ring, no subscribers | 29 | 0 | 0 |

### With the JSON v1 engine (`GOEXPERIMENT=nojsonv2`)

| benchmark | v2 ns/op | v1 ns/op | v2 allocs/op | v1 allocs/op |
|---|---|---|---|---|
| `jsonx.BenchmarkMarshal` | 204 | 100 | 2 | 1 |
| `jsonx.BenchmarkUnmarshal` | 225 | 452 | 0 | 4 |
| `httpx.BenchmarkWriteJSON` | 2066 | 1410 | 18 | 15 |
| `httpx.BenchmarkDecodeJSON` | 3014 | 3610 | 16 | 22 |

On the default Go 1.27 toolchain v2 decodes about twice as fast as v1 allocates
nothing on decode, but encodes slower with one extra allocation. The `httpx`
numbers include `httptest` recorder and request setup, which dominate the
difference. `jsonx` exists so this trade-off is revisited in one place when
the Go toolchain changes the default.

## What the numbers say about the design

- `httpx.DecodeJSON` allocates far more than the body it reads; most of that is
  `httptest` request and recorder setup, not plinth. Decoding the same 77-byte
  body directly through `jsonx` allocates nothing.
- `httpx.WriteJSON` cost is the JSON encode plus two map/reflect layers in the
  benchmark's `map[string]any` payload. Handlers that encode a typed envelope
  pay less.
- `idem` replay is not cheaper than a miss: both take the per-key lock and
  hash the body, by design, so a duplicate cannot start before the original
  finished.
- `realtime.Publish` is a mutex, an append and a copy into the ring: it is not
  the bottleneck in a realtime service, the network write is.

## Rerunning

```sh
go test -run '^$' -bench . -benchmem ./jsonx ./httpx ./page ./idem ./realtime
GOEXPERIMENT=nojsonv2 go test -run '^$' -bench . -benchmem ./jsonx ./httpx
```

Package changes that touch these paths should report before/after numbers for
the affected benchmark in the pull request.
