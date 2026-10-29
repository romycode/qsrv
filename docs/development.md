# Development

## Build and test

```sh
go build ./...
go test -race ./...                       # WASM driver (default)
go test -race -tags sqlite_cgo ./...      # cgo driver
QSRV_TEST_LOG=1 go test ./internal/...    # show server logs in tests
```

Tests share a cache of SQLite compiled from WASM (`$XDG_CACHE_HOME/qsrv-test`,
keyed by `go.sum`); without it every test binary recompiles SQLite (~1.5s, or
~15s under `-race`). Override with `QSRV_WASM_CACHE_DIR`.

PHP: see [php/README.md](../php/README.md#tests). Examples: [examples/](../examples).

## Benchmarks

Go benchmarks (in-process, real fsync) live in `internal/queue/perf_bench_test.go`
and `internal/httpapi/bench_test.go`:

```sh
go test -run '^$' -bench . -benchmem ./...            # all
go test -run '^$' -bench 'Send|HTTP' -count 6 ./... > new.txt && benchstat old.txt new.txt
```

| benchmark | measures |
|---|---|
| `Send/body=…` | durable send throughput, 8×GOMAXPROCS producers, 128B/1KiB/16KiB |
| `SendSequential` | single producer: per-op durable send cost (no group commit help) |
| `ReceiveAck` | draining a backlog: receive (body read) + durable ack |
| `PersistentCycle` / `GroupsCycle` | send+receive+ack; the latter with 64 ordering groups |
| `Replay` | re-enqueue history, 1000-message pages (`msgs/s` metric) |
| `Open10kLive` | startup with 10k live messages |
| `HTTPCycle` | full cycle through HTTP + auth + Go client |

## Internals

- SQLite (`data-dir/qsrv.db`, WAL mode) is the source of truth. One table
  for queues, one for messages (live + history), indexed by `(queue, id)`,
  `(queue, seq)` and `(queue, state, done_at)`.
- Live message **metadata** is kept in memory (heaps for ready/delayed,
  lease expiries, ordering groups); bodies are read from SQLite on delivery.
  Memory is ~200 bytes per live message regardless of body size.
- A single writer goroutine commits all pending operations in one
  transaction (group commit). Readers use a separate pool.
- On startup, live messages are loaded from SQLite; expired leases are
  redelivered on the first operation.

## CI

`.github/workflows/ci.yml` runs on every push to `main` and every PR:

- **Lint & build**: gofmt, `go vet` (both drivers), `go mod verify`,
  static cross-builds for linux/amd64, linux/arm64 and darwin/arm64.
- **Tests**: `go test -race` with the WASM driver and with the cgo driver;
  coverage (`-coverpkg=./...`) on the WASM run, compared with the PR base.
- **PHP**: unit tests plus integration tests against a real qsrv, for PHP
  8.4 and 8.5 × Symfony 7.4 and 8.1.
- **Examples**: runs `examples/go` and `examples/php` against a real qsrv, so
  the documented examples keep working.
- **Conventional Commits** (PRs): every commit and the PR title.

`.github/workflows/docker.yml` builds the image for amd64 and arm64 on every
PR (with a smoke test of the amd64 image: healthy, then send/receive/ack with
`qctl` inside it), and pushes it to GHCR on `main` (`edge`) and on `vX.Y.Z`
tags.
- **Performance**: the Go benchmarks, base vs head interleaved 6 times on
  the same runner and compared with `benchstat`, plus a `qbench` load test
  against a real server.
- **PR report**: one comment per PR, updated on each push, with test
  results, coverage per package (with delta) and performance. The same
  content goes to each run's job summary.

Benchmarks report, they never fail the build: CI runners share disks and
fsync timing is noisy.

## Commits

[Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/):
`type(scope)!: description`, types `feat fix docs style refactor perf test
build ci chore revert`, scopes like `queue`, `httpapi`, `php`, `ci`. CI checks
every PR commit and the PR title (the subject of a squash merge). Locally:

```sh
git config core.hooksPath .githooks   # rejects non-conforming messages on commit
```

## Releases

The server and the PHP package are versioned independently:

| tag in this repo | releases |
|---|---|
| `v0.4.0` | Go server and module (`github.com/romycode/qsrv@v0.4.0`) and Docker image `ghcr.io/romycode/qsrv:0.4.0` |
| `php/v0.2.0` | PHP package `romycode/qsrv-php` `v0.2.0` |

`php/` is developed here and published to the read-only mirror
[romycode/qsrv-php](https://github.com/romycode/qsrv-php), which Packagist
reads, with your own git credentials (no CI secrets), from an up-to-date `main`:

```sh
scripts/release-php.sh v0.2.0   # tags php/v0.2.0 here and v0.2.0 on the mirror
scripts/release-php.sh --sync   # only refresh the mirror's main
```

The script refuses to publish anything that is not `origin/main`, and never
force-pushes the mirror.
