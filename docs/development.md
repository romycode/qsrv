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

PHP: see [php/README.md](../php/README.md#tests).

`examples/` is documentation: CI doesn't run it. Run it by hand against a
local qsrv when you change it (each file says how).

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

One workflow per area, each triggered only when its files change (GitHub
`paths` filters), on PRs and on pushes to `main`:

| workflow | runs when these change | what it does |
|---|---|---|
| `go.yml` | `**.go` (except `examples/`), `go.mod`, `go.sum`, `.github/ci/`, `.github/actions/` | gofmt, `go vet` (both drivers), `go mod verify`, static cross-builds; `go test -race` with the WASM and cgo drivers; coverage vs the PR base; benchmarks (base vs head with `benchstat`) and a `qbench` load test |
| `php.yml` | `php/`, Go server code (`cmd/`, `internal/`, `pkg/`, `go.mod`, `go.sum`) | PHPUnit, unit and integration against a real qsrv, PHP 8.4/8.5 × Symfony 7.4/8.1 |
| `docker.yml` | `Dockerfile`, `.dockerignore`, Go server code; every `v*` tag | builds amd64+arm64, smoke-tests the image; pushes to GHCR from `main` and tags |
| `commits.yml` | every PR (also on title edits) | Conventional Commits: each commit and the PR title |

PHP also runs on server changes because its integration tests run against
the server. Each workflow edits its own file, so changing a workflow runs it.
A docs-only PR runs only `commits.yml`.

`go.yml` and `php.yml` keep one comment each on the PR, updated on every run,
with their results (tests, coverage with delta, performance; PHPUnit per
version). The same content goes to the run's job summary. Benchmarks report,
they never fail the build: CI runners share disks and fsync timing is noisy.

If you make checks required in branch protection, require only `commits.yml`
(or use a ruleset with "skip if not run"): a workflow skipped by its path
filter never reports, and a required check would wait forever.

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
