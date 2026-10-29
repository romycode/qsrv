# qsrv

Single-node durable message queue server backed by SQLite. Named FIFO queues,
leases with ack/nack/extend, retry policy with exponential backoff,
dead-letter queues, per-entity ordering groups, and replay of processed
messages.

## Guarantees

- **Durability**: every client-visible result (send 201, receive 200, ack/nack
  204, extend 200, replay 200) is returned only after the SQLite transaction
  commits with `synchronous=FULL`. `kill -9` or power loss never rolls back
  something a client observed.
- **At-least-once**: a message whose lease is lost (consumer crash, server
  crash while in flight, ack after expiry → `410 lease_gone`) is redelivered.
  **Consumers must be idempotent.**
- **Ordering** (per queue, `ordering`):
  - `available` (default): FIFO among deliverable messages; one in flight or
    backing off does not block the rest. A retried message returns to its
    original position. Messages with a `group_id` are **strictly ordered within
    the group**: N+1 is not delivered until N is acked or dead-lettered.
    Groups run in parallel. Holds across crashes.
  - `strict`: the whole queue is one group. Total order, one in flight.
- **Poison messages**: after `retry.max_attempts` deliveries the message is
  copied to `dead_letter_queue` (keeping id, body and group), or marked
  `dropped` if none is set. Either way it unblocks its group.

## History, retention and replay

Ack does not delete. Processed messages stay in the database as history with
their final state: `acked`, `dropped` or `dead_lettered`. A janitor deletes
history older than the queue's `retention` (default 90 days; `0` = forever).
Live messages are never purged.

Replay copies history of a queue into a destination queue as **new messages**:
new id, same body and `group_id`, `replay_of` = original id, original order.
Filters: time range on `enqueued_at`, `group_id`, final state. Paginated with
a cursor; each page is atomic (all or nothing, respects `max_messages`).

Replay into a dedicated queue unless your consumers tolerate old events mixed
with live traffic: replaying into a live queue interleaves old and new
messages of the same group.

```sh
qctl create -retention 0 orders-replay
qctl replay -to orders-replay -from 2026-10-01 -until 2026-10-15 orders
qctl replay -to orders-replay -group customer-42 -states acked orders
qctl replay -to orders-replay -dry-run orders   # print the request only
```

Replayed copies are themselves history of the destination once processed.

## Run

Requires Go 1.27+. Pure Go by default: SQLite runs as WASM inside the wazero runtime
(`ncruces/go-sqlite3`), so builds are static and cross-compile anywhere.
The first start compiles SQLite to native code (~1-1.5s) and caches it in
`data-dir/.wasmcache`; later starts take ~50ms.

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags='-s -w' -o bin/ ./cmd/...   # e.g. Graviton
```

Optional cgo driver (`mattn/go-sqlite3`), same behavior and file format:
`CGO_ENABLED=1 go build -tags sqlite_cgo ./cmd/...` (needs a C toolchain for
the target). Measured difference over HTTP: ~20% more max throughput on a fast
local disk, ~10% with 1ms commits, same latency at normal load, ~15MB less RSS.

```sh
CGO_ENABLED=0 go build -o bin/ ./cmd/...
export QSRV_API_KEYS=$(openssl rand -hex 32)      # comma-separated, ≥32 chars each
./bin/qsrv -data-dir /var/lib/qsrv -addr :8080    # or -api-keys-file keys.txt
```

| Flag / env | Default | |
|---|---|---|
| `-addr` / `QSRV_ADDR` | `:8080` | |
| `-data-dir` / `QSRV_DATA_DIR` | `./data` | holds `qsrv.db`; flock'd, one process per dir |
| `-api-keys-file` / `QSRV_API_KEYS_FILE`, `QSRV_API_KEYS` | — | required unless `-insecure-no-auth` |
| `-tls-cert`, `-tls-key` | — | serve HTTPS directly; otherwise put TLS in front |
| `-max-message-bytes` | 256KiB | |
| `-retention-check` | 1m | janitor interval |
| `-log-level` | info | JSON logs to stderr |

Key rotation: add the new key, roll clients, remove the old key, restart.
SIGTERM: long-polls get 503 immediately, pending writes commit, exit 0.

## Backups and replication

The single-node weak point is the disk. Options, from least to most effort:

- `sqlite3 data/qsrv.db "VACUUM INTO '/backup/qsrv-$(date +%F).db'"` from cron (hot, consistent).
- [Litestream](https://litestream.io) continuously replicating `qsrv.db` to
  S3/R2/GCS: point-in-time restore with seconds of RPO, no code changes
  (the database already runs in WAL mode).

## API (all under `/v1`, `Authorization: Bearer <key>`; `/healthz` is public)

| Method | Path | |
|---|---|---|
| GET | `/queues` | list |
| PUT | `/queues/{name}` | create: `{"visibility_timeout":"30s","retry":{"max_attempts":5,"initial_backoff":"1s","max_backoff":"1m","multiplier":2},"dead_letter_queue":"dlq","ordering":"available","max_messages":0,"retention":"2160h"}` |
| GET | `/queues/{name}` | config + stats (`history` = retained processed messages) |
| DELETE | `/queues/{name}` | deletes live messages **and history**; refused (422) while it is another queue's DLQ |
| POST | `/queues/{name}/messages` | `{"body":"text"}` or `{"body_base64":"..."}`, optional `"group_id"` and `"delay":"30s"` (max `720h`; deliverable after that, still FIFO by send order, and in a group it also waits for the messages before it) |
| POST | `/queues/{name}/receive?wait=20s` | 200 delivery, 204 empty |
| POST | `/queues/{name}/leases/{lease}/ack` | 204, 410 if lease gone |
| POST | `/queues/{name}/leases/{lease}/nack` | 204, applies retry policy |
| POST | `/queues/{name}/leases/{lease}/extend` | `{"by":"1m"}` heartbeat |
| POST | `/queues/{name}/replay` | `{"to":"dst","from":"RFC3339","until":"RFC3339","group_id":"","states":["acked"],"cursor":0,"limit":1000}` → `{"replayed":N,"next_cursor":C}` (no `next_cursor` = done) |

Errors: `{"error":"...","code":"not_found|lease_gone|already_exists|invalid_config|queue_full|unauthorized|bad_request|unavailable"}`.
`429 queue_full` when `max_messages` is reached (backpressure).

## CLI

```sh
export QSRV_ADDR=http://localhost:8080 QSRV_API_KEY=...
qctl create -max-attempts 3 dlq
qctl create -visibility 1m -max-attempts 5 -backoff 2s -dlq dlq -retention 2160h orders
echo '{"id":1}' | qctl send -group customer-42 orders
qctl recv -wait 20s orders            # exit 3 if empty
qctl ack orders <lease>
qctl consume -concurrency 8 orders -- ./handler.sh   # stdin=body, exit 0=ack else nack
qctl replay -to orders-replay orders
```

`consume` heartbeats the lease while the command runs, exports
`QSRV_MESSAGE_ID/GROUP_ID/ATTEMPTS`, and on SIGINT lets running jobs finish.

Go SDK: `github.com/romycode/qsrv/pkg/client`.

## PHP and Symfony Messenger

`composer require romycode/qsrv-php`: a PHP client and a Messenger transport
(`qsrv://KEY@host:8080/queue`). See [php/README.md](php/README.md).

## Releases

The server and the PHP package are versioned independently:

| tag in this repo | releases |
|---|---|
| `v0.4.0` | Go server and module (`github.com/romycode/qsrv@v0.4.0`) |
| `php/v0.2.0` | PHP package `romycode/qsrv-php` `v0.2.0` |

`php/` is developed here and published by `.github/workflows/php-split.yml`
to the read-only mirror [romycode/qsrv-php](https://github.com/romycode/qsrv-php):
every push to `main` that touches `php/` syncs its `main`, and each
`php/vX.Y.Z` tag becomes tag `vX.Y.Z` there. Packagist reads the mirror.

## Metrics and alerts

Prometheus text format on `-metrics-addr` (default `127.0.0.1:9464`, path
`/metrics`). No auth: keep it on localhost or a private interface.
Ready-to-use rules in `deploy/prometheus/alerts.yml` (thresholds marked `TUNE`).

| Metric | Type | What it tells you |
|---|---|---|
| `qsrv_queue_oldest_message_age_seconds{queue}` | gauge | **The** health signal: how long the oldest live message has waited |
| `qsrv_queue_messages{queue,state}` | gauge | ready / delayed / in_flight |
| `qsrv_queue_groups{queue}` | gauge | ordering groups with live messages |
| `qsrv_messages_total{queue,event}` | counter | sent, dead_letter_in, replay_in, delivered, redelivered, acked, nacked, expired, dead_lettered, dropped |
| `qsrv_store_commit_duration_seconds` | histogram | commit + fsync latency (disk health; every op waits for it) |
| `qsrv_store_commit_batch_ops` | histogram | ops per commit (group commit efficiency) |
| `qsrv_store_pending_writes` | gauge | ops waiting for the disk |
| `qsrv_http_requests_total{route,method,code}` | counter | route = ServeMux pattern (bounded cardinality) |
| `qsrv_http_request_duration_seconds{route}` | histogram | receive includes long-poll wait |
| `qsrv_disk_free_bytes`, `qsrv_disk_size_bytes`, `qsrv_db_size_bytes{file}` | gauge | capacity planning, WAL growth |
| `qsrv_queue_info`, `qsrv_queue_max_messages`, `qsrv_build_info`, `qsrv_start_time_seconds`, process/Go stats | gauge | context for alerts |

Alerts included: down, restart loop, 5xx ratio, messages too old (warning /
critical), queue without consumers, dead-lettered/dropped messages, DLQ not
empty, high redelivery ratio, lease expiries, queue near full, 429s, disk
low / critical / will fill in 3 days, slow commits, writer backlog, WAL
growth, memory. DLQs are excluded from the "too old" and "no consumers"
alerts via the `qsrv:dlq:is_dlq` recording rule.

Validate before deploying: `promtool check rules deploy/prometheus/alerts.yml`.

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

## Performance

Measure on your own hardware with `qbench` (included) against a running server:

```sh
qbench -producers 8 -consumers 16 -duration 30s -size 1024   # saturation
qbench -rate 50 -groups 20 -duration 60s                      # latency at steady load
```

Reference (HTTP end to end, 1KB bodies, 2 vCPU shared with the load
generator, local disk with ~260µs fdatasync; slower disks emulated with
`-debug-simulated-sync-latency`, which adds a fixed delay per commit, so real
tail latencies will be worse):

| extra commit latency | ≈ disk | max full cycles/s (send+recv+ack) | end2end p50 / p99 at 50 msg/s |
|---|---|---|---|
| 0 | local NVMe | ~1,800 | 4ms / 11ms |
| 250µs | io2 Block Express | ~1,400 | 9ms / 15ms |
| 1ms | gp3 (good case) | ~1,280 | 9ms / 15ms |
| 3ms | gp3 (bad case) | ~740 | 18ms / 25ms |

Server CPU ≈ 0.5ms per full message cycle (3 HTTP requests); RSS ~20-30MB.
Storage: ~body size + ~150 bytes per message; 50k/day of 1KB ≈ 60MB/day of
history, ~5.5GB at the default 90-day retention.

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

## CI

`.github/workflows/ci.yml` runs on every push to `main` and every PR:

- **Lint & build**: gofmt, `go vet` (both drivers), `go mod verify`,
  static cross-builds for linux/amd64, linux/arm64 and darwin/arm64.
- **Tests**: `go test -race` with the WASM driver and with the cgo driver;
  coverage (`-coverpkg=./...`) on the WASM run, compared with the PR base.
- **PHP**: unit tests plus integration tests against a real qsrv, for PHP
  8.4 and 8.5 × Symfony 7.4 and 8.1.
- **Performance**: the Go benchmarks, base vs head interleaved 6 times on
  the same runner and compared with `benchstat`, plus a `qbench` load test
  against a real server.
- **PR report**: one comment per PR, updated on each push, with test
  results, coverage per package (with delta) and performance. The same
  content goes to each run's job summary.

Benchmarks report, they never fail the build: CI runners share disks and
fsync timing is noisy.

## Limits / not included

- Single node. Use Litestream (above) for disaster recovery.
- No metrics endpoint, no per-key permissions, no batch endpoints, no dedup.
- Replay copies are new messages: consumers must still be idempotent if they
  can see both an original and its replay (use `replay_of`).
