# Operations

Running qsrv in production: configuration, Docker, backups, monitoring and
sizing.

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
| `-metrics-addr` / `QSRV_METRICS_ADDR` | `127.0.0.1:9464` | Prometheus `/metrics`, no auth; empty disables |
| `-log-level` / `QSRV_LOG_LEVEL` | info | JSON logs to stderr |

Key rotation: add the new key, roll clients, remove the old key, restart.
SIGTERM: long-polls get 503 immediately, pending writes commit, exit 0.

## Backups

The single-node weak point is the disk. Options, from least to most effort:

- `sqlite3 data/qsrv.db "VACUUM INTO '/backup/qsrv-$(date +%F).db'"` from cron (hot, consistent).
- [Litestream](https://litestream.io) continuously replicating `qsrv.db` to
  S3/R2/GCS: point-in-time restore with seconds of RPO, no code changes
  (the database already runs in WAL mode).

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

## Limits

- Single node: no replication or failover. Use Litestream (above) for
  disaster recovery with seconds of RPO.
- One process per data directory (it is locked).
- No per-key permissions: any valid key can do everything.
