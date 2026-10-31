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
export QSRV_API_KEYS="acme:billing:$(openssl rand -hex 32)"   # tenant:name:key, comma-separated
./bin/qsrv -data-dir /var/lib/qsrv -addr :8080    # or -api-keys-file keys.txt
```

| Flag / env | Default | |
|---|---|---|
| `-addr` / `QSRV_ADDR` | `:8080` | |
| `-data-dir` / `QSRV_DATA_DIR` | `./data` | one SQLite database per tenant (`tenants/<tenant>/qsrv.db`); flock'd, one process per dir. See [Data directory](#data-directory) |
| `-api-keys-file` / `QSRV_API_KEYS_FILE`, `QSRV_API_KEYS` | — | `tenant:name:key` entries; required unless `-insecure-no-auth`. See [API keys and tenants](#api-keys-and-tenants) |
| `-tls-cert`, `-tls-key` | — | serve HTTPS directly; otherwise put TLS in front |
| `-max-message-bytes` | 256KiB | |
| `-retention-check` | 1m | janitor interval |
| `-metrics-addr` / `QSRV_METRICS_ADDR` | `127.0.0.1:9464` | Prometheus `/metrics`, no auth; empty disables |
| `-log-level` / `QSRV_LOG_LEVEL` | info | JSON logs to stderr |

Signals: SIGTERM shuts down (long-polls get 503 immediately, pending writes
commit, exit 0); SIGHUP reloads the API keys.

## API keys and tenants

qsrv is multitenant. Every API key belongs to a **tenant**, and a key only
sees its tenant's queues and topics. Entries are `tenant:name:key`:

```text
# /etc/qsrv/keys  (-api-keys-file; QSRV_API_KEYS takes the same entries, comma separated)
acme:billing:9f2c...            # tenant acme, key "billing"
acme:emails:41ab...             # another key of acme: same queues
globex:app:7d0e...              # tenant globex
*:ops:c3a1...                   # admin: every tenant
```

| part | rules |
|---|---|
| tenant | `[a-z0-9][a-z0-9_-]{0,39}`, or `*` for an admin key |
| name | `[A-Za-z0-9_.-]{1,40}`: shows up in the access logs, not secret |
| key | the secret the client sends (`Authorization: Bearer <key>`), 32+ chars: `openssl rand -hex 32` |

**Tenant keys** work inside their tenant and don't change anything for
clients: they create and use `orders`, which qsrv stores as `acme/orders`.
Every name in a request resolves in the key's tenant, in paths and in bodies
(`dead_letter_queue`, a topic's queues, a replay's `to`). Another tenant's
queues, topics and leases look exactly like ones that don't exist (`404`), and
two tenants can have queues with the same name.

**Admin keys** (`*`) see every tenant and use full names: `GET /v1/queues`
returns `acme/orders`, and `qctl info acme/orders` works. A queue or topic
always belongs to a tenant, so an admin creating one must name it
`tenant/name`. Even admins can't make references across tenants (a DLQ, a
subscription or a replay into another tenant). `-insecure-no-auth` makes
every request an admin.

There is no tenant to create: a tenant exists as soon as a key names it, and
its queues appear when it creates them. Each tenant has **its own SQLite
database** (and its own writer and fsyncs), created with its first queue or
topic: a tenant's commits don't wait for another's, and a tenant's data is
one file you can back up, move or delete. Tenants still share the process,
CPU and disk bandwidth, and there are no per-tenant quotas: for hard
isolation run one qsrv per tenant.

**Rotation without restarts**: edit the keys file (or `QSRV_API_KEYS`) and
send `SIGHUP` (`kill -HUP <pid>`, `docker kill -s HUP qsrv`). The new set
applies to the next request; a file that doesn't parse is rejected and the
current keys stay (logged as an error). To rotate a key: add the new entry,
reload, move the clients, remove the old entry, reload.

**Access logs** carry `tenant` and `key` (the name). Metrics label queues
with their full name (`queue="acme/orders"`).

## Data directory

```
data/
  qsrv.db                    names without a tenant: unused by the server (only the Go library uses them)
  tenants/acme/qsrv.db       one database per tenant, each with its -wal and -shm
  tenants/globex/qsrv.db
  .wasmcache/                compiled SQLite (WASM build), safe to delete
```

Every queue, topic, message and its history live in their tenant's
database: dead letter queues, topics and replays never cross tenants, so
every write is a transaction in exactly one database. On startup qsrv opens
every `tenants/*/qsrv.db`; each open database costs about 1 MB of memory and
6 file descriptors (100 tenants: ~100 MB, 600 fds; raise `ulimit -n` if
needed).

## Docker

Multi-arch image (`linux/amd64`, `linux/arm64`) on GitHub Container Registry,
built from the `Dockerfile`: static binary on `distroless/static`, non-root,
no shell. It contains `qsrv` and `qctl`.

| tag | |
|---|---|
| `latest`, `X`, `X.Y`, `X.Y.Z` | releases (`vX.Y.Z` tags; `X` only from 1.0) |
| `edge`, `sha-<commit>` | every push to `main` |

```sh
docker run -d --name qsrv -p 8080:8080 \
  -e QSRV_API_KEYS="acme:app:$(openssl rand -hex 32)" \
  -v qsrv-data:/data \
  ghcr.io/romycode/qsrv:latest
```

- **Data**: `/data` (volume). It holds the databases (see [Data directory](#data-directory)) and the compiled-SQLite cache;
  one container per volume.
- **Config**: same flags as the binary (`docker run ... ghcr.io/romycode/qsrv -retention-check 5m`)
  or env vars. The image sets `QSRV_ADDR=:8080`, `QSRV_DATA_DIR=/data` and
  `QSRV_METRICS_ADDR=:9464` (metrics have no auth: don't publish 9464
  beyond your network). Prefer `QSRV_API_KEYS_FILE` with a mounted secret
  over keys in the environment.
- **Health**: built-in `HEALTHCHECK` (`qsrv -healthcheck`, which probes
  `/healthz`). For Kubernetes use `httpGet /healthz` on 8080.
- **Shutdown**: SIGTERM commits pending writes; give it a grace period
  (`docker stop -t 30`, `stop_grace_period`).
- **CLI inside**: `docker exec -e QSRV_ADDR=http://127.0.0.1:8080 -e QSRV_API_KEY=... qsrv qctl queues`.

Compose file with a volume and private metrics: [deploy/docker-compose.yml](../deploy/docker-compose.yml).

## Backups

The single-node weak point is the disk. Options, from least to most effort:

- `VACUUM INTO` every database from cron (hot, consistent per tenant):

  ```sh
  cd data && for db in qsrv.db tenants/*/qsrv.db; do
    mkdir -p "/backup/$(date +%F)/$(dirname "$db")"
    sqlite3 "$db" "VACUUM INTO '/backup/$(date +%F)/$db'"
  done
  ```

  To restore, stop qsrv and put the files back in the same layout. A single
  tenant can be restored alone.
- [Litestream](https://litestream.io) continuously replicating every database to
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
| `qsrv_disk_free_bytes`, `qsrv_disk_size_bytes`, `qsrv_db_size_bytes{tenant,file}` | gauge | capacity planning, WAL growth |
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
