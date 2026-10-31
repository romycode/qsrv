# Changelog

Server releases (`vX.Y.Z` tags). The PHP package has its own versions
(`php/vX.Y.Z` tags, see [php/README.md](php/README.md)). Format:
[Keep a Changelog](https://keepachangelog.com); versions follow
[SemVer](https://semver.org), and before 1.0 a minor version may break the
API or the data format (always called out here).

## [Unreleased]

### Added
- Admin UI at `/admin/` (on by default; `-admin=false` turns it off): queues
  and topics of every tenant or of one, with live counts, oldest message age
  and dead letters; per queue its settings and its messages, live and
  processed, with their bodies. Read-only. Sign in with an API key.
- Peek API: `GET /v1/queues/{name}/messages` (live or processed, filterable
  by state, paginated) and `GET /v1/queues/{name}/messages/{id}` (with body)
  look at messages without receiving them; `GET /v1/overview` returns every
  queue and topic of the key in one call. Go client: `PeekMessages`,
  `PeekMessage`, `Overview`.
- Grafana dashboards (overview, one queue, alerts) in `deploy/grafana`,
  with provisioning, and `deploy/monitoring`: qsrv, Prometheus,
  Alertmanager and Grafana in one compose file.
- `qsrv_queue_messages{state="waiting"}`: messages behind the head of their
  ordering group. The four states now add up to the queue's messages.
- The Docker image has a `/backup` directory owned by its user: a volume
  mounted there for `-backup-dir` is writable without a `chown`.

### Fixed
- `qsrv_messages_total` series now exist at 0 from the moment a queue is
  created or loaded. Before, a series appeared with its first event, so
  `increase()` and `rate()` missed it: the first dead letters of a queue (or
  after a restart) didn't trigger `QsrvDeadLetters`.

## [0.1.0] - 2026-11-01

First release. A durable message queue server: one binary, SQLite storage,
HTTP API.

### Queues
- Named FIFO queues with leases: receive, ack, nack, extend, long polling
  (up to 20s).
- Retries with exponential backoff (`max_attempts`: `0` retries forever, `1`
  never retries), dead letter queues, and nack with `dead_letter: true` to
  skip the retries.
- Ordering groups: strict order per group, parallel across groups.
- Delayed messages (up to 720h).
- Topics: one publish puts a copy in every subscribed queue, atomically.
- History of processed messages, kept for the queue's `retention`, and replay
  of it into any queue of the same tenant.
- `max_messages` per queue for backpressure (`429 queue_full`).

### Durability
- Every write that is answered (201 on send, 204 on ack) is committed and
  fsynced first; concurrent operations share one transaction and one fsync
  (group commit).
- A crash test kills the server with SIGKILL repeatedly under load, with both
  SQLite drivers, on every change: no confirmed send lost, no confirmed ack
  redelivered.
- Snapshots: `-backup-dir`, `-backup-every`, `-backup-keep`. Consistent copies
  of the database, written atomically, rotated, monitored.
  `-restore-tenant T -from SNAPSHOT` puts one tenant back.
- Free-disk guard: below `-min-free-disk` (1GiB) new data is refused with
  `503 storage_full` while consumers keep draining, instead of crashing on a
  full disk.
- Schema version in every database (`PRAGMA user_version`); a database from a
  newer qsrv is refused.

### Multitenancy and security
- API keys are `tenant:name:key`; a key only sees its tenant's queues and
  topics. Admin keys (`*`) see every tenant. References never cross tenants.
- Keys from a file and/or `QSRV_API_KEYS`, reloaded on SIGHUP.
- Optional TLS (`-tls-cert`, `-tls-key`).

### Operations
- Prometheus metrics (queues, latencies, commits, disk, snapshots) and alert
  rules in `deploy/prometheus/alerts.yml`.
- Docker image `ghcr.io/romycode/qsrv` (amd64, arm64): native SQLite through
  cgo, linked statically, on distroless, non-root, with a health check.
- Downloadable binaries: pure Go (SQLite as WASM), for Linux, macOS and
  Windows.
- `qctl` CLI (queues, send, receive, consume with a command, replay, topics)
  and `qbench` load generator.

### Clients
- Go client: `github.com/romycode/qsrv/pkg/client`.
- PHP client and Symfony Messenger transport: `romycode/qsrv-php`, released
  separately.

[Unreleased]: https://github.com/romycode/qsrv/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/romycode/qsrv/releases/tag/v0.1.0
