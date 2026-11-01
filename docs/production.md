# Production checklist

What to set up before running qsrv for real, and what it does and doesn't
promise. Details of every flag are in [operations.md](operations.md).

## What qsrv promises

- A send or publish answered `201` is on disk (committed and fsynced) and
  will be delivered, after any number of crashes or restarts.
- An ack answered `204` is final: that message is not delivered again.
- Delivery is **at-least-once**: a message whose ack didn't get its 204 (the
  consumer crashed, the lease expired, the server restarted) comes back.
  Consumers must be idempotent.
- Order is FIFO per queue, and strict per ordering group.

Both durability promises are tested on every change by killing the server
with SIGKILL under load (`internal/e2e`). A power cut additionally relies on
the disk honoring fsync: use real disks or cloud block storage (EBS, PD), not
a network filesystem or a disk with a volatile write cache.

## What it is not

- **One node.** No replication, no failover: a deploy or a restart is a few
  seconds without service (clients retry), and losing the disk loses what is
  newer than the last snapshot. If that is not acceptable, use a replicated
  broker.
- **Tenants share resources.** Names and data are isolated; CPU, memory and
  disk are not, and there are no per-tenant quotas. A tenant that needs hard
  isolation gets its own qsrv (see [operations.md](operations.md#api-keys-and-tenants)).

## Checklist

**Storage**
- [ ] `-data-dir` on a local SSD or block volume, not NFS. One qsrv per
  directory (it is locked).
- [ ] `-min-free-disk` above a few minutes of traffic (default 1GiB). Below it
  qsrv refuses new messages with `503 storage_full` instead of crashing.
- [ ] Sized for persistent queues: they keep every processed message until
  the queue is deleted, roughly `messages/day × (body + ~200 B) × days`,
  plus the WAL. Only make persistent the queues you need to replay.

**Backups**
- [ ] `-backup-dir` on **another disk**, or synced off the machine.
- [ ] `-backup-every` = how much you accept to lose if the disk dies.
- [ ] Restore rehearsed once: whole server, and `-restore-tenant`
  ([operations.md](operations.md#backups)).

**Security**
- [ ] Keys in a file (`QSRV_API_KEYS_FILE`, a mounted secret), one per
  application, 32+ random bytes; rotated with SIGHUP.
- [ ] TLS: `-tls-cert`/`-tls-key`, or a proxy in front. Never plain HTTP across
  networks you don't control.
- [ ] Metrics (`-metrics-addr`) on localhost or a private interface: no auth.
- [ ] Admin keys (`*`) only for operators, not for applications.

**Monitoring**
- [ ] Prometheus scraping `/metrics`, with `deploy/prometheus/alerts.yml`
  loaded and the `TUNE` thresholds reviewed.
- [ ] Alerts routed, at least: `QsrvDown`, `QsrvMessagesTooOld`,
  `QsrvDLQNotEmpty`, `QsrvStorageFull`, `QsrvBackupStale`,
  `QsrvDiskSpaceCritical`, `QsrvWriterBacklog`.
- [ ] `/healthz` in the load balancer or orchestrator.

**Runtime**
- [ ] Docker image (native SQLite; `deploy/docker-compose.yml`) or the binary
  as a systemd service (`deploy/qsrv.service`).
- [ ] Shutdown grace period of 30s (`stop_grace_period`,
  `TimeoutStopSec`): SIGTERM commits pending writes.
- [ ] `ulimit -n` above the expected connections (long-polls hold one each).

**Queues**
- [ ] Every queue with a `dead_letter_queue`, and someone watching it.
- [ ] `retention` (default 14 days) longer than the longest outage of its
  consumers you will tolerate: past it, waiting messages are deleted
  unprocessed. `QsrvMessagesExpired` routed. Persistent queues never expire.
- [ ] `visibility_timeout` above the slowest handler, or consumers extending
  the lease (Messenger: `--keepalive`).
- [ ] `max_messages` on queues whose producers can outrun their consumers.

## Sizing

Measured on 2 vCPUs, cgo driver, 100 tenants, 10KB messages, 200 msg/s
(about 17M messages/day), with the load generator on the same machine:
17% of one core, 38 MiB RSS, send p50 52ms (dominated by fsync on that
disk). Memory grows with **live** (not yet acked) messages, ~200 B each: a
backlog of 10M messages is ~2 GB. Disk grows with history: see above.
Run `qbench -tenants N -size S -rate R` against your own hardware before
deciding.
