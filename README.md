# qsrv

A durable message queue server you run as a single binary backed by SQLite.
Named FIFO queues over HTTP, with:

- **leases** with ack / nack / extend, and long polling;
- **retries** with exponential backoff and **dead-letter queues**;
- **ordering groups**: strict order per entity, parallel across entities;
- **delayed messages**;
- **topics**: publish once, every subscribed queue gets a copy, atomically;
- **history and replay** of processed messages;
- every acknowledged write is **fsynced**: a crash never loses what a client saw.

Clients for **Go** and **PHP**, a **Symfony Messenger** transport, a CLI,
Prometheus metrics and alerts. Pure Go (SQLite runs as WASM), static binaries
for any platform.

## Quickstart

```sh
git clone https://github.com/romycode/qsrv && cd qsrv
CGO_ENABLED=0 go build -o bin/ ./cmd/...                 # qsrv, qctl, qbench (Go 1.27+)
export PATH=$PWD/bin:$PATH

export QSRV_API_KEYS=$(openssl rand -hex 32)             # one or more, comma-separated
qsrv -data-dir ./data -addr :8080 &

export QSRV_ADDR=http://localhost:8080 QSRV_API_KEY=$QSRV_API_KEYS
qctl create -max-attempts 5 -backoff 1s orders
echo '{"order_id": 1}' | qctl send -group customer-42 orders
qctl recv -wait 20s orders                               # prints the message and its lease
qctl ack orders <lease>
```

With Docker:

```sh
docker run -d -p 8080:8080 -e QSRV_API_KEYS=$QSRV_API_KEYS -v qsrv-data:/data ghcr.io/romycode/qsrv:latest
```

Or over plain HTTP:

```sh
H="Authorization: Bearer $QSRV_API_KEY"
curl -sH "$H" -X PUT  localhost:8080/v1/queues/emails -d '{}'
curl -sH "$H" -X POST localhost:8080/v1/queues/emails/messages -d '{"body":"hello"}'
curl -sH "$H" -X POST 'localhost:8080/v1/queues/emails/receive?wait=20s'
```

## Clients

### Go

```go
import "github.com/romycode/qsrv/pkg/client"

c := client.New("http://localhost:8080", os.Getenv("QSRV_API_KEY"))
c.SendWith(ctx, "orders", body, client.SendOptions{GroupID: "customer-42"})

for {
    d, err := c.Receive(ctx, "orders", 20*time.Second) // nil, nil when nothing arrived
    if err != nil || d == nil {
        continue
    }
    if err := handle(d.Message.Bytes()); err != nil {
        c.Nack(ctx, "orders", d.LeaseID) // retried with backoff, then the DLQ
        continue
    }
    c.Ack(ctx, "orders", d.LeaseID)
}
```

Fan-out: `c.CreateTopic(ctx, "orders", "billing", "email")`, then
`c.Publish(ctx, "orders", body, client.SendOptions{})` writes one copy to each
queue in a single transaction.

Runnable example with groups, a delayed message, retries, a DLQ and lease
heartbeats: [examples/go](examples/go/main.go).

### PHP and Symfony Messenger

```sh
composer require romycode/qsrv-php
```

```php
$qsrv = new Romycode\Qsrv\Client('http://localhost:8080', $_ENV['QSRV_API_KEY']);
$qsrv->send('orders', json_encode($event), groupId: 'customer-42');
if ($d = $qsrv->receive('orders', wait: 20)) {
    handle($d->message->body);
    $qsrv->ack('orders', $d->leaseId);
}
```

```yaml
# Symfony: config/packages/messenger.yaml
framework:
    messenger:
        transports:
            async: 'qsrv://%env(QSRV_API_KEY)%@qsrv.internal:8080/app-async'
```

Docs: [php/README.md](php/README.md). Runnable examples:
[examples/php](examples/php) (plain client, and Messenger with a Worker).

### CLI

```sh
qctl queues | info NAME | create [flags] NAME | delete NAME
qctl send [-group G] [-delay D] QUEUE [BODY|-]
qctl recv [-wait D] [-ack] [-raw] QUEUE         # exit 3 if empty
qctl ack|nack QUEUE LEASE   ·   qctl extend [-by D] QUEUE LEASE
qctl consume -concurrency 8 QUEUE -- ./handler.sh   # stdin = body; exit 0 = ack, else nack
qctl replay -to DEST [-from T] [-until T] [-group G] [-states S] QUEUE
qctl topic create NAME [QUEUE...] | sub|unsub NAME QUEUE | publish [-group G] NAME [BODY|-]
```

`consume` heartbeats the lease while the command runs, exports
`QSRV_MESSAGE_ID`, `QSRV_GROUP_ID` and `QSRV_ATTEMPTS`, and on SIGINT lets
running jobs finish. `qctl <command> -h` lists each command's flags.

## Documentation

| | |
|---|---|
| [Concepts](docs/concepts.md) | leases, retries and DLQs, ordering groups, delays, topics, replay, guarantees |
| [HTTP API](docs/api.md) | every endpoint, request and response, error codes |
| [Operations](docs/operations.md) | configuration, Docker, backups, metrics and alerts, performance and sizing |
| [PHP](php/README.md) | client, Messenger transport, retries with Messenger vs qsrv |
| [Development](docs/development.md) | tests, benchmarks, CI, commits, releases, internals |
