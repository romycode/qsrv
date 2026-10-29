# qsrv for PHP

PHP client and Symfony Messenger transport for [qsrv](../README.md).
PHP 8.4+, Symfony 7.4 LTS or 8.x.

```sh
composer require romycode/qsrv
```

The package lives in the qsrv repository (`composer.json` at the root, code
in `php/`). Composer downloads only `php/src`: the Go server is excluded via
`.gitattributes`. Versions are the repository tags, shared with the Go module.

## Client

```php
use Romycode\Qsrv\Client;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\RetryPolicy;

$qsrv = new Client('http://qsrv.internal:8080', $_ENV['QSRV_API_KEY']);

$qsrv->createQueue('emails', new QueueConfig(
    visibilityTimeout: 60,
    retry: new RetryPolicy(maxAttempts: 5, initialBackoff: 1, maxBackoff: 300, multiplier: 2),
    deadLetterQueue: 'emails-dlq',
));

$qsrv->send('emails', json_encode($payload));
$qsrv->send('emails', $body, groupId: 'user-42');   // strict order within the group
$qsrv->send('emails', $body, delay: 30);            // deliverable in 30s

while ($d = $qsrv->receive('emails', wait: 20)) {   // long-poll up to 20s
    try {
        handle($d->message->body);
        $qsrv->ack('emails', $d->leaseId);
    } catch (\Throwable $e) {
        $qsrv->nack('emails', $d->leaseId);         // retry policy decides when it comes back
    }
}
```

Also `extend()` (heartbeat for long jobs), `getQueue()` (config + stats),
`listQueues()`, `deleteQueue()`, `replay()` and `healthy()`. Durations are
seconds (floats). Bodies are arbitrary bytes.

Errors: `ApiException` (HTTP status + API code) with subclasses
`NotFoundException`, `LeaseGoneException`, `QueueExistsException`,
`QueueFullException`, `UnauthorizedException`; `TransportException` when the
server can't be reached. All implement `Romycode\Qsrv\Exception\QsrvException`.

It uses `symfony/http-client`; pass your own `HttpClientInterface` as the
third argument (in Symfony, the `http_client` service gets you the profiler).

## Symfony Messenger

```yaml
# config/services.yaml
services:
    Romycode\Qsrv\Messenger\QsrvTransportFactory:
        tags: [messenger.transport_factory]
```

```yaml
# config/packages/messenger.yaml
framework:
    messenger:
        transports:
            async:
                dsn: 'qsrv://%env(QSRV_API_KEY)%@qsrv.internal:8080/app-async'  # qsrvs:// for https
                serializer: messenger.transport.symfony_serializer            # JSON, readable from Go
                options:
                    wait_time: 5
                    queue_config:
                        visibility_timeout: 5m
                        retention: 2160h
                retry_strategy:
                    max_retries: 3
                    delay: 1000
                    multiplier: 2
        routing:
            App\Message\SendEmail: async
```

```sh
bin/console messenger:setup-transports        # creates the queue (also automatic on first use)
bin/console messenger:consume async --keepalive
bin/console messenger:stats                   # messages waiting
```

| option | default | |
|---|---|---|
| `queue` | DSN path | queue name |
| `api_key` | DSN user | API key |
| `wait_time` | `5` | seconds a worker long-polls per fetch (0-20) |
| `auto_setup` | `true` | create the queue on first use if missing |
| `on_reject` | `ack` | `ack` or `nack`, see below |
| `timeout` | `10` | HTTP timeout, seconds |
| `queue_config` | server defaults | used when the transport creates the queue: `visibility_timeout`, `retry` (`max_attempts`, `initial_backoff`, `max_backoff`, `multiplier`), `dead_letter_queue`, `ordering`, `max_messages`, `retention`. Ignored if the queue already exists. |

### Stamps

- `QsrvGroupStamp('order-42')` on dispatch: sends the message in that
  ordering group (strict order, one at a time per group).
- `DelayStamp` (and Messenger's retry delays) map to qsrv's `delay` (max 720h).
- Received envelopes carry `QsrvReceivedStamp` (lease id, qsrv message id,
  qsrv attempts, group, lease expiry) and `TransportMessageIdStamp`.

### Retries: Messenger's or qsrv's

**Default (`on_reject: ack`): Messenger retries.** On failure Messenger re-sends
the message with a delay (`retry_strategy`) and rejects the original; reject
acks it in qsrv. After `max_retries` it goes to Messenger's `failure_transport`.
qsrv's own retry policy only applies to messages whose lease expired
(a worker crashed or a handler took longer than `visibility_timeout`).

**`on_reject: nack` with `retry_strategy: {max_retries: 0}`: qsrv retries.**
Reject nacks the lease, so qsrv's `retry` policy applies backoff and, when
exhausted, moves the message to the queue's `dead_letter_queue`. Don't set a
`failure_transport` for this transport: Messenger would copy the message there
on every failed attempt. Use this
with ordering groups: a Messenger retry is a *new* message at the end of the
group, so later messages of the group would overtake the failed one; a qsrv
nack keeps it at the head of its group.

Either way, processed messages stay in qsrv's history and can be replayed.

### Long handlers

A received message is leased for the queue's `visibility_timeout`. If a
handler can take longer, consume with `--keepalive`: the transport extends the
lease every few seconds while the handler runs. Without it, an expired lease
makes qsrv deliver the message again and the late `ack` fails with
`TransportException` ("lease expired").

### Wire format (other languages)

Each envelope is one qsrv message whose body is JSON:

```json
{"body": "<serializer output>", "headers": {"type": "App\\Message\\SendEmail", "Content-Type": "application/json"}}
```

`body_base64` replaces `body` if the serializer output isn't UTF-8 (e.g.
`PhpSerializer`). With `messenger.transport.symfony_serializer` the `body` is
the message as JSON and `headers.type` the PHP class, so a Go service can
consume these queues, or produce into them, by reading or writing that object
with the same headers. Bodies that are not this format are passed to the
serializer as they are, with no headers.

## Tests

```sh
composer install
composer test                                   # unit tests; integration tests are skipped
QSRV_API_KEYS=$(openssl rand -hex 32) go run ./cmd/qsrv -data-dir /tmp/qsrv-test -addr 127.0.0.1:18080 &
QSRV_TEST_URL=http://127.0.0.1:18080 QSRV_TEST_API_KEY=$QSRV_API_KEYS composer test
```

`vendor/` goes to `php/vendor`: a `vendor/` directory at the root would switch
the Go toolchain to vendor mode.
