# qsrv for PHP

PHP client and Symfony Messenger transport for [qsrv](https://github.com/romycode/qsrv).
PHP 8.4+, Symfony 7.4 LTS or 8.x.

```sh
composer require romycode/qsrv-php
```

Developed in the `php/` directory of [romycode/qsrv](https://github.com/romycode/qsrv)
and published with `scripts/release-php.sh` to the read-only mirror
[romycode/qsrv-php](https://github.com/romycode/qsrv-php), which Packagist reads. Issues and pull requests go to romycode/qsrv. The PHP
package has its own versions, independent of the server's.

Runnable examples (plain client, and Messenger with a Worker) are in
[examples/php](https://github.com/romycode/qsrv/tree/main/examples/php).

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
        // $qsrv->nack('emails', $d->leaseId, deadLetter: true);  // no retries: straight to the DLQ
    }
}
```

Fan-out with topics: every subscribed queue gets its own copy, atomically.

```php
$qsrv->createTopic('orders', ['billing', 'email']);   // or $qsrv->subscribe('orders', 'email')
$copies = $qsrv->publish('orders', json_encode($event), groupId: 'customer-42');
// ['billing' => Message, 'email' => Message]
```

Also `extend()` (heartbeat for long jobs), `getQueue()` (config + stats),
`listQueues()`, `deleteQueue()`, `replay()` and `healthy()`. Durations are
seconds (floats). Bodies are arbitrary bytes.

Errors: `ApiException` (HTTP status + API code) with subclasses
`NotFoundException`, `LeaseGoneException`, `QueueExistsException`,
`QueueFullException`, `StorageFullException` (server low on disk: retry
later), `UnauthorizedException`; `TransportException` when the
server can't be reached. All implement `Romycode\Qsrv\Exception\QsrvException`.

qsrv is multitenant: use an API key of your tenant and plain queue names
(`orders`); the server scopes them to the key's tenant.

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
                    # native_retries: true (default) -> qsrv retries, configured below
                    queue_config:
                        visibility_timeout: 5m          # longest a handler may run (or use --keepalive)
                        retry:
                            max_attempts: 5             # 1 delivery + 4 retries
                            initial_backoff: 1s
                            multiplier: 2               # 1s, 2s, 4s, 8s
                            max_backoff: 1m
                        dead_letter_queue: app-async-dlq
                        retention: 336h                 # unprocessed messages expire after this (default 14 days)
                        # persistent: true              # instead: never expire, keep processed messages for replay
        routing:
            App\Message\SendEmail: async
```

```sh
bin/console messenger:setup-transports        # creates the DLQ and the queue (also automatic on first use)
bin/console messenger:consume async --keepalive
bin/console messenger:stats                   # messages waiting
```

### Retries: `native_retries`

Who retries a failed message is the one decision to make per transport.

| | `native_retries: true` (default) | `native_retries: false` |
|---|---|---|
| retried by | **qsrv**: the queue's `queue_config.retry` | **Symfony**: the transport's `retry_strategy` |
| a retry is | the **same** message, redelivered with backoff | a **new** message, sent with Messenger's delay |
| ordering groups | kept: the failed message stays at the head of its group | **broken**: the retry goes to the end of its group |
| when retries run out | the queue's `dead_letter_queue` (dropped if none) | Messenger's `failure_transport` |
| unrecoverable errors¹ | straight to the `dead_letter_queue` | straight to the `failure_transport` |
| inspect / retry failed | `qctl recv`, `qctl replay` on the DLQ | `messenger:failed:show`, `messenger:failed:retry` |
| configure | `queue_config.retry`, `queue_config.dead_letter_queue` | `retry_strategy`, `failure_transport` |
| ignored | `retry_strategy` delays and counts² | `queue_config.retry`³ |

¹ `UnrecoverableMessageHandlingException`, or any exception when
`max_retries: 0`. ² Messenger still decides *whether* a failure is retryable;
qsrv decides *when* and *how many times*. Leave `max_retries` above 0.
³ Except for messages whose lease expired (a crashed worker, or a handler
running past `visibility_timeout` without `--keepalive`): qsrv always
redelivers those, with the queue's policy.

**To use Symfony's retries, pass `native_retries: false`:**

```yaml
transports:
    async:
        dsn: 'qsrv://%env(QSRV_API_KEY)%@qsrv.internal:8080/app-async'
        options:
            native_retries: false     # <- Messenger's retry_strategy and failure_transport
        retry_strategy:
            max_retries: 3
            delay: 1000
            multiplier: 2
        failure_transport: failed
```

Rules of thumb:

- Use the default (qsrv) with `QsrvGroupStamp`: Symfony's retries reorder
  groups. It is also the only mode where a message keeps its id and its
  attempt count (`QsrvReceivedStamp::$attempts`) across retries.
- With `native_retries: true`, don't set a `failure_transport` for the
  transport: unrecoverable messages would land both there and in the DLQ.
- **No retries at all**: `queue_config: {retry: false}` (same as
  `max_attempts: 1`). The first failure goes to the DLQ. `max_attempts: 0`
  means the opposite: retry forever.
- Fields of `retry` you leave out take qsrv's defaults: 5 attempts, 1s
  backoff doubling up to 1m. `initial_backoff: 0` retries immediately.
- Messages that can't be decoded go straight to the DLQ in both modes:
  retrying can't fix them.

### Queue settings are applied when the queue is created

`queue_config` is used by `messenger:setup-transports` (and by `auto_setup`
on first use) to create the queue. qsrv can't change an existing queue's
settings, so if the queue exists with different ones, `setup-transports`
fails and lists them, e.g. `retry.max_attempts: configured 8, queue has 5`.
Settings you don't configure aren't checked. To change them: delete the queue
(`qctl delete`, drops its messages and history) and run `setup-transports`
again, or create a new queue and point the transport at it.

### Options

| option | default | |
|---|---|---|
| `native_retries` | `true` | who retries failures: qsrv (`true`) or Symfony (`false`). See above |
| `queue_config` | qsrv defaults | settings to create the queue with: `visibility_timeout`, `retry` (`false`, or `max_attempts`, `initial_backoff`, `max_backoff`, `multiplier`), `dead_letter_queue` (created too), `ordering`, `max_messages`, `retention`, `persistent`. Durations as `30s` or seconds. Options only, not the DSN |
| `queue` | DSN path | queue name |
| `api_key` | DSN user | API key |
| `wait_time` | `5` | seconds a worker long-polls per fetch (0-20) |
| `auto_setup` | `true` | create the queue on first use if missing |
| `timeout` | `10` | HTTP timeout, seconds |
| `topic` | — | `send` publishes to this topic instead of the queue (the queue is then optional: a publish-only transport) |
| `subscribe` | — | topics this queue subscribes to on setup (created if missing); list, or comma-separated in the DSN |

### Topics: one event, several services

The producing app publishes to a topic; each consuming app has its own queue
subscribed to it, so each gets every event with its own retries and DLQ:

```yaml
# producer app
framework:
    messenger:
        transports:
            order_events: 'qsrv://%env(QSRV_API_KEY)%@qsrv:8080?topic=orders'
        routing:
            App\Event\OrderPlaced: order_events

# billing app (same in the email app, with its own queue)
framework:
    messenger:
        transports:
            orders: 'qsrv://%env(QSRV_API_KEY)%@qsrv:8080/billing-orders?subscribe=orders'
```

`messenger:setup-transports` in the billing app creates `billing-orders`,
creates the `orders` topic if needed and subscribes the queue. A
publish-only transport can't be consumed. Sending adds `QsrvPublishedStamp`
with the message id in each queue.

### Stamps

- `QsrvGroupStamp('order-42')` on dispatch: sends the message in that
  ordering group (strict order, one at a time per group).
- `DelayStamp` maps to qsrv's `delay` (max 720h).
- Received envelopes carry `QsrvReceivedStamp` (lease id, qsrv message id,
  qsrv attempts, group, lease expiry, queue) and `TransportMessageIdStamp`.

### Long handlers

A received message is leased for the queue's `visibility_timeout`. If a
handler can take longer, consume with `--keepalive`: the transport extends the
lease every few seconds while the handler runs. Without it, an expired lease
makes qsrv deliver the message again (counting it as a failed attempt) and
the late `ack` fails with `TransportException` ("lease expired").

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

From `php/` in the qsrv repository:

```sh
composer install
composer test                                   # unit tests; integration tests are skipped
KEY=$(openssl rand -hex 32)
QSRV_API_KEYS="test:phpunit:$KEY" go run ../cmd/qsrv -data-dir /tmp/qsrv-test -addr 127.0.0.1:18080 &
QSRV_TEST_URL=http://127.0.0.1:18080 QSRV_TEST_API_KEY=$KEY composer test
```
