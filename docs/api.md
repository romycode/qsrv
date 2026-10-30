# HTTP API

All endpoints are under `/v1` and need `Authorization: Bearer <key>`.
`/healthz` is public. Bodies are JSON; durations are Go strings (`"30s"`,
`"1m30s"`, `"2160h"`); times are RFC 3339.

```sh
export Q=http://localhost:8080 KEY=...      # helper for the examples below
alias qs='curl -sS -H "Authorization: Bearer $KEY" -H "Content-Type: application/json"'
```

## Queues

| | | |
|---|---|---|
| `GET /v1/queues` | list | `200 {"queues":["a","b"]}` |
| `PUT /v1/queues/{name}` | create | `201` queue · `409 already_exists` |
| `GET /v1/queues/{name}` | config + stats | `200` queue |
| `DELETE /v1/queues/{name}` | delete live messages **and history** | `204` · `422` while it is another queue's DLQ |

```sh
qs -X PUT $Q/v1/queues/orders -d '{
  "visibility_timeout": "1m",
  "retry": {"max_attempts": 5, "initial_backoff": "1s", "max_backoff": "1m", "multiplier": 2},
  "dead_letter_queue": "orders-dlq",
  "ordering": "available",
  "max_messages": 0,
  "retention": "2160h"
}'
```

Every field is optional; see [concepts.md](concepts.md#queues) for defaults.
A queue response is the config plus stats:

```json
{"name": "orders", "visibility_timeout": "1m0s", "retry": {...}, "dead_letter_queue": "orders-dlq",
 "ordering": "available", "retention": "2160h0m0s",
 "stats": {"messages": 12, "ready": 9, "delayed": 1, "in_flight": 2, "groups": 3, "history": 4031}}
```

`messages` counts live messages (ready + delayed + in flight + blocked behind
their group); `history` counts processed messages kept for replay.

## Messages

### Send: `POST /v1/queues/{name}/messages` → `201`

```sh
qs $Q/v1/queues/orders/messages -d '{"body": "{\"order_id\":1}", "group_id": "customer-42"}'
qs $Q/v1/queues/orders/messages -d '{"body_base64": "AAEC", "delay": "30s"}'
```

| field | |
|---|---|
| `body` / `body_base64` | exactly one: UTF-8 text, or any bytes base64-encoded |
| `group_id` | optional ordering group (max 128 bytes) |
| `delay` | optional, up to `720h` |

Returns the message (`id`, `enqueued_at`, ...). `429 queue_full` when
`max_messages` is reached; `413` over `-max-message-bytes`.

### Receive: `POST /v1/queues/{name}/receive?wait=20s` → `200` or `204`

Leases the next deliverable message. `wait` (max `20s`) long-polls; without
it the call returns at once. `204` = nothing available.

```json
{"message": {"id": "3ff08afa9c1e4b2d8a6f0c5e7d9b1a24", "group_id": "customer-42",
             "body": "{\"order_id\":1}", "attempts": 1, "enqueued_at": "2026-10-29T19:33:00.123Z"},
 "lease_id": "9fd20bcf41a8e6d37b0c2f5a8e1d4c96", "expires_at": "2026-10-29T19:34:00.123Z"}
```

`attempts` counts deliveries including this one; `replay_of` (the original id)
appears on replayed copies. Ids are 32 hex characters.

### Settle

| | |
|---|---|
| `POST /v1/queues/{name}/leases/{lease}/ack` | `204` processed |
| `POST /v1/queues/{name}/leases/{lease}/nack` | `204` failed: retry policy applies. With `{"dead_letter": true}` it skips retries: straight to the DLQ (dropped if none) |
| `POST /v1/queues/{name}/leases/{lease}/extend` `{"by":"1m"}` | `200 {"expires_at": ...}`; body optional (default: visibility timeout) |

All three return `410 lease_gone` if the lease expired or was already
settled: the message may already be with another consumer.

## Topics

A topic copies each published message into its subscribed queues, atomically
(see [concepts.md](concepts.md#topics-fan-out)). Topic and queue names are
separate namespaces.

| | | |
|---|---|---|
| `GET /v1/topics` | list | `200 {"topics":[{"name":"orders","queues":["billing","email"]}]}` |
| `PUT /v1/topics/{topic}` `{"queues":["billing"]}` | create; `queues` optional, must exist | `201` topic · `409 already_exists` · `422` unknown queue |
| `GET /v1/topics/{topic}` | | `200 {"name":"orders","queues":["billing","email"]}` |
| `DELETE /v1/topics/{topic}` | queues and their messages stay | `204` |
| `PUT /v1/topics/{topic}/subscriptions/{queue}` | subscribe (idempotent) | `200` topic · `422` unknown queue |
| `DELETE /v1/topics/{topic}/subscriptions/{queue}` | unsubscribe (idempotent) | `200` topic |

### Publish: `POST /v1/topics/{topic}/messages` → `201`

Same body as a send (`body` / `body_base64`, `group_id`, `delay`). Returns
the copy created in each queue:

```sh
qs $Q/v1/topics/orders/messages -d '{"body": "{\"order_id\":1}", "group_id": "customer-42"}'
```

```json
{"messages": [
  {"queue": "billing", "id": "9a1c...", "group_id": "customer-42", "body": "{\"order_id\":1}", "attempts": 0, "enqueued_at": "..."},
  {"queue": "email",   "id": "4be0...", "group_id": "customer-42", "body": "{\"order_id\":1}", "attempts": 0, "enqueued_at": "..."}
]}
```

`messages` is empty when the topic has no subscriptions. `429 queue_full` if
any subscribed queue is full: then no queue gets the message.

## Replay: `POST /v1/queues/{name}/replay` → `200`

```sh
qs $Q/v1/queues/orders/replay -d '{"to": "orders-replay", "from": "2026-10-01T00:00:00Z",
  "until": "2026-10-15T00:00:00Z", "group_id": "", "states": ["acked"], "limit": 1000}'
# {"replayed": 1000, "next_cursor": 81234}  -> repeat with "cursor": 81234
# {"replayed": 37}                          -> no next_cursor: done
```

`states`: any of `acked`, `dropped`, `dead_lettered` (default all). `limit`
default 1000, max 10000. Each page is atomic.

## Errors

```json
{"error": "queue \"x\" not found", "code": "not_found"}
```

| status | code | |
|---|---|---|
| 400 | `bad_request` | malformed request, bad duration, both or neither body fields |
| 401 | `unauthorized` | missing or wrong API key |
| 404 | `not_found` | queue or topic doesn't exist |
| 409 | `already_exists` | create of an existing queue or topic |
| 410 | `lease_gone` | lease expired or already settled |
| 413 | `bad_request` | message larger than `-max-message-bytes` |
| 422 | `invalid_config` | invalid settings or message; deleting a queue that is a DLQ or subscribed to a topic |
| 429 | `queue_full` | `max_messages` reached: back off and retry |
| 503 | `unavailable` | shutting down |
