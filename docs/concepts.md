# Concepts

How qsrv behaves, and what you can rely on. For the endpoints see
[api.md](api.md).

## Queues

A queue is a named, durable FIFO list of messages (`[A-Za-z0-9_.-]`, up to 80
chars). Each queue has its own settings, fixed at creation:

| setting | default | |
|---|---|---|
| `visibility_timeout` | `30s` | how long a received message stays leased (1s-12h) |
| `retry` | 5 attempts, 1s backoff ×2, max 1m | see [Retries](#retries-backoff-and-dead-letter-queues) |
| `dead_letter_queue` | none | where exhausted messages go (must exist) |
| `ordering` | `available` | see [Ordering](#ordering-and-groups) |
| `max_messages` | `0` (unlimited) | live messages; sends beyond it get `429 queue_full` (backpressure) |
| `retention` | `2160h` (90 days) | how long processed messages are kept for replay; `0` = forever |

Messages are opaque bytes (up to `-max-message-bytes`, 256 KiB by default).
UTF-8 bodies travel as `body`, anything else as `body_base64`; clients handle
that for you.

## Leases: receive, ack, nack, extend

Receiving a message does not remove it: it **leases** it for the queue's
`visibility_timeout`. Within the lease the consumer settles it:

- **ack**: processed. It leaves the queue and becomes history.
- **nack**: failed. The retry policy decides when it is delivered again.
  `nack` with `dead_letter` skips the retries and sends it to the DLQ now:
  for failures a retry can't fix (a payload that can't be decoded).
- **extend**: heartbeat for long jobs; the lease now ends `by` from now
  (default: the visibility timeout again).

If the lease expires (consumer crashed, hung, or took too long) the message
counts as failed, exactly like a nack. A late ack then gets `410 lease_gone`:
another consumer may already have it.

**Delivery is at-least-once: consumers must be idempotent.** A crash between
your side effect and the ack means the message is delivered again.

### Long polling

`receive?wait=20s` waits up to 20s for a message and returns the moment one
becomes available, so idle consumers don't hammer the server. Without
`wait` it returns immediately (`204` when empty). Every waiting consumer of a
queue is woken on a send; one gets the message, the rest wait again (fine up
to hundreds of consumers per queue).

## Retries, backoff and dead-letter queues

A failed delivery (nack or lease expiry) applies the queue's `retry` policy:

| field | default | |
|---|---|---|
| `max_attempts` | `5` | total deliveries: 5 = 1 delivery + 4 retries. `0` = retry forever (a poison message then loops forever) |
| `initial_backoff` | `1s` | wait before the first retry |
| `multiplier` | `2` | exponential backoff: 1s, 2s, 4s, 8s... (`< 1` = constant) |
| `max_backoff` | `1m` | cap on the wait |

- While backing off, a message is `delayed` and does **not** block the rest
  of the queue. When due it returns to its **original FIFO position**.
- In an ordering group it stays the group's head: the group waits for it.
- When attempts run out it goes to the `dead_letter_queue` (same id, body and
  group), or is `dropped` if there is none. Either way it stays in history and
  can be replayed, and it unblocks its group.
- No jitter: messages that fail together retry together.

## Ordering and groups

`ordering: available` (default): FIFO among messages that are deliverable now.
A message in flight or backing off does not hold back the ones behind it, so
consumers run in parallel.

Messages sent with a **`group_id`** are delivered **strictly in order within
the group, one at a time**: message N+1 is not delivered until N is acked or
dead-lettered. Different groups run in parallel. Use the entity id (order,
customer, account) as the group to get per-entity ordering without
serializing the whole queue. Holds across restarts and crashes.

`ordering: strict`: the whole queue is one group. Total order, one message in
flight at a time.

## Delayed messages

`delay` on send (up to 720h) makes a message deliverable only after that
time. It keeps its send-order position and does not block later messages; in
a group it also waits for the messages before it. Useful for scheduled work
and for client-side retries (Symfony Messenger's retries use it).

## Topics (fan-out)

A queue is point to point: each message is processed by one consumer. When
several services must each get every event, give each its own queue and
**subscribe** those queues to a **topic**. Publishing to the topic inserts one
copy of the message into every subscribed queue.

```sh
qctl create billing && qctl create email
qctl topic create orders billing email        # or: qctl topic sub orders email
echo '{"order_id": 1}' | qctl topic publish -group customer-42 orders
```

- **Atomic**: all copies are written in one transaction. Every subscriber
  gets the message or none does: if one queue is full (`max_messages`) the
  publish fails with `429` and no queue gets it.
- **Independent copies**: each copy is an ordinary message of its queue (own
  id), with that queue's visibility timeout, retries, DLQ, ordering groups,
  retention and replay. A slow or failing subscriber doesn't affect the others.
  `group_id` and `delay` apply in each queue.
- **From now on**: a new subscription gets messages published after it, not
  earlier ones (replay the publisher's history if you need them).
- **No subscribers**: the publish succeeds and the message is dropped.
- A subscribed queue can't be deleted: unsubscribe it first. Deleting a topic
  keeps its queues and their messages. Up to 64 queues per topic.
- Each copy stores the body, so a topic with N queues uses N times the space.

## History, retention and replay

Ack does not delete. Processed messages stay in the database with their final
state: `acked`, `dropped` or `dead_lettered`. A janitor deletes history older
than the queue's `retention`. Live messages are never purged.

**Replay** copies history of a queue into a destination queue as **new
messages**: new id, same body and `group_id`, `replay_of` = original id, in
original order. Filter by `enqueued_at` range, group and final state. It is
paginated with a cursor, and each page is atomic.

Replay into a dedicated queue unless your consumers tolerate old events mixed
with live traffic: replaying into a live queue interleaves old and new
messages of the same group.

```sh
qctl create -retention 0 orders-replay
qctl replay -to orders-replay -from 2026-10-01 -until 2026-10-15 orders
qctl replay -to orders-replay -group customer-42 -states acked orders
```

## Guarantees

- **Durability**: every result a client sees (send `201`, receive `200`,
  ack/nack `204`, extend `200`, replay `200`) is returned only after the SQLite
  transaction commits with `synchronous=FULL`. `kill -9` or power loss never
  rolls back something a client observed.
- **At-least-once** delivery, as above.
- **Ordering** as described, across crashes.
- **Single node**: no replication. See [operations.md](operations.md#backups)
  for backups and point-in-time restore.

## Not included

No batch endpoints, no deduplication, no per-key permissions (any valid API
key can do everything), no priorities, no content-based filtering on topics
(every subscriber gets every message).
