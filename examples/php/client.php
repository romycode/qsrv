<?php

/*
 * Plain PHP client: create queues with retries and a DLQ, produce with
 * ordering groups and a delay, and consume with ack / nack / extend.
 *
 *   composer install
 *   QSRV_ADDR=http://localhost:8080 QSRV_API_KEY=... php client.php
 */

declare(strict_types=1);

require __DIR__.'/vendor/autoload.php';

use Romycode\Qsrv\Client;
use Romycode\Qsrv\Exception\LeaseGoneException;
use Romycode\Qsrv\Exception\NotFoundException;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\RetryPolicy;

const QUEUE = 'example-php-orders';
const DLQ = 'example-php-orders-dlq';

$qsrv = new Client(getenv('QSRV_ADDR') ?: 'http://localhost:8080', (string) getenv('QSRV_API_KEY'));

// --- Setup -----------------------------------------------------------------
// Up to 3 deliveries with 200ms, 400ms backoff, then the DLQ. Processed
// messages stay in history (replayable) for the default 90 days.
foreach ([QUEUE, DLQ] as $q) { // a DLQ can't be deleted while in use
    try {
        $qsrv->deleteQueue($q);
    } catch (NotFoundException) {
    }
}
$qsrv->createQueue(DLQ);
$qsrv->createQueue(QUEUE, new QueueConfig(
    visibilityTimeout: 5,                         // lease length in seconds
    retry: new RetryPolicy(maxAttempts: 3, initialBackoff: 0.2, maxBackoff: 1, multiplier: 2),
    deadLetterQueue: DLQ,
));

// --- Producer --------------------------------------------------------------
// Same group = strict order, one at a time. Different groups run in parallel.
$events = [
    ['order_id' => 1, 'customer' => 'alice', 'kind' => 'created'],
    ['order_id' => 1, 'customer' => 'alice', 'kind' => 'paid'],
    ['order_id' => 2, 'customer' => 'bob', 'kind' => 'created'],
    ['order_id' => 3, 'customer' => 'carol', 'kind' => 'broken'], // always fails: ends in the DLQ
];
foreach ($events as $e) {
    $m = $qsrv->send(QUEUE, json_encode($e, \JSON_THROW_ON_ERROR), groupId: $e['customer']);
    printf("sent     %s %s/%s\n", substr($m->id, 0, 8), $e['customer'], $e['kind']);
}
$qsrv->send(QUEUE, json_encode(['order_id' => 2, 'customer' => 'bob', 'kind' => 'reminder']), delay: 1.5);
echo "sent     reminder, deliverable in 1.5s\n";

// --- Consumer --------------------------------------------------------------
// One process here; run several for parallelism. Handlers must be
// idempotent: delivery is at-least-once.
$want = 4; // alice x2, bob, reminder
$handled = 0;
$deadline = microtime(true) + 30;
while ($handled < $want && microtime(true) < $deadline) {
    $d = $qsrv->receive(QUEUE, wait: 5); // long-poll: returns as soon as a message is ready
    if (null === $d) {
        continue;
    }
    $e = json_decode($d->message->body, true, flags: \JSON_THROW_ON_ERROR);
    try {
        if ('broken' === $e['kind']) {
            throw new RuntimeException('cannot process this order');
        }
        // A job longer than the visibility timeout would call
        // $qsrv->extend(QUEUE, $d->leaseId, 30) periodically.
        $qsrv->ack(QUEUE, $d->leaseId);
        ++$handled;
        printf("handled  %s %s/%s\n", substr($d->message->id, 0, 8), $e['customer'], $e['kind']);
    } catch (LeaseGoneException) {
        echo "lease expired before ack: it will be delivered again\n";
    } catch (RuntimeException $ex) {
        $qsrv->nack(QUEUE, $d->leaseId); // the queue's retry policy decides when it comes back
        printf("FAILED   %s %s/%s attempt %d: %s\n", substr($d->message->id, 0, 8), $e['customer'], $e['kind'], $d->message->attempts, $ex->getMessage());
    }
}

// The broken message needs 3 attempts (~0.6s of backoff) to reach the DLQ.
for ($i = 0; $i < 50 && 0 === $qsrv->getQueue(DLQ)->stats->messages; ++$i) {
    while ($d = $qsrv->receive(QUEUE)) {
        $qsrv->nack(QUEUE, $d->leaseId);
    }
    usleep(100_000);
}
$inDlq = $qsrv->getQueue(DLQ)->stats->messages;
printf("\nhandled %d/%d messages, %d in the DLQ\n", $handled, $want, $inDlq);
exit($handled === $want && 1 === $inDlq ? 0 : 1);
