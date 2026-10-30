<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Messenger;

use Romycode\Qsrv\Client;
use Romycode\Qsrv\Exception\ApiException;
use Romycode\Qsrv\Exception\LeaseGoneException;
use Romycode\Qsrv\Exception\NotFoundException;
use Romycode\Qsrv\Exception\QsrvException;
use Romycode\Qsrv\Exception\QueueExistsException;
use Romycode\Qsrv\Model\Delivery;
use Romycode\Qsrv\Model\QueueConfig;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\RedeliveryStamp;
use Symfony\Component\Messenger\Stamp\SentForRetryStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\KeepaliveReceiverInterface;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\SetupableTransportInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

/**
 * Symfony Messenger transport backed by one qsrv queue and, optionally, topics.
 *
 * - send: one qsrv message per envelope (see WireFormat); DelayStamp maps to
 *   qsrv's delay, QsrvGroupStamp to its ordering group. With a `topic`, send
 *   publishes there instead: every subscribed queue gets a copy.
 * - get: leases messages, long-polling up to wait_time seconds.
 * - ack / reject: settle the lease. Failures depend on $nativeRetries, below.
 * - keepalive (consume --keepalive): extends the lease.
 * - setup: creates the dead letter queue, the queue, the `topic`, and the
 *   `subscribe` topics with this queue subscribed to them.
 *
 * Retries. Messenger reports a failure in one of two ways depending on the
 * version (7.4 acks a message it sent for retry, 8.x always rejects); both
 * carry a SentForRetryStamp saying whether Messenger chose to retry.
 *
 * - $nativeRetries = true (default): qsrv retries. Messenger's retry send is
 *   dropped, and the failure becomes a nack, so the queue's retry policy
 *   (backoff, max attempts, dead letter queue) applies and the message keeps
 *   its place in its ordering group. If Messenger chose not to retry (an
 *   UnrecoverableMessageHandlingException, or max_retries: 0) the message goes
 *   straight to the dead letter queue. Messenger's retry_strategy delays and
 *   counts are not used.
 * - $nativeRetries = false: Messenger retries. Its retry is a new qsrv message
 *   sent with Messenger's delay (at the end of its group), the failed one is
 *   acked, and exhausted messages go to Messenger's failure_transport.
 */
final class QsrvTransport implements TransportInterface, MessageCountAwareInterface, SetupableTransportInterface, KeepaliveReceiverInterface
{
    public function __construct(
        private readonly Client $client,
        private readonly string $queue,
        private readonly SerializerInterface $serializer,
        /** Seconds a get() long-polls for the first message (0-20). */
        private readonly float $waitTime = 5,
        private readonly bool $autoSetup = true,
        /** true: qsrv's retry policy and DLQ; false: Messenger's retry_strategy and failure_transport. */
        private readonly bool $nativeRetries = true,
        private readonly QueueConfig $queueConfig = new QueueConfig(),
        /** Publish here on send instead of sending to the queue. */
        private readonly ?string $topic = null,
        /** @var list<string> topics this queue subscribes to on setup */
        private readonly array $subscribe = [],
    ) {
        if ('' === $queue && null === $topic) {
            throw new \InvalidArgumentException('qsrv: a transport needs a queue, a topic, or both');
        }
        if ('' === $queue && [] !== $subscribe) {
            throw new \InvalidArgumentException('qsrv: "subscribe" needs a queue to subscribe');
        }
    }

    public function getQueueName(): string
    {
        return $this->queue;
    }

    public function getTopicName(): ?string
    {
        return $this->topic;
    }

    public function usesNativeRetries(): bool
    {
        return $this->nativeRetries;
    }

    public function send(Envelope $envelope): Envelope
    {
        // Messenger's retry of a message this transport delivered: it goes
        // back to this queue (never to the topic, which would copy it to every
        // subscriber again), or, with native retries, is left to qsrv.
        $retryOf = $this->retryOf($envelope);
        if (null !== $retryOf && $this->nativeRetries) {
            return $envelope; // ack()/reject() will nack it: qsrv redelivers the original
        }

        $body = WireFormat::encode($this->serializer->encode($envelope));
        $delay = max(0, ($envelope->last(DelayStamp::class)?->getDelay() ?? 0) / 1000);
        $group = $envelope->last(QsrvGroupStamp::class)?->groupId ?? $retryOf?->groupId;

        if (null !== $this->topic && null === $retryOf) {
            $copies = $this->call(fn () => $this->client->publish($this->topic, $body, $group, $delay));

            return $envelope->with(new QsrvPublishedStamp($this->topic, array_map(static fn ($m) => $m->id, $copies)));
        }
        $message = $this->call(fn () => $this->client->send($this->queue, $body, $group, $delay));

        return $envelope->withoutAll(TransportMessageIdStamp::class)->with(new TransportMessageIdStamp($message->id));
    }

    /**
     * Yields up to $fetchSize envelopes. Only the first receive long-polls;
     * each next lease is taken after the previous envelope was handled, so
     * leases don't tick while earlier messages are processed.
     *
     * @return iterable<Envelope>
     */
    public function get(int $fetchSize = 1): iterable
    {
        $this->requireQueue();
        for ($i = 0; $i < max(1, $fetchSize); ++$i) {
            $wait = 0 === $i ? $this->waitTime : 0;
            $delivery = $this->call(fn () => $this->client->receive($this->queue, $wait));
            if (null === $delivery) {
                return;
            }
            yield $this->decode($delivery);
        }
    }

    /**
     * Handled: acks. Messenger 7.4 also acks a failed message it sent for
     * retry (it carries SentForRetryStamp): that is a failure, not a success.
     */
    public function ack(Envelope $envelope): void
    {
        $failure = $envelope->last(SentForRetryStamp::class);
        if (null !== $failure) {
            $this->fail($envelope, $failure->isSent);

            return;
        }
        $stamp = $this->stamp($envelope);
        try {
            $this->client->ack($this->queue, $stamp->leaseId);
        } catch (LeaseGoneException $e) {
            throw new TransportException(\sprintf('qsrv: lease of message %s expired before ack; it will be delivered again (increase visibility_timeout or consume with --keepalive)', $stamp->messageId), 0, $e);
        } catch (QsrvException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    /** Failed. Without a SentForRetryStamp (reject outside the worker) qsrv's retry policy decides. */
    public function reject(Envelope $envelope): void
    {
        $this->fail($envelope, $envelope->last(SentForRetryStamp::class)?->isSent ?? true);
    }

    /**
     * Extends the lease. $seconds is Messenger's keepalive interval; the lease
     * is extended to twice that so one late tick doesn't lose it. Without an
     * interval the queue's visibility timeout is used.
     */
    public function keepalive(Envelope $envelope, ?int $seconds = null): void
    {
        $stamp = $this->stamp($envelope);
        $this->call(fn () => $this->client->extend($this->queue, $stamp->leaseId, null === $seconds ? null : (float) max(1, 2 * $seconds)), false);
    }

    /** Messages waiting to be consumed: ready, delayed or blocked behind their group (not in flight). */
    public function getMessageCount(): int
    {
        if ('' === $this->queue) {
            return 0; // send-only transport
        }
        $stats = $this->call(fn () => $this->client->getQueue($this->queue))->stats;

        return max(0, $stats->messages - $stats->inFlight);
    }

    /**
     * messenger:setup-transports: creates the queue's dead letter queue (with
     * default settings) and the queue with queue_config, the topic, and the
     * subscribe topics with the queue subscribed.
     *
     * qsrv can't change an existing queue's settings: if the queue exists with
     * settings that differ from the ones configured, this fails and says which,
     * instead of silently running with the old ones.
     */
    public function setup(): void
    {
        try {
            if ('' !== $this->queue) {
                if (null !== $dlq = $this->queueConfig->deadLetterQueue) {
                    $this->ignoreExists(fn () => $this->client->createQueue($dlq));
                }
                try {
                    $this->client->createQueue($this->queue, $this->queueConfig);
                } catch (QueueExistsException) {
                    $this->checkExistingQueue();
                }
            }
            if (null !== $this->topic) {
                $this->ignoreExists(fn () => $this->client->createTopic($this->topic));
            }
            foreach ($this->subscribe as $topic) {
                $this->ignoreExists(fn () => $this->client->createTopic($topic));
                $this->client->subscribe($topic, $this->queue);
            }
        } catch (QsrvException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    private function checkExistingQueue(): void
    {
        $diff = $this->queueConfig->diff($this->client->getQueue($this->queue)->config);
        if ([] === $diff) {
            return;
        }
        throw new TransportException(\sprintf(
            'qsrv: queue "%s" already exists with different settings (%s). qsrv cannot change the settings of an existing queue: '
            .'align queue_config with the queue, or delete it and run messenger:setup-transports again (deleting drops its messages and history).',
            $this->queue,
            implode('; ', array_map(static fn ($k, $v) => "$k: configured {$v[0]}, queue has {$v[1]}", array_keys($diff), $diff)),
        ));
    }

    /** A Messenger retry of a message received from this queue, or null. */
    private function retryOf(Envelope $envelope): ?QsrvReceivedStamp
    {
        $received = $envelope->last(QsrvReceivedStamp::class);
        if (null === $received || '' === $this->queue || $received->queue !== $this->queue || null === $envelope->last(RedeliveryStamp::class)) {
            return null;
        }

        return $received;
    }

    /** Settles a failed message: $retry says whether Messenger chose to retry it. */
    private function fail(Envelope $envelope, bool $retry): void
    {
        $stamp = $this->stamp($envelope);
        try {
            match (true) {
                // Messenger owns retries: it already re-sent the message, or gave up
                // (failure_transport). Either way this copy is done.
                !$this->nativeRetries => $this->client->ack($this->queue, $stamp->leaseId),
                // Messenger 8.x: undecodable payload. Retrying can't fix it.
                $envelope->getMessage() instanceof MessageDecodingFailedException => $this->client->nack($this->queue, $stamp->leaseId, deadLetter: true),
                $retry => $this->client->nack($this->queue, $stamp->leaseId),
                default => $this->client->nack($this->queue, $stamp->leaseId, deadLetter: true),
            };
        } catch (LeaseGoneException) {
            // The lease expired: qsrv already counted it as a failure and will redeliver.
        } catch (QsrvException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    private function ignoreExists(callable $fn): void
    {
        try {
            $fn();
        } catch (QueueExistsException) { // queue or topic: same API code
        }
    }

    private function requireQueue(): void
    {
        if ('' === $this->queue) {
            throw new \LogicException(\sprintf('qsrv: this transport only publishes to topic "%s"; set a queue to consume', $this->topic));
        }
    }

    private function decode(Delivery $delivery): Envelope
    {
        try {
            $envelope = $this->serializer->decode(WireFormat::decode($delivery->message->body));
        } catch (MessageDecodingFailedException $e) {
            // Messenger 7.4 throws here (8.x returns an envelope wrapping the
            // exception, which the worker handles). Retrying can't fix it: to the
            // dead letter queue, where the body stays available for inspection.
            try {
                $this->client->nack($this->queue, $delivery->leaseId, deadLetter: true);
            } catch (QsrvException) {
            }
            throw $e;
        }

        $m = $delivery->message;

        return $envelope->withoutAll(TransportMessageIdStamp::class)->with(
            new QsrvReceivedStamp($delivery->leaseId, $m->id, $m->attempts, $m->groupId, $delivery->expiresAt, $this->queue),
            new TransportMessageIdStamp($m->id),
        );
    }

    private function stamp(Envelope $envelope): QsrvReceivedStamp
    {
        $this->requireQueue();

        return $envelope->last(QsrvReceivedStamp::class)
            ?? throw new \LogicException('qsrv: envelope was not received from a qsrv transport (no QsrvReceivedStamp)');
    }

    /**
     * Runs a client call, creating the queue once on not_found when
     * auto_setup is on, and wrapping errors in Messenger's TransportException.
     *
     * @template T
     *
     * @param callable(): T $fn
     *
     * @return T
     */
    private function call(callable $fn, bool $setupOnMissing = true): mixed
    {
        try {
            try {
                return $fn();
            } catch (NotFoundException $e) {
                if (!$this->autoSetup || !$setupOnMissing) {
                    throw $e;
                }
                $this->setup();

                return $fn();
            }
        } catch (QsrvException $e) {
            throw new TransportException($e->getMessage(), $e instanceof ApiException ? $e->status : 0, $e);
        }
    }
}
