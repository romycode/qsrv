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
 * - setup: creates the queue, the `topic`, and the `subscribe` topics with
 *   this queue subscribed to them.
 * - get: leases messages, long-polling up to wait_time seconds.
 * - ack: acks the lease. reject: acks too by default (Messenger already
 *   re-sent the message if it will be retried); with on_reject=nack the
 *   queue's own retry policy and dead-letter queue take over.
 * - keepalive (consume --keepalive): extends the lease.
 */
final class QsrvTransport implements TransportInterface, MessageCountAwareInterface, SetupableTransportInterface, KeepaliveReceiverInterface
{
    public const ON_REJECT_ACK = 'ack';
    public const ON_REJECT_NACK = 'nack';

    public function __construct(
        private readonly Client $client,
        private readonly string $queue,
        private readonly SerializerInterface $serializer,
        /** Seconds a get() long-polls for the first message (0-20). */
        private readonly float $waitTime = 5,
        private readonly bool $autoSetup = true,
        private readonly string $onReject = self::ON_REJECT_ACK,
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
        if (!\in_array($onReject, [self::ON_REJECT_ACK, self::ON_REJECT_NACK], true)) {
            throw new \InvalidArgumentException(\sprintf('qsrv: on_reject must be "ack" or "nack", got "%s"', $onReject));
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

    public function send(Envelope $envelope): Envelope
    {
        $body = WireFormat::encode($this->serializer->encode($envelope));
        $delay = max(0, ($envelope->last(DelayStamp::class)?->getDelay() ?? 0) / 1000);
        $group = $envelope->last(QsrvGroupStamp::class)?->groupId;

        if (null !== $this->topic) {
            $copies = $this->call(fn () => $this->client->publish($this->topic, $body, $group, $delay));

            return $envelope->with(new QsrvPublishedStamp($this->topic, array_map(static fn ($m) => $m->id, $copies)));
        }
        $message = $this->call(fn () => $this->client->send($this->queue, $body, $group, $delay));

        return $envelope->with(new TransportMessageIdStamp($message->id));
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

    public function ack(Envelope $envelope): void
    {
        $stamp = $this->stamp($envelope);
        try {
            $this->client->ack($this->queue, $stamp->leaseId);
        } catch (LeaseGoneException $e) {
            throw new TransportException(\sprintf('qsrv: lease of message %s expired before ack; it will be delivered again (increase visibility_timeout or consume with --keepalive)', $stamp->messageId), 0, $e);
        } catch (QsrvException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    public function reject(Envelope $envelope): void
    {
        $stamp = $this->stamp($envelope);
        try {
            if (self::ON_REJECT_NACK === $this->onReject) {
                $this->client->nack($this->queue, $stamp->leaseId);
            } else {
                $this->client->ack($this->queue, $stamp->leaseId);
            }
        } catch (LeaseGoneException) {
            // Already back in the queue: qsrv will redeliver it, nothing left to settle.
        } catch (QsrvException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
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
     * messenger:setup-transports: creates the queue, the topic, and the
     * subscribe topics with the queue subscribed. Existing ones are kept as
     * they are (queue settings are not changed).
     */
    public function setup(): void
    {
        try {
            if ('' !== $this->queue) {
                $this->ignoreExists(fn () => $this->client->createQueue($this->queue, $this->queueConfig));
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
            // Messenger < 8.1 throws here (8.1+ returns an envelope wrapping the
            // exception, which the worker handles). Let qsrv's retry policy / dead-letter queue deal with
            // poison messages; either way the body stays in qsrv's history for replay.
            try {
                $this->client->nack($this->queue, $delivery->leaseId);
            } catch (QsrvException) {
            }
            throw $e;
        }

        $m = $delivery->message;

        return $envelope->withoutAll(TransportMessageIdStamp::class)->with(
            new QsrvReceivedStamp($delivery->leaseId, $m->id, $m->attempts, $m->groupId, $delivery->expiresAt),
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
