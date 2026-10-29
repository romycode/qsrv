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
 * Symfony Messenger transport backed by one qsrv queue.
 *
 * - send: one qsrv message per envelope (see WireFormat); DelayStamp maps to
 *   qsrv's delay, QsrvGroupStamp to its ordering group.
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
    ) {
        if (!\in_array($onReject, [self::ON_REJECT_ACK, self::ON_REJECT_NACK], true)) {
            throw new \InvalidArgumentException(\sprintf('qsrv: on_reject must be "ack" or "nack", got "%s"', $onReject));
        }
    }

    public function getQueueName(): string
    {
        return $this->queue;
    }

    public function send(Envelope $envelope): Envelope
    {
        $encoded = $this->serializer->encode($envelope);
        $delay = ($envelope->last(DelayStamp::class)?->getDelay() ?? 0) / 1000;
        $group = $envelope->last(QsrvGroupStamp::class)?->groupId;

        $message = $this->call(fn () => $this->client->send($this->queue, WireFormat::encode($encoded), $group, max(0, $delay)));

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
        $stats = $this->call(fn () => $this->client->getQueue($this->queue))->stats;

        return max(0, $stats->messages - $stats->inFlight);
    }

    /** Creates the queue (messenger:setup-transports). Existing queues are left as they are. */
    public function setup(): void
    {
        try {
            $this->client->createQueue($this->queue, $this->queueConfig);
        } catch (QueueExistsException) {
        } catch (QsrvException $e) {
            throw new TransportException($e->getMessage(), 0, $e);
        }
    }

    private function decode(Delivery $delivery): Envelope
    {
        try {
            $envelope = $this->serializer->decode(WireFormat::decode($delivery->message->body));
        } catch (MessageDecodingFailedException $e) {
            // Let qsrv's retry policy / dead-letter queue deal with poison messages;
            // either way the body stays in qsrv's history for replay.
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
