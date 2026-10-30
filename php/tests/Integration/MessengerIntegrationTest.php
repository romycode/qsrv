<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Integration;

use Romycode\Qsrv\Messenger\QsrvGroupStamp;
use Romycode\Qsrv\Messenger\QsrvPublishedStamp;
use Romycode\Qsrv\Messenger\QsrvReceivedStamp;
use Romycode\Qsrv\Messenger\QsrvTransport;
use Romycode\Qsrv\Messenger\QsrvTransportFactory;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\RetryPolicy;
use Romycode\Qsrv\Tests\Integration\Fixtures\DummyMessage;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Exception\MessageDecodingFailedException;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\SendMessageMiddleware;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Sender\SendersLocator;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Transport\Serialization\Serializer;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Worker;
use Psr\Container\ContainerInterface;

final class MessengerIntegrationTest extends IntegrationTestCase
{
    /** @param array<string, mixed> $options */
    private function transport(string $queue, array $options = [], ?SerializerInterface $serializer = null): QsrvTransport
    {
        $url = parse_url((string) getenv('QSRV_TEST_URL'));
        // An empty queue is allowed with a topic: a publish-only transport.
        $dsn = \sprintf('qsrv://%s@%s:%d/%s', rawurlencode((string) getenv('QSRV_TEST_API_KEY')), $url['host'], $url['port'] ?? 80, rawurlencode($queue));
        $dsn = rtrim($dsn, '/');
        $t = (new QsrvTransportFactory())->createTransport($dsn, $options + ['wait_time' => 0], $serializer ?? new PhpSerializer());
        self::assertInstanceOf(QsrvTransport::class, $t);

        return $t;
    }

    /** @return list<Envelope> */
    private static function all(QsrvTransport $t, int $fetchSize = 1): array
    {
        return [...$t->get($fetchSize)];
    }

    public function testSendGetAck(): void
    {
        $q = $this->queueName();
        $t = $this->transport($q);
        $sent = $t->send(new Envelope(new DummyMessage('hello')));
        $id = $sent->last(TransportMessageIdStamp::class)?->getId();
        self::assertNotNull($id);
        self::assertSame(1, $t->getMessageCount());

        [$got] = self::all($t);
        self::assertEquals(new DummyMessage('hello'), $got->getMessage());
        self::assertSame($id, $got->last(TransportMessageIdStamp::class)?->getId());
        $stamp = $got->last(QsrvReceivedStamp::class);
        self::assertNotNull($stamp);
        self::assertSame($id, $stamp->messageId);
        self::assertSame(1, $stamp->attempts);
        self::assertSame(0, $t->getMessageCount(), 'in-flight messages are not waiting');

        $t->ack($got);
        self::assertSame([], self::all($t));
        self::assertSame(1, $this->client->getQueue($q)->stats->history);
    }

    public function testWireFormatIsPlainJson(): void
    {
        $q = $this->queueName();
        $this->transport($q)->send(new Envelope(new DummyMessage('x')));
        $raw = json_decode((string) $this->client->receive($q)?->message->body, true);
        self::assertIsArray($raw);
        self::assertIsArray($raw['headers'] ?? null); // PhpSerializer sets none; the Symfony serializer sets "type"
        self::assertIsString($raw['body'] ?? $raw['body_base64'] ?? null);
    }

    /** With the Symfony serializer the payload is JSON plus a "type" header: readable from Go. */
    public function testJsonSerializerInterop(): void
    {
        $q = $this->queueName();
        $t = $this->transport($q, serializer: Serializer::create());
        $t->send(new Envelope(new DummyMessage('from php')));
        $raw = json_decode((string) $this->client->receive($q)?->message->body, true);
        self::assertSame(DummyMessage::class, $raw['headers']['type'] ?? null);
        self::assertSame(['text' => 'from php'], json_decode($raw['body'], true));

        // Another producer (e.g. Go) writing the same format is consumed as a normal message.
        $this->client->send($q, json_encode(['body' => '{"text":"from go"}', 'headers' => ['type' => DummyMessage::class]]));
        [$env] = self::all($t);
        self::assertEquals(new DummyMessage('from go'), $env->getMessage());
    }

    public function testAutoSetupUsesQueueConfig(): void
    {
        $q = $this->queueName();
        $t = $this->transport($q, ['queue_config' => ['visibility_timeout' => '45s', 'retry' => ['max_attempts' => 7]]]);
        $t->send(new Envelope(new DummyMessage('x'))); // creates the queue on not_found
        $cfg = $this->client->getQueue($q)->config;
        self::assertSame(45.0, $cfg->visibilityTimeout);
        self::assertSame(7, $cfg->retry?->maxAttempts);
        $t->setup(); // idempotent
    }

    public function testNoAutoSetup(): void
    {
        $t = $this->transport($this->queueName(), ['auto_setup' => false]);
        $this->expectException(TransportException::class);
        $t->send(new Envelope(new DummyMessage('x')));
    }

    public function testDelayStamp(): void
    {
        $t = $this->transport($this->queueName());
        $t->send(new Envelope(new DummyMessage('later'), [new DelayStamp(400)]));
        $t->send(new Envelope(new DummyMessage('now')));
        $got = self::all($t, 5);
        self::assertCount(1, $got);
        self::assertEquals(new DummyMessage('now'), $got[0]->getMessage());
        usleep(500_000);
        self::assertEquals(new DummyMessage('later'), self::all($t)[0]->getMessage());
    }

    public function testGroupStampKeepsOrder(): void
    {
        $q = $this->queueName();
        $t = $this->transport($q);
        foreach (['1', '2', '3'] as $n) {
            $t->send(new Envelope(new DummyMessage($n), [new QsrvGroupStamp('order-42')]));
        }
        $first = self::all($t, 3);
        self::assertCount(1, $first, 'only the group head is deliverable');
        self::assertSame('order-42', $first[0]->last(QsrvReceivedStamp::class)?->groupId);
        $t->ack($first[0]);
        self::assertEquals(new DummyMessage('2'), self::all($t)[0]->getMessage());
    }

    public function testFetchSizeLeasesLazily(): void
    {
        $q = $this->queueName();
        $t = $this->transport($q);
        foreach (['a', 'b', 'c'] as $n) {
            $t->send(new Envelope(new DummyMessage($n)));
        }
        $gen = $t->get(2);
        foreach ($gen as $i => $env) {
            if (0 === $i) {
                self::assertSame(1, $this->client->getQueue($q)->stats->inFlight, 'next lease taken only when iterated');
            }
            $t->ack($env);
        }
        self::assertSame(1, $t->getMessageCount());
    }

    public function testRejectNacksByDefault(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q, new QueueConfig(retry: new RetryPolicy(maxAttempts: 5, initialBackoff: 0)));
        $t = $this->transport($q);
        $t->send(new Envelope(new DummyMessage('x')));
        $t->reject(self::all($t)[0]); // outside a worker: qsrv's retry policy decides
        self::assertSame(2, self::all($t)[0]->last(QsrvReceivedStamp::class)?->attempts);
    }

    public function testRejectAcksWithSymfonyRetries(): void
    {
        $q = $this->queueName();
        $t = $this->transport($q, ['native_retries' => false]);
        $t->send(new Envelope(new DummyMessage('x')));
        $t->reject(self::all($t)[0]); // Messenger already re-sent it or gave up
        self::assertSame([], self::all($t));
        self::assertSame(1, $this->client->getQueue($q)->stats->history);
    }

    public function testKeepaliveExtendsLease(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q, new QueueConfig(visibilityTimeout: 1));
        $t = $this->transport($q);
        $t->send(new Envelope(new DummyMessage('slow')));
        $env = self::all($t)[0];
        $t->keepalive($env, 2); // lease now expires in 4s
        usleep(1_500_000);      // past the 1s visibility timeout
        $t->ack($env);          // still ours
        self::assertSame(0, $this->client->getQueue($q)->stats->messages);
    }

    public function testExpiredLeaseAckFails(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q, new QueueConfig(visibilityTimeout: 1)); // server minimum
        $t = $this->transport($q);
        $t->send(new Envelope(new DummyMessage('x')));
        $env = self::all($t)[0];
        usleep(1_200_000);
        $this->expectException(TransportException::class);
        $this->expectExceptionMessageMatches('/expired/');
        $t->ack($env);
    }

    public function testUndecodableMessageGoesToTheDeadLetterQueue(): void
    {
        $q = $this->queueName();
        $dlq = $this->queueName();
        $this->client->createQueue($dlq);
        $this->client->createQueue($q, new QueueConfig(retry: new RetryPolicy(maxAttempts: 5), deadLetterQueue: $dlq));
        $this->client->send($q, '{"not":"a messenger envelope"}');
        $t = $this->transport($q);

        if (method_exists(MessageDecodingFailedException::class, 'wrap')) {
            // Messenger 8.x: the serializer returns an envelope whose message is
            // the decoding exception; the worker rejects it.
            [$env] = self::all($t);
            self::assertInstanceOf(MessageDecodingFailedException::class, $env->getMessage());
            $t->reject($env);
        } else {
            // Messenger 7.4: decode() throws, and the transport dead-letters it.
            try {
                self::all($t);
                self::fail('decoded garbage');
            } catch (MessageDecodingFailedException) {
            }
        }
        self::assertSame(0, $this->client->getQueue($q)->stats->messages, 'not retried');
        self::assertSame('{"not":"a messenger envelope"}', $this->client->receive($dlq)?->message->body);
    }

    public function testWorkerConsumes(): void
    {
        $q = $this->queueName();
        // No retry listener here: with Symfony retries a failed message is just acked.
        $t = $this->transport($q, ['wait_time' => 1, 'native_retries' => false]);
        $handled = [];
        $worker = null;
        $handler = function (DummyMessage $m) use (&$handled, &$worker): void {
            $handled[] = $m->text;
            if ('boom' === $m->text) {
                throw new \RuntimeException('handler failed');
            }
            if (3 === \count($handled)) {
                $worker?->stop();
            }
        };
        $senders = new SendersLocator([DummyMessage::class => ['qsrv']], new class($t) implements ContainerInterface {
            public function __construct(private readonly QsrvTransport $t)
            {
            }

            public function get(string $id): QsrvTransport
            {
                return $this->t;
            }

            public function has(string $id): bool
            {
                return 'qsrv' === $id;
            }
        });
        $bus = new MessageBus([new SendMessageMiddleware($senders), new HandleMessageMiddleware(new HandlersLocator([DummyMessage::class => [$handler]]))]);

        foreach (['one', 'boom', 'three'] as $text) {
            $bus->dispatch(new DummyMessage($text)); // routed to qsrv, not handled yet
        }
        self::assertSame([], $handled);
        self::assertSame(3, $t->getMessageCount());

        $worker = new Worker(['qsrv' => $t], $bus);
        $worker->run(['sleep' => 100_000]);

        self::assertSame(['one', 'boom', 'three'], $handled);
        $stats = $this->client->getQueue($q)->stats;
        self::assertSame(0, $stats->messages, 'handled are acked, failed are rejected (= acked)');
        self::assertSame(3, $stats->history);
    }

    public function testTopicPublishAndSubscribe(): void
    {
        $topic = $this->topicName();
        $billing = $this->queueName();
        $email = $this->queueName();
        $producer = $this->transport('', ['topic' => $topic]);
        $billingT = $this->transport($billing, ['subscribe' => [$topic]]);
        $emailT = $this->transport($email, ['subscribe' => $topic]);
        $billingT->setup(); // creates the queue and the topic, and subscribes
        $emailT->setup();
        $producer->setup(); // the topic already exists: kept

        $sent = $producer->send(new Envelope(new DummyMessage('order 1'), [new QsrvGroupStamp('c42')]));
        $stamp = $sent->last(QsrvPublishedStamp::class);
        self::assertNotNull($stamp);
        self::assertSame($topic, $stamp->topic);
        self::assertEqualsCanonicalizing([$billing, $email], array_keys($stamp->messageIds));

        foreach ([$billingT, $emailT] as $t) {
            [$env] = self::all($t);
            self::assertEquals(new DummyMessage('order 1'), $env->getMessage());
            self::assertSame('c42', $env->last(QsrvReceivedStamp::class)?->groupId);
            $t->ack($env);
        }
    }

    public function testPublishToMissingTopicAutoCreatesIt(): void
    {
        $topic = $this->topicName();
        $producer = $this->transport('', ['topic' => $topic]);
        $sent = $producer->send(new Envelope(new DummyMessage('nobody listens')));
        self::assertSame([], $sent->last(QsrvPublishedStamp::class)?->messageIds);
        self::assertSame([], $this->client->getTopic($topic)->queues);
    }
}
