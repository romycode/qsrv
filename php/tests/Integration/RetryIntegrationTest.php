<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Integration;

use Romycode\Qsrv\Messenger\QsrvGroupStamp;
use Romycode\Qsrv\Messenger\QsrvReceivedStamp;
use Romycode\Qsrv\Messenger\QsrvTransport;
use Romycode\Qsrv\Messenger\QsrvTransportFactory;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\RetryPolicy;
use Romycode\Qsrv\Tests\Integration\Fixtures\DummyMessage;
use Romycode\Qsrv\Tests\Integration\Fixtures\ServiceMap;
use Symfony\Component\EventDispatcher\EventDispatcher;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\EventListener\SendFailedMessageForRetryListener;
use Symfony\Component\Messenger\EventListener\StopWorkerOnTimeLimitListener;
use Symfony\Component\Messenger\Exception\TransportException;
use Symfony\Component\Messenger\Exception\UnrecoverableMessageHandlingException;
use Symfony\Component\Messenger\Handler\HandlersLocator;
use Symfony\Component\Messenger\MessageBus;
use Symfony\Component\Messenger\Middleware\HandleMessageMiddleware;
use Symfony\Component\Messenger\Middleware\MiddlewareInterface;
use Symfony\Component\Messenger\Middleware\StackInterface;
use Symfony\Component\Messenger\Retry\MultiplierRetryStrategy;
use Symfony\Component\Messenger\Transport\Serialization\PhpSerializer;
use Symfony\Component\Messenger\Worker;

/**
 * Real Messenger worker + SendFailedMessageForRetryListener against qsrv, in
 * both retry modes: what the docs promise about native_retries.
 */
final class RetryIntegrationTest extends IntegrationTestCase
{
    /** @var list<array{text: string, id: string, attempts: int, ok: bool}> */
    private array $log = [];

    /** @param array<string, mixed> $options */
    private function transport(string $queue, array $options = []): QsrvTransport
    {
        $url = parse_url((string) getenv('QSRV_TEST_URL'));
        $dsn = rtrim(\sprintf('qsrv://%s@%s:%d/%s', rawurlencode((string) getenv('QSRV_TEST_API_KEY')), $url['host'], $url['port'] ?? 80, rawurlencode($queue)), '/');

        return (new QsrvTransportFactory())->createTransport($dsn, $options + ['wait_time' => 0], new PhpSerializer());
    }

    /**
     * Runs a worker until $stop says so (or 10s). $fail decides, per call,
     * whether the handler throws (and what).
     *
     * @param callable(DummyMessage, int): ?\Throwable $fail  message, call number for this text
     * @param callable(list<array{text: string, id: string, attempts: int, ok: bool}>): bool $stop
     */
    private function consume(QsrvTransport $t, callable $fail, callable $stop, int $maxRetries = 3): void
    {
        $calls = [];
        $worker = null;
        $record = new class implements MiddlewareInterface {
            public ?Envelope $current = null;

            public function handle(Envelope $envelope, StackInterface $stack): Envelope
            {
                $this->current = $envelope;

                return $stack->next()->handle($envelope, $stack);
            }
        };
        $handler = function (DummyMessage $m) use (&$calls, &$worker, $fail, $stop, $record): void {
            $calls[$m->text] = ($calls[$m->text] ?? 0) + 1;
            $s = $record->current?->last(QsrvReceivedStamp::class);
            $e = $fail($m, $calls[$m->text]);
            $this->log[] = ['text' => $m->text, 'id' => (string) $s?->messageId, 'attempts' => (int) $s?->attempts, 'ok' => null === $e];
            if ($stop($this->log)) {
                $worker?->stop();
            }
            if (null !== $e) {
                throw $e;
            }
        };
        $bus = new MessageBus([$record, new HandleMessageMiddleware(new HandlersLocator([DummyMessage::class => [$handler]]))]);

        $events = new EventDispatcher();
        // Fast Messenger retries (50ms) so Symfony-mode tests stay quick.
        $events->addSubscriber(new SendFailedMessageForRetryListener(
            new ServiceMap(['qsrv' => $t]),
            new ServiceMap(['qsrv' => new MultiplierRetryStrategy($maxRetries, 50)]),
        ));
        $events->addSubscriber(new StopWorkerOnTimeLimitListener(10));
        $worker = new Worker(['qsrv' => $t], $bus, $events);
        $worker->run(['sleep' => 20_000]);
    }

    private function queueWith(RetryPolicy $retry, ?string $dlq = null): string
    {
        $q = $this->queueName();
        if (null !== $dlq) {
            $this->client->createQueue($dlq);
        }
        $this->client->createQueue($q, new QueueConfig(retry: $retry, deadLetterQueue: $dlq));

        return $q;
    }

    private function texts(bool $okOnly = false): array
    {
        return array_values(array_map(static fn ($l) => $l['text'], array_filter($this->log, static fn ($l) => !$okOnly || $l['ok'])));
    }

    // --- native_retries: true (default) --------------------------------------

    public function testNativeRetriesRedeliverTheSameMessage(): void
    {
        $q = $this->queueWith(new RetryPolicy(maxAttempts: 5, initialBackoff: 0));
        $t = $this->transport($q);
        $t->send(new Envelope(new DummyMessage('flaky')));

        $this->consume($t, fn ($m, $n) => $n < 3 ? new \RuntimeException('boom') : null, fn ($log) => end($log)['ok']);

        self::assertSame([1, 2, 3], array_column($this->log, 'attempts'), 'qsrv counts the attempts');
        self::assertCount(1, array_unique(array_column($this->log, 'id')), 'same qsrv message every time: no copies');
        $stats = $this->client->getQueue($q)->stats;
        self::assertSame([0, 0], [$stats->messages, $stats->history], 'one message, acked (deleted)');
    }

    public function testNativeRetriesEndInTheDeadLetterQueue(): void
    {
        $dlq = $this->queueName();
        $q = $this->queueWith(new RetryPolicy(maxAttempts: 3, initialBackoff: 0), $dlq);
        $t = $this->transport($q);
        $t->send(new Envelope(new DummyMessage('poison')));

        // Messenger's max_retries (1 here) doesn't matter: qsrv's max_attempts does.
        $this->consume($t, fn () => new \RuntimeException('always'), fn ($log) => 3 === \count($log), maxRetries: 1);
        usleep(100_000);

        self::assertSame([1, 2, 3], array_column($this->log, 'attempts'));
        self::assertSame(1, $this->client->getQueue($dlq)->stats->messages);
        self::assertSame(0, $this->client->getQueue($q)->stats->messages);
    }

    public function testUnrecoverableGoesStraightToTheDeadLetterQueue(): void
    {
        $dlq = $this->queueName();
        $q = $this->queueWith(new RetryPolicy(maxAttempts: 5, initialBackoff: 0), $dlq);
        $t = $this->transport($q);
        $t->send(new Envelope(new DummyMessage('invalid')));

        $this->consume($t, fn () => new UnrecoverableMessageHandlingException('bad data'), fn ($log) => 1 === \count($log));

        self::assertCount(1, $this->log);
        self::assertSame(1, $this->client->getQueue($dlq)->stats->messages, 'no retries left to try');
        self::assertSame(0, $this->client->getQueue($q)->stats->messages);
    }

    public function testRetryFalseQueueNeverRetries(): void
    {
        $dlq = $this->queueName();
        $q = $this->queueName();
        $t = $this->transport($q, ['queue_config' => ['retry' => false, 'dead_letter_queue' => $dlq]]);
        $t->send(new Envelope(new DummyMessage('once'))); // auto setup creates the DLQ too

        $this->consume($t, fn () => new \RuntimeException('boom'), fn ($log) => 1 === \count($log));

        self::assertSame(1, $this->client->getQueue($q)->config->retry?->maxAttempts);
        self::assertSame(1, $this->client->getQueue($dlq)->stats->messages);
    }

    public function testNativeRetriesKeepGroupOrder(): void
    {
        $q = $this->queueWith(new RetryPolicy(maxAttempts: 5, initialBackoff: 0));
        $t = $this->transport($q);
        foreach (['a1', 'a2'] as $text) {
            $t->send(new Envelope(new DummyMessage($text), [new QsrvGroupStamp('A')]));
        }

        $this->consume($t, fn ($m, $n) => 'a1' === $m->text && 1 === $n ? new \RuntimeException('boom') : null, fn () => 2 === \count($this->texts(true)));

        self::assertSame(['a1', 'a1', 'a2'], $this->texts(), 'a2 waits until a1 succeeds');
    }

    // --- native_retries: false -----------------------------------------------

    public function testSymfonyRetriesSendANewMessage(): void
    {
        $q = $this->queueWith(new RetryPolicy(maxAttempts: 5, initialBackoff: 0));
        $t = $this->transport($q, ['native_retries' => false]);
        $t->send(new Envelope(new DummyMessage('flaky')));

        $this->consume($t, fn ($m, $n) => 1 === $n ? new \RuntimeException('boom') : null, fn ($log) => end($log)['ok']);

        self::assertSame([1, 1], array_column($this->log, 'attempts'), 'each try is a fresh qsrv message');
        self::assertCount(2, array_unique(array_column($this->log, 'id')));
        $stats = $this->client->getQueue($q)->stats;
        self::assertSame([0, 0], [$stats->messages, $stats->history], 'original acked, retry acked (both deleted)');
    }

    public function testSymfonyRetriesMoveTheMessageToTheEndOfItsGroup(): void
    {
        $q = $this->queueWith(new RetryPolicy(maxAttempts: 5, initialBackoff: 0));
        $t = $this->transport($q, ['native_retries' => false]);
        foreach (['a1', 'a2'] as $text) {
            $t->send(new Envelope(new DummyMessage($text), [new QsrvGroupStamp('A')]));
        }

        $this->consume($t, fn ($m, $n) => 'a1' === $m->text && 1 === $n ? new \RuntimeException('boom') : null, fn () => 2 === \count($this->texts(true)));

        self::assertSame(['a1', 'a2', 'a1'], $this->texts(), 'the retry of a1 runs after a2: use native retries with groups');
    }

    // --- topics: a retry never goes back through the topic --------------------

    /** @return iterable<string, array{bool}> */
    public static function modes(): iterable
    {
        yield 'native' => [true];
        yield 'symfony' => [false];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('modes')]
    public function testRetryDoesNotRepublishToTheTopic(bool $native): void
    {
        $topic = $this->topicName();
        $mine = $this->queueWith(new RetryPolicy(maxAttempts: 5, initialBackoff: 0));
        $other = $this->queueName();
        $this->client->createQueue($other);
        $this->client->createTopic($topic, [$mine, $other]);
        // A transport that consumes $mine and publishes to $topic.
        $t = $this->transport($mine, ['topic' => $topic, 'native_retries' => $native]);
        $t->send(new Envelope(new DummyMessage('event')));

        $this->consume($t, fn ($m, $n) => 1 === $n ? new \RuntimeException('boom') : null, fn ($log) => end($log)['ok']);

        self::assertSame(1, $this->client->getQueue($other)->stats->messages, 'the other subscriber got exactly one copy');
    }

    // --- setup ---------------------------------------------------------------

    public function testSetupRefusesDifferentSettings(): void
    {
        $q = $this->queueWith(new RetryPolicy(maxAttempts: 5));
        $t = $this->transport($q, ['queue_config' => ['retry' => ['max_attempts' => 8], 'visibility_timeout' => '30s']]);
        try {
            $t->setup();
            self::fail('setup accepted different settings');
        } catch (TransportException $e) {
            self::assertStringContainsString('retry.max_attempts: configured 8, queue has 5', $e->getMessage());
            self::assertStringNotContainsString('visibility_timeout', $e->getMessage(), '30s is what the queue has');
        }
        $this->transport($q, ['queue_config' => ['retry' => ['max_attempts' => 5]]])->setup(); // same: fine
    }
}
