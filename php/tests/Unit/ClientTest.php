<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Romycode\Qsrv\Client;
use Romycode\Qsrv\Exception\ApiException;
use Romycode\Qsrv\Exception\LeaseGoneException;
use Romycode\Qsrv\Exception\NotFoundException;
use Romycode\Qsrv\Exception\QueueFullException;
use Romycode\Qsrv\Exception\TransportException;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\RetryPolicy;
use Symfony\Component\HttpClient\MockHttpClient;
use Symfony\Component\HttpClient\Response\JsonMockResponse;
use Symfony\Component\HttpClient\Response\MockResponse;

final class ClientTest extends TestCase
{
    private const KEY = 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk';

    /** @var list<array{method: string, url: string, options: array<string, mixed>}> */
    private array $requests = [];

    /** @param list<MockResponse> $responses */
    private function client(array $responses): Client
    {
        $this->requests = [];
        $http = new MockHttpClient(function (string $method, string $url, array $options) use (&$responses) {
            $this->requests[] = ['method' => $method, 'url' => $url, 'options' => $options];

            return array_shift($responses) ?? throw new \LogicException('unexpected request '.$method.' '.$url);
        });

        return new Client('http://qsrv.test:8080/', self::KEY, $http);
    }

    private function sentJson(int $i = 0): array
    {
        return json_decode($this->requests[$i]['options']['body'], true);
    }

    private static function message(array $extra = []): JsonMockResponse
    {
        return new JsonMockResponse(['id' => 'm1', 'attempts' => 0, 'enqueued_at' => '2026-10-29T19:33:00.123456789Z'] + $extra, ['http_code' => 201]);
    }

    public function testSendTextBodyWithGroupAndDelay(): void
    {
        $c = $this->client([self::message(['group_id' => 'g'])]);
        $m = $c->send('orders', '{"a":1}', groupId: 'g', delay: 1.5);

        self::assertSame('POST', $this->requests[0]['method']);
        self::assertSame('http://qsrv.test:8080/v1/queues/orders/messages', $this->requests[0]['url']);
        self::assertContains('Authorization: Bearer '.self::KEY, $this->requests[0]['options']['headers']);
        self::assertSame(['body' => '{"a":1}', 'group_id' => 'g', 'delay' => '1500ms'], $this->sentJson());
        self::assertSame('m1', $m->id);
        self::assertSame('{"a":1}', $m->body);
        self::assertSame('g', $m->groupId);
        self::assertSame('2026-10-29T19:33:00.123456+00:00', $m->enqueuedAt->format('Y-m-d\TH:i:s.uP'));
    }

    public function testSendBinaryBodyUsesBase64(): void
    {
        $c = $this->client([self::message()]);
        $c->send('q', "\xff\x00\xfe");
        self::assertSame(['body_base64' => base64_encode("\xff\x00\xfe")], $this->sentJson());
    }

    public function testSendRejectsDelayOutOfRange(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->client([])->send('q', 'x', delay: Client::MAX_DELAY + 1);
    }

    public function testQueueNameIsEscaped(): void
    {
        $c = $this->client([self::message()]);
        $c->send('a b/c', 'x');
        self::assertSame('http://qsrv.test:8080/v1/queues/a%20b%2Fc/messages', $this->requests[0]['url']);
    }

    public function testReceiveEmptyReturnsNull(): void
    {
        $c = $this->client([new MockResponse('', ['http_code' => 204])]);
        self::assertNull($c->receive('q', wait: 30));
        self::assertSame('http://qsrv.test:8080/v1/queues/q/receive?wait=20000ms', $this->requests[0]['url'], 'wait is capped at 20s');
        self::assertGreaterThanOrEqual(30, $this->requests[0]['options']['timeout'], 'HTTP timeout must exceed the long-poll wait');
    }

    public function testReceiveDecodesBase64Body(): void
    {
        $c = $this->client([new JsonMockResponse([
            'message' => ['id' => 'm1', 'body_base64' => base64_encode("\xff"), 'attempts' => 2, 'enqueued_at' => '2026-10-29T19:33:00Z', 'replay_of' => 'm0'],
            'lease_id' => 'L1',
            'expires_at' => '2026-10-29T19:33:30Z',
        ])]);
        $d = $c->receive('q');
        self::assertNotNull($d);
        self::assertSame("\xff", $d->message->body);
        self::assertSame(2, $d->message->attempts);
        self::assertSame('m0', $d->message->replayOf);
        self::assertNull($d->message->groupId);
        self::assertSame('L1', $d->leaseId);
        self::assertSame('http://qsrv.test:8080/v1/queues/q/receive', $this->requests[0]['url']);
    }

    public function testAckNackExtend(): void
    {
        $c = $this->client([
            new MockResponse('', ['http_code' => 204]),
            new MockResponse('', ['http_code' => 204]),
            new JsonMockResponse(['expires_at' => '2026-10-29T19:34:00Z']),
        ]);
        $c->ack('q', 'L1');
        $c->nack('q', 'L1');
        $exp = $c->extend('q', 'L1', 60);
        self::assertStringEndsWith('/v1/queues/q/leases/L1/ack', $this->requests[0]['url']);
        self::assertStringEndsWith('/v1/queues/q/leases/L1/nack', $this->requests[1]['url']);
        self::assertSame(['by' => '60000ms'], $this->sentJson(2));
        self::assertSame('2026-10-29T19:34:00+00:00', $exp->format(\DATE_ATOM));
    }

    /** @return iterable<string, array{int, string, class-string<ApiException>}> */
    public static function errors(): iterable
    {
        yield 'lease gone' => [410, 'lease_gone', LeaseGoneException::class];
        yield 'not found' => [404, 'not_found', NotFoundException::class];
        yield 'queue full' => [429, 'queue_full', QueueFullException::class];
        yield 'other' => [422, 'invalid_config', ApiException::class];
    }

    #[\PHPUnit\Framework\Attributes\DataProvider('errors')]
    public function testErrorsMapToExceptions(int $status, string $code, string $class): void
    {
        $c = $this->client([new JsonMockResponse(['error' => 'boom', 'code' => $code], ['http_code' => $status])]);
        try {
            $c->ack('q', 'L1');
            self::fail('no exception');
        } catch (ApiException $e) {
            self::assertSame($class, $e::class);
            self::assertSame($status, $e->status);
            self::assertSame($code, $e->errorCode);
            self::assertStringContainsString('boom', $e->getMessage());
        }
    }

    public function testNonJsonErrorFromProxy(): void
    {
        $c = $this->client([new MockResponse('<html><body>Bad Gateway</body></html>', ['http_code' => 502])]);
        try {
            $c->listQueues();
            self::fail('no exception');
        } catch (ApiException $e) {
            self::assertSame('http_502', $e->errorCode);
            self::assertStringContainsString('Bad Gateway', $e->getMessage());
        }
    }

    public function testNetworkErrorIsTransportException(): void
    {
        $c = $this->client([new MockResponse('', ['error' => 'Connection refused'])]);
        $this->expectException(TransportException::class);
        $c->listQueues();
    }

    public function testCreateQueueSendsConfig(): void
    {
        $c = $this->client([new JsonMockResponse(['name' => 'q', 'visibility_timeout' => '30s', 'retention' => '0s', 'stats' => ['messages' => 0]], ['http_code' => 201])]);
        $q = $c->createQueue('q', new QueueConfig(
            visibilityTimeout: 30,
            retry: new RetryPolicy(maxAttempts: 5, initialBackoff: 1, maxBackoff: 60, multiplier: 2),
            deadLetterQueue: 'dlq',
            retention: 0,
        ));
        self::assertSame('PUT', $this->requests[0]['method']);
        self::assertSame([
            'visibility_timeout' => '30000ms',
            'retry' => ['max_attempts' => 5, 'initial_backoff' => '1000ms', 'max_backoff' => '60000ms', 'multiplier' => 2.0],
            'dead_letter_queue' => 'dlq',
            'retention' => '0ms',
        ], $this->sentJson());
        self::assertSame(30.0, $q->config->visibilityTimeout);
        self::assertSame(0.0, $q->config->retention);
    }

    public function testCreateQueueWithDefaultsSendsEmptyObject(): void
    {
        $c = $this->client([new JsonMockResponse(['name' => 'q', 'stats' => []], ['http_code' => 201])]);
        $c->createQueue('q');
        self::assertSame('{}', $this->requests[0]['options']['body']);
    }

    public function testReplay(): void
    {
        $c = $this->client([new JsonMockResponse(['replayed' => 1000, 'next_cursor' => 1000]), new JsonMockResponse(['replayed' => 3])]);
        $r = $c->replay('src', 'dst', states: ['acked'], from: new \DateTimeImmutable('2026-10-01T00:00:00Z'));
        self::assertSame(['to' => 'dst', 'states' => ['acked'], 'from' => '2026-10-01T00:00:00.000+00:00'], $this->sentJson());
        self::assertSame(1000, $r->nextCursor);
        $r = $c->replay('src', 'dst', cursor: $r->nextCursor);
        self::assertSame(['to' => 'dst', 'cursor' => 1000], $this->sentJson(1));
        self::assertNull($r->nextCursor);
    }
}
