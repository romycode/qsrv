<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Integration;

use Romycode\Qsrv\Exception\LeaseGoneException;
use Romycode\Qsrv\Exception\NotFoundException;
use Romycode\Qsrv\Exception\QueueExistsException;
use Romycode\Qsrv\Exception\UnauthorizedException;
use Romycode\Qsrv\Client;
use Romycode\Qsrv\Model\QueueConfig;
use Romycode\Qsrv\Model\RetryPolicy;

final class ClientIntegrationTest extends IntegrationTestCase
{
    public function testLifecycle(): void
    {
        $q = $this->queueName();
        self::assertTrue($this->client->healthy());
        $created = $this->client->createQueue($q, new QueueConfig(visibilityTimeout: 30, retention: 0));
        self::assertSame(30.0, $created->config->visibilityTimeout);
        self::assertContains($q, $this->client->listQueues());

        try {
            $this->client->createQueue($q);
            self::fail('duplicate create accepted');
        } catch (QueueExistsException) {
        }

        $sent = $this->client->send($q, 'hello');
        $d = $this->client->receive($q);
        self::assertNotNull($d);
        self::assertSame($sent->id, $d->message->id);
        self::assertSame('hello', $d->message->body);
        self::assertSame(1, $d->message->attempts);
        self::assertGreaterThan(new \DateTimeImmutable(), $d->expiresAt);

        $exp = $this->client->extend($q, $d->leaseId, 120);
        self::assertGreaterThan($d->expiresAt, $exp);
        $this->client->ack($q, $d->leaseId);

        try {
            $this->client->ack($q, $d->leaseId);
            self::fail('double ack accepted');
        } catch (LeaseGoneException) {
        }

        self::assertNull($this->client->receive($q));
        $stats = $this->client->getQueue($q)->stats;
        self::assertSame(0, $stats->messages);
        self::assertSame(1, $stats->history);

        $this->client->deleteQueue($q);
        $this->expectException(NotFoundException::class);
        $this->client->getQueue($q);
    }

    public function testBinaryBody(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q);
        $bin = random_bytes(64)."\xff\x00";
        $this->client->send($q, $bin);
        self::assertSame($bin, $this->client->receive($q)?->message->body);
    }

    public function testNackAppliesRetryPolicy(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q, new QueueConfig(retry: new RetryPolicy(maxAttempts: 5)));
        $this->client->send($q, 'x');
        $this->client->nack($q, $this->client->receive($q)->leaseId);
        $again = $this->client->receive($q);
        self::assertSame(2, $again?->message->attempts);
    }

    public function testDelay(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q);
        $this->client->send($q, 'later', delay: 0.4);
        $this->client->send($q, 'now');
        self::assertSame('now', $this->client->receive($q)?->message->body);
        self::assertNull($this->client->receive($q), 'delayed message delivered early');
        self::assertSame(1, $this->client->getQueue($q)->stats->delayed);

        $t = microtime(true);
        $d = $this->client->receive($q, wait: 5);
        self::assertSame('later', $d?->message->body);
        self::assertLessThan(3, microtime(true) - $t, 'long-poll must wake up when the delay ends');
    }

    public function testGroupsAreStrictlyOrdered(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q);
        $this->client->send($q, 'a1', groupId: 'A');
        $this->client->send($q, 'a2', groupId: 'A');
        $this->client->send($q, 'b1', groupId: 'B');

        $a1 = $this->client->receive($q);
        $b1 = $this->client->receive($q);
        self::assertSame(['a1', 'b1'], [$a1?->message->body, $b1?->message->body]);
        self::assertSame('A', $a1->message->groupId);
        self::assertNull($this->client->receive($q), 'a2 must wait for a1');
        $this->client->ack($q, $a1->leaseId);
        self::assertSame('a2', $this->client->receive($q)?->message->body);
    }

    public function testLongPollWakesOnSend(): void
    {
        $q = $this->queueName();
        $this->client->createQueue($q);
        // Nothing to receive: the call waits for the full wait.
        $t = microtime(true);
        self::assertNull($this->client->receive($q, wait: 1));
        self::assertGreaterThanOrEqual(0.9, microtime(true) - $t);
    }

    public function testReplay(): void
    {
        $src = $this->queueName();
        $dst = $this->queueName();
        $this->client->createQueue($src, new QueueConfig(retention: 0));
        $this->client->createQueue($dst);
        foreach (['1', '2', '3'] as $b) {
            $this->client->send($src, $b);
            $this->client->ack($src, $this->client->receive($src)->leaseId);
        }
        $r = $this->client->replay($src, $dst, states: ['acked'], limit: 2);
        self::assertSame(2, $r->replayed);
        self::assertNotNull($r->nextCursor);
        $r = $this->client->replay($src, $dst, cursor: $r->nextCursor, limit: 2);
        self::assertSame(1, $r->replayed);
        self::assertNull($r->nextCursor);

        $bodies = [];
        while ($d = $this->client->receive($dst)) {
            $bodies[] = $d->message->body;
            self::assertNotNull($d->message->replayOf);
        }
        self::assertSame(['1', '2', '3'], $bodies);
    }

    public function testWrongKey(): void
    {
        $this->expectException(UnauthorizedException::class);
        (new Client((string) getenv('QSRV_TEST_URL'), str_repeat('x', 40)))->listQueues();
    }
}
