<?php

declare(strict_types=1);

namespace Romycode\Qsrv\Tests\Integration;

use PHPUnit\Framework\TestCase;
use Romycode\Qsrv\Client;
use Romycode\Qsrv\Exception\QsrvException;

/**
 * Runs against a real qsrv. Start one and export:
 *
 *     KEY=$(openssl rand -hex 32)
 *     QSRV_API_KEYS="test:phpunit:$KEY" qsrv -data-dir /tmp/qsrv-test -addr 127.0.0.1:18080
 *     export QSRV_TEST_URL=http://127.0.0.1:18080 QSRV_TEST_API_KEY=$KEY
 */
abstract class IntegrationTestCase extends TestCase
{
    protected Client $client;
    /** @var list<string> */
    private array $queues = [];
    /** @var list<string> */
    private array $topics = [];

    protected function setUp(): void
    {
        $url = getenv('QSRV_TEST_URL');
        $key = getenv('QSRV_TEST_API_KEY');
        if (!$url || !$key) {
            self::markTestSkipped('set QSRV_TEST_URL and QSRV_TEST_API_KEY to run integration tests');
        }
        $this->client = new Client($url, $key);
    }

    protected function tearDown(): void
    {
        // Topics first (a subscribed queue can't be deleted), DLQs last.
        foreach ($this->topics as $t) {
            try {
                $this->client->deleteTopic($t);
            } catch (QsrvException) {
            }
        }
        foreach (array_reverse($this->queues) as $q) {
            try {
                $this->client->deleteQueue($q);
            } catch (QsrvException) {
            }
        }
    }

    /** A unique topic name, deleted after the test. */
    protected function topicName(string $prefix = 'php-topic'): string
    {
        return $this->topics[] = $prefix.'-'.bin2hex(random_bytes(6));
    }

    /** A unique queue name, deleted after the test. */
    protected function queueName(string $prefix = 'php'): string
    {
        return $this->queues[] = $prefix.'-'.bin2hex(random_bytes(6));
    }
}
